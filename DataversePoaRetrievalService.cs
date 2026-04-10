using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Query;
using POACleanupPlanner.Models;

namespace POACleanupPlanner.Services
{
    public class DataversePoaRetrievalService
    {
        private const string PrincipalObjectAccessEntityName = "principalobjectaccess";
        private const int DefaultMaxResults = 25;
        private const int DefaultEnrichmentLimit = 10;
        private const int MaximumMaxResults = 250;
        private const int HotspotSamplePageSize = 5000;
        private const int HotspotSamplePageLimit = 4;
        private const int MaximumFailureMessages = 50;
        private const int PrincipalResolutionBatchSize = 250;

        private readonly IOrganizationService _service;
        private readonly Dictionary<string, string> _entityDisplayNameCache;
        private readonly List<string> _lastEnrichmentFailureMessages;
        private Dictionary<int, string> _objectTypeCodeToLogicalNameCache;
        private Dictionary<string, int> _logicalNameToObjectTypeCodeCache;

        public DataversePoaRetrievalService(IOrganizationService service)
        {
            _service = service;
            _entityDisplayNameCache = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            _lastEnrichmentFailureMessages = new List<string>();
        }

        public bool IsReady
        {
            get { return _service != null; }
        }

        public int LastEnrichmentAttemptCount { get; private set; }

        public int LastEnrichmentSuccessCount { get; private set; }

        public int LastEnrichmentFailureCount { get; private set; }

        public int LastEnrichmentLimitUsed { get; private set; }

        public int LastHotspotSampleRowCount { get; private set; }

        public IList<string> LastEnrichmentFailureMessages
        {
            get { return _lastEnrichmentFailureMessages.AsReadOnly(); }
        }

        public IList<PoaEntityHotspot> GetTopPoaEntities(int maxResults = DefaultMaxResults, int enrichmentLimit = DefaultEnrichmentLimit)
        {
            ResetEnrichmentStats();

            var results = new List<PoaEntityHotspot>();

            if (!IsReady)
            {
                return results;
            }

            maxResults = NormalizeMaxResults(maxResults);
            enrichmentLimit = NormalizeEnrichmentLimit(enrichmentLimit, maxResults);

            var sampledRows = RetrieveHotspotSampleRows();
            LastHotspotSampleRowCount = sampledRows.Count;

            if (sampledRows.Count == 0)
            {
                return results;
            }

            ResolveMissingPrincipalTypes(sampledRows);

            var groupedRows = sampledRows
                .Where(r => r.ObjectTypeCode > 0)
                .GroupBy(r => r.ObjectTypeCode)
                .OrderByDescending(g => g.Count())
                .Take(maxResults)
                .ToList();

            foreach (var group in groupedRows)
            {
                var rows = group.ToList();
                var logicalName = ResolveLogicalName(group.Key);

                if (string.IsNullOrWhiteSpace(logicalName))
                {
                    logicalName = "ObjectTypeCode:" + group.Key;
                }

                results.Add(new PoaEntityHotspot
                {
                    EntityLogicalName = logicalName,
                    EntityDisplayName = ResolveDisplayName(logicalName),
                    TotalPoaRowCount = rows.Count,
                    UniqueOwnerCount = rows
                        .Where(r => r.ObjectId != Guid.Empty)
                        .Select(r => r.ObjectId)
                        .Distinct()
                        .Count(),
                    UniquePrincipalCount = rows
                        .Where(r => r.PrincipalId != Guid.Empty)
                        .Select(r => r.PrincipalId)
                        .Distinct()
                        .Count(),
                    InheritedAccessCount = 0,
                    DirectShareCount = rows.Count,
                    DominantPrincipalType = "Unknown",
                    DominantAccessPattern = "Unknown",
                    RiskSignal = "Low",
                    IsCleanupCandidate = false
                });
            }

            EnrichHotspots(results, sampledRows, enrichmentLimit);

            var grandTotal = results.Sum(r => r.TotalPoaRowCount);

            foreach (var hotspot in results)
            {
                hotspot.RiskSignal = CalculateRiskSignal(hotspot, grandTotal);
                hotspot.IsCleanupCandidate =
                    string.Equals(hotspot.RiskSignal, "High", StringComparison.OrdinalIgnoreCase) ||
                    hotspot.PercentOfTotal(grandTotal) >= 10m;
            }

            return results
                .OrderByDescending(r => r.TotalPoaRowCount)
                .ToList();
        }

        private List<PoaSampleRow> RetrieveHotspotSampleRows()
        {
            var results = new List<PoaSampleRow>();

            for (var pageNumber = 1; pageNumber <= HotspotSamplePageLimit; pageNumber++)
            {
                var query = new QueryExpression(PrincipalObjectAccessEntityName)
                {
                    ColumnSet = new ColumnSet(
                        "objecttypecode",
                        "objectid",
                        "principalid",
                        "accessrightsmask",
                        "inheritedaccessrightsmask"),
                    NoLock = true,
                    PageInfo = new PagingInfo
                    {
                        Count = HotspotSamplePageSize,
                        PageNumber = pageNumber
                    }
                };

                var response = _service.RetrieveMultiple(query);
                if (response == null || response.Entities.Count == 0)
                {
                    break;
                }

                foreach (var entity in response.Entities)
                {
                    results.Add(new PoaSampleRow
                    {
                        ObjectTypeCode = GetObjectTypeCodeFromEntity(entity),
                        ObjectId = GetReferenceId(entity, "objectid"),
                        PrincipalId = GetReferenceId(entity, "principalid"),
                        PrincipalLogicalName = GetReferenceLogicalName(entity, "principalid"),
                        AccessRightsMask = GetAttributeInt(entity, "accessrightsmask"),
                        InheritedAccessRightsMask = GetAttributeInt(entity, "inheritedaccessrightsmask")
                    });
                }

                if (!response.MoreRecords)
                {
                    break;
                }

                query.PageInfo.PagingCookie = response.PagingCookie;
            }

            return results;
        }

        private void ResolveMissingPrincipalTypes(IList<PoaSampleRow> sampledRows)
        {
            if (sampledRows == null || sampledRows.Count == 0)
            {
                return;
            }

            var unresolvedPrincipalIds = sampledRows
                .Where(r => r.PrincipalId != Guid.Empty && string.IsNullOrWhiteSpace(r.PrincipalLogicalName))
                .Select(r => r.PrincipalId)
                .Distinct()
                .ToList();

            if (unresolvedPrincipalIds.Count == 0)
            {
                return;
            }

            var principalTypeMap = new Dictionary<Guid, string>();

            ResolvePrincipalIdsForEntity(unresolvedPrincipalIds, "systemuser", "systemuserid", principalTypeMap);
            ResolvePrincipalIdsForEntity(unresolvedPrincipalIds, "team", "teamid", principalTypeMap);

            foreach (var row in sampledRows)
            {
                if (row.PrincipalId == Guid.Empty || !string.IsNullOrWhiteSpace(row.PrincipalLogicalName))
                {
                    continue;
                }

                string principalLogicalName;
                if (principalTypeMap.TryGetValue(row.PrincipalId, out principalLogicalName))
                {
                    row.PrincipalLogicalName = principalLogicalName;
                }
            }
        }

        private void ResolvePrincipalIdsForEntity(
            IList<Guid> principalIds,
            string entityLogicalName,
            string idAttributeName,
            IDictionary<Guid, string> principalTypeMap)
        {
            if (principalIds == null || principalIds.Count == 0)
            {
                return;
            }

            foreach (var batch in BatchGuids(principalIds, PrincipalResolutionBatchSize))
            {
                var query = new QueryExpression(entityLogicalName)
                {
                    ColumnSet = new ColumnSet(idAttributeName),
                    NoLock = true
                };

                query.Criteria.AddCondition(idAttributeName, ConditionOperator.In, batch.Cast<object>().ToArray());

                var response = _service.RetrieveMultiple(query);
                if (response == null || response.Entities.Count == 0)
                {
                    continue;
                }

                foreach (var entity in response.Entities)
                {
                    if (entity.Id != Guid.Empty && !principalTypeMap.ContainsKey(entity.Id))
                    {
                        principalTypeMap.Add(entity.Id, entityLogicalName);
                    }
                }
            }
        }

        private IEnumerable<IList<Guid>> BatchGuids(IList<Guid> source, int batchSize)
        {
            if (source == null || source.Count == 0)
            {
                yield break;
            }

            var effectiveBatchSize = batchSize <= 0 ? 100 : batchSize;

            for (var index = 0; index < source.Count; index += effectiveBatchSize)
            {
                yield return source.Skip(index).Take(effectiveBatchSize).ToList();
            }
        }

        private int GetObjectTypeCodeFromEntity(Entity entity)
        {
            if (entity == null || !entity.Attributes.Contains("objecttypecode"))
            {
                return 0;
            }

            var value = UnwrapAliasedValue(entity["objecttypecode"]);
            if (value == null)
            {
                return 0;
            }

            if (value is OptionSetValue optionSetValue)
            {
                return optionSetValue.Value;
            }

            if (value is int intValue)
            {
                return intValue;
            }

            if (value is long longValue)
            {
                return Convert.ToInt32(longValue);
            }

            if (value is decimal decimalValue)
            {
                return Convert.ToInt32(decimalValue);
            }

            var stringValue = value as string;
            if (!string.IsNullOrWhiteSpace(stringValue))
            {
                int parsedInt;
                if (int.TryParse(stringValue, out parsedInt))
                {
                    return parsedInt;
                }

                return ResolveObjectTypeCode(stringValue);
            }

            try
            {
                return Convert.ToInt32(value);
            }
            catch
            {
                return 0;
            }
        }

        private void EnrichHotspots(IList<PoaEntityHotspot> hotspots, IList<PoaSampleRow> sampledRows, int enrichmentLimit)
        {
            if (hotspots == null || hotspots.Count == 0 || sampledRows == null || sampledRows.Count == 0 || enrichmentLimit <= 0)
            {
                return;
            }

            var sampleLookup = sampledRows
                .Where(r => r.ObjectTypeCode > 0)
                .GroupBy(r => r.ObjectTypeCode)
                .ToDictionary(g => g.Key, g => g.ToList());

            var orderedHotspots = hotspots
                .OrderByDescending(h => h.TotalPoaRowCount)
                .Take(enrichmentLimit)
                .ToList();

            LastEnrichmentLimitUsed = enrichmentLimit;

            foreach (var hotspot in orderedHotspots)
            {
                LastEnrichmentAttemptCount++;

                try
                {
                    var objectTypeCode = ResolveObjectTypeCode(hotspot.EntityLogicalName);
                    if (objectTypeCode <= 0 || !sampleLookup.ContainsKey(objectTypeCode))
                    {
                        ApplyEnrichmentFallback(hotspot);
                        AddFailureMessage(hotspot.DisplayNameOrLogicalName, "No sampled enrichment rows were available for this entity.");
                        LastEnrichmentFailureCount++;
                        continue;
                    }

                    var rows = sampleLookup[objectTypeCode];
                    ApplySampleBasedEnrichment(hotspot, rows);
                    LastEnrichmentSuccessCount++;
                }
                catch (Exception ex)
                {
                    ApplyEnrichmentFallback(hotspot);
                    AddFailureMessage(hotspot.DisplayNameOrLogicalName, ex.Message);
                    LastEnrichmentFailureCount++;
                }
            }
        }

        private void ApplySampleBasedEnrichment(PoaEntityHotspot hotspot, IList<PoaSampleRow> rows)
        {
            if (hotspot == null || rows == null || rows.Count == 0)
            {
                ApplyEnrichmentFallback(hotspot);
                return;
            }

            hotspot.UniqueOwnerCount = rows
                .Where(r => r.ObjectId != Guid.Empty)
                .Select(r => r.ObjectId)
                .Distinct()
                .Count();

            hotspot.UniquePrincipalCount = rows
                .Where(r => r.PrincipalId != Guid.Empty)
                .Select(r => r.PrincipalId)
                .Distinct()
                .Count();

            hotspot.InheritedAccessCount = rows.Count(r => r.InheritedAccessRightsMask > 0);
            hotspot.DirectShareCount = Math.Max(0, hotspot.TotalPoaRowCount - hotspot.InheritedAccessCount);

            var dominantMaskGroup = rows
                .GroupBy(r => r.AccessRightsMask)
                .OrderByDescending(g => g.Count())
                .ThenBy(g => g.Key)
                .FirstOrDefault();

            hotspot.DominantAccessPattern = dominantMaskGroup == null
                ? "Unknown"
                : DecodeAccessRights(dominantMaskGroup.Key);

            var dominantPrincipalGroup = rows
                .Where(r => !string.IsNullOrWhiteSpace(r.PrincipalLogicalName))
                .GroupBy(r => r.PrincipalLogicalName, StringComparer.OrdinalIgnoreCase)
                .OrderByDescending(g => g.Count())
                .ThenBy(g => g.Key, StringComparer.OrdinalIgnoreCase)
                .FirstOrDefault();

            if (dominantPrincipalGroup == null)
            {
                hotspot.DominantPrincipalType = "Unknown";
            }
            else if (string.Equals(dominantPrincipalGroup.Key, "systemuser", StringComparison.OrdinalIgnoreCase))
            {
                hotspot.DominantPrincipalType = "User";
            }
            else if (string.Equals(dominantPrincipalGroup.Key, "team", StringComparison.OrdinalIgnoreCase))
            {
                hotspot.DominantPrincipalType = "Team";
            }
            else
            {
                hotspot.DominantPrincipalType = dominantPrincipalGroup.Key;
            }
        }

        private void ApplyEnrichmentFallback(PoaEntityHotspot hotspot)
        {
            if (hotspot == null)
            {
                return;
            }

            hotspot.UniqueOwnerCount = 0;
            hotspot.UniquePrincipalCount = 0;
            hotspot.InheritedAccessCount = 0;
            hotspot.DirectShareCount = hotspot.TotalPoaRowCount;
            hotspot.DominantPrincipalType = "Unknown";
            hotspot.DominantAccessPattern = "Unknown";
        }

        private void AddFailureMessage(string entityName, string message)
        {
            if (string.IsNullOrWhiteSpace(message))
            {
                return;
            }

            if (_lastEnrichmentFailureMessages.Count >= MaximumFailureMessages)
            {
                return;
            }

            _lastEnrichmentFailureMessages.Add(
                string.Format(
                    "{0}: {1}",
                    string.IsNullOrWhiteSpace(entityName) ? "(unknown entity)" : entityName,
                    message));
        }

        private void ResetEnrichmentStats()
        {
            LastEnrichmentAttemptCount = 0;
            LastEnrichmentSuccessCount = 0;
            LastEnrichmentFailureCount = 0;
            LastEnrichmentLimitUsed = 0;
            LastHotspotSampleRowCount = 0;
            _lastEnrichmentFailureMessages.Clear();
        }

        private int NormalizeMaxResults(int maxResults)
        {
            if (maxResults <= 0)
            {
                return DefaultMaxResults;
            }

            if (maxResults > MaximumMaxResults)
            {
                return MaximumMaxResults;
            }

            return maxResults;
        }

        private int NormalizeEnrichmentLimit(int enrichmentLimit, int maxResults)
        {
            if (enrichmentLimit <= 0)
            {
                return DefaultEnrichmentLimit > maxResults
                    ? maxResults
                    : DefaultEnrichmentLimit;
            }

            if (enrichmentLimit > maxResults)
            {
                return maxResults;
            }

            return enrichmentLimit;
        }

        private int GetAttributeInt(Entity row, string attributeName)
        {
            if (row == null || string.IsNullOrWhiteSpace(attributeName) || !row.Attributes.Contains(attributeName))
            {
                return 0;
            }

            var value = UnwrapAliasedValue(row[attributeName]);
            if (value == null)
            {
                return 0;
            }

            if (value is OptionSetValue optionSetValue)
            {
                return optionSetValue.Value;
            }

            if (value is int intValue)
            {
                return intValue;
            }

            if (value is long longValue)
            {
                return Convert.ToInt32(longValue);
            }

            if (value is decimal decimalValue)
            {
                return Convert.ToInt32(decimalValue);
            }

            var stringValue = value as string;
            if (!string.IsNullOrWhiteSpace(stringValue))
            {
                int parsedInt;
                if (int.TryParse(stringValue, out parsedInt))
                {
                    return parsedInt;
                }
            }

            try
            {
                return Convert.ToInt32(value);
            }
            catch
            {
                return 0;
            }
        }

        private Guid GetReferenceId(Entity row, string attributeName)
        {
            if (row == null ||
                string.IsNullOrWhiteSpace(attributeName) ||
                !row.Attributes.Contains(attributeName) ||
                row[attributeName] == null)
            {
                return Guid.Empty;
            }

            var value = UnwrapAliasedValue(row[attributeName]);
            if (value == null)
            {
                return Guid.Empty;
            }

            if (value is EntityReference entityReference)
            {
                return entityReference.Id;
            }

            if (value is Guid guidValue)
            {
                return guidValue;
            }

            Guid parsedGuid;
            if (Guid.TryParse(Convert.ToString(value), out parsedGuid))
            {
                return parsedGuid;
            }

            return Guid.Empty;
        }

        private string GetReferenceLogicalName(Entity row, string attributeName)
        {
            if (row == null ||
                string.IsNullOrWhiteSpace(attributeName) ||
                !row.Attributes.Contains(attributeName) ||
                row[attributeName] == null)
            {
                return null;
            }

            var value = UnwrapAliasedValue(row[attributeName]);
            var entityReference = value as EntityReference;
            return entityReference != null ? entityReference.LogicalName : null;
        }

        private object UnwrapAliasedValue(object value)
        {
            var aliasedValue = value as AliasedValue;
            return aliasedValue != null ? aliasedValue.Value : value;
        }

        private string ResolveLogicalName(int objectTypeCode)
        {
            if (objectTypeCode <= 0)
            {
                return null;
            }

            EnsureEntityCachesLoaded();

            string logicalName;
            if (_objectTypeCodeToLogicalNameCache != null &&
                _objectTypeCodeToLogicalNameCache.TryGetValue(objectTypeCode, out logicalName))
            {
                return logicalName;
            }

            return null;
        }

        private int ResolveObjectTypeCode(string logicalName)
        {
            if (string.IsNullOrWhiteSpace(logicalName))
            {
                return 0;
            }

            EnsureEntityCachesLoaded();

            int objectTypeCode;
            if (_logicalNameToObjectTypeCodeCache != null &&
                _logicalNameToObjectTypeCodeCache.TryGetValue(logicalName, out objectTypeCode))
            {
                return objectTypeCode;
            }

            return 0;
        }

        private void EnsureEntityCachesLoaded()
        {
            if (_objectTypeCodeToLogicalNameCache != null && _logicalNameToObjectTypeCodeCache != null)
            {
                return;
            }

            _objectTypeCodeToLogicalNameCache = new Dictionary<int, string>();
            _logicalNameToObjectTypeCodeCache = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

            var request = new RetrieveAllEntitiesRequest
            {
                EntityFilters = EntityFilters.Entity
            };

            var response = (RetrieveAllEntitiesResponse)_service.Execute(request);

            foreach (var metadata in response.EntityMetadata)
            {
                if (!metadata.ObjectTypeCode.HasValue || string.IsNullOrWhiteSpace(metadata.LogicalName))
                {
                    continue;
                }

                var objectTypeCode = metadata.ObjectTypeCode.Value;

                if (!_objectTypeCodeToLogicalNameCache.ContainsKey(objectTypeCode))
                {
                    _objectTypeCodeToLogicalNameCache.Add(objectTypeCode, metadata.LogicalName);
                }

                if (!_logicalNameToObjectTypeCodeCache.ContainsKey(metadata.LogicalName))
                {
                    _logicalNameToObjectTypeCodeCache.Add(metadata.LogicalName, objectTypeCode);
                }
            }
        }

        private string ResolveDisplayName(string logicalName)
        {
            if (string.IsNullOrWhiteSpace(logicalName))
            {
                return string.Empty;
            }

            string cachedValue;
            if (_entityDisplayNameCache.TryGetValue(logicalName, out cachedValue))
            {
                return cachedValue;
            }

            try
            {
                var request = new RetrieveEntityRequest
                {
                    LogicalName = logicalName,
                    EntityFilters = EntityFilters.Entity
                };

                var response = (RetrieveEntityResponse)_service.Execute(request);
                var label = response.EntityMetadata != null &&
                            response.EntityMetadata.DisplayName != null &&
                            response.EntityMetadata.DisplayName.UserLocalizedLabel != null
                    ? response.EntityMetadata.DisplayName.UserLocalizedLabel.Label
                    : null;

                var value = string.IsNullOrWhiteSpace(label)
                    ? logicalName
                    : label;

                _entityDisplayNameCache[logicalName] = value;
                return value;
            }
            catch
            {
                _entityDisplayNameCache[logicalName] = logicalName;
                return logicalName;
            }
        }

        private string CalculateRiskSignal(PoaEntityHotspot hotspot, int grandTotal)
        {
            if (hotspot == null)
            {
                return "Low";
            }

            var percentOfTotal = hotspot.PercentOfTotal(grandTotal);
            var inheritedRatio = hotspot.TotalPoaRowCount <= 0
                ? 0m
                : Math.Round((decimal)hotspot.InheritedAccessCount / hotspot.TotalPoaRowCount * 100m, 2);

            if (
                hotspot.TotalPoaRowCount >= 50000 ||
                (percentOfTotal >= 20m && hotspot.TotalPoaRowCount >= 10000) ||
                (inheritedRatio >= 80m && hotspot.TotalPoaRowCount >= 5000)
            )
            {
                return "High";
            }

            if (
                hotspot.TotalPoaRowCount >= 10000 ||
                percentOfTotal >= 10m ||
                (inheritedRatio >= 50m && hotspot.TotalPoaRowCount >= 1000)
            )
            {
                return "Medium";
            }

            return "Low";
        }

        private string DecodeAccessRights(int accessMask)
        {
            if (accessMask <= 0)
            {
                return "None";
            }

            var rights = new List<string>();

            AddRightIfPresent(rights, accessMask, 0x00000001, "Read");
            AddRightIfPresent(rights, accessMask, 0x00000002, "Write");
            AddRightIfPresent(rights, accessMask, 0x00000004, "Append");
            AddRightIfPresent(rights, accessMask, 0x00000010, "Append To");
            AddRightIfPresent(rights, accessMask, 0x00000020, "Create");
            AddRightIfPresent(rights, accessMask, 0x00010000, "Delete");
            AddRightIfPresent(rights, accessMask, 0x00040000, "Share");
            AddRightIfPresent(rights, accessMask, 0x00080000, "Assign");

            if (rights.Count == 0)
            {
                return "Mask: " + accessMask;
            }

            return string.Join("/", rights);
        }

        private void AddRightIfPresent(ICollection<string> rights, int accessMask, int flag, string label)
        {
            if ((accessMask & flag) == flag)
            {
                rights.Add(label);
            }
        }

        private sealed class PoaSampleRow
        {
            public int ObjectTypeCode { get; set; }

            public Guid ObjectId { get; set; }

            public Guid PrincipalId { get; set; }

            public string PrincipalLogicalName { get; set; }

            public int AccessRightsMask { get; set; }

            public int InheritedAccessRightsMask { get; set; }
        }
    }
}
