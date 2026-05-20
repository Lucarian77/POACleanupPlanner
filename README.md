# POA Cleanup Planner

POA Cleanup Planner is an XrmToolBox plugin designed to help administrators analyze principalobjectaccess (POA) growth within Microsoft Dataverse and Dynamics 365 environments.

The tool highlights POA hotspots, identifies entities and records contributing to concentrated access patterns, and provides cleanup planning guidance to support improved performance, security maintenance, and reduced database growth.

## Features
- Analyze POA growth by entity and identify the largest contributors
- Detect records with unusually high numbers of access grants
- Highlight users, teams, business units, and ownership patterns contributing to POA expansion
- Generate cleanup candidate groups for inactive, inherited, or concentrated access scenarios
- Estimate cleanup impact before any remediation work is performed
- Export findings to CSV for further review and reporting
- Display clear prioritization guidance for cleanup activities

## Key Benefits
- Helps reduce principalobjectaccess table growth
- Supports improved Dynamics 365 and Dataverse performance
- Identifies high-risk access concentration areas
- Provides visibility into inherited access and ownership-related POA expansion
- Assists with planning cleanup activities before implementing automation or bulk changes
- Helps administrators better understand the impact of security model design decisions

## Main Sections

### POA Hotspots
Displays the entities, records, or ownership patterns contributing the highest POA volume within the environment.
This section helps identify where the largest concentrations of access records exist and where cleanup efforts may provide the greatest value.
<img width="3762" height="1970" alt="image" src="https://github.com/user-attachments/assets/723e6c7f-3bc7-4461-bbd9-1fc5f8e75dba" />

### Principal Hotspots
Displays users, teams, or other principals with the highest POA concentration. This section helps identify whether POA growth is being driven by specific users, teams, access teams, or ownership/security model patterns.
The grid includes:
- Principal
- Principal type
- POA rows
- Percentage of scan
- Entities affected
- Records affected
- Dominant entity
- Access pattern
- Risk level
- Recommendation
<img width="3770" height="1805" alt="image" src="https://github.com/user-attachments/assets/80cbd3b5-4735-47b7-b7ee-d849ac0fc27e" />

### Record Hotspots
Displays individual records with concentrated POA access. This helps identify specific records that may be contributing to POA growth and should be reviewed after confirming entity-level and principal-level drivers.
The grid includes:
- Entity
- Record
- Record ID
- POA rows
- Percentage of scan
- Principal count
- User principal count
- Team principal count
- Access pattern
- Risk level
- Recommendation
<img width="3742" height="1792" alt="image" src="https://github.com/user-attachments/assets/2f7ba836-13ea-4475-8def-ac963242ea07" />

### Cleanup Candidates
Displays potential cleanup targets based on inactive records, inherited access patterns, business unit changes, ownership concentrations, or other high-volume scenarios.
This section is intended to help administrators prioritize which access records or security structures should be reviewed first.

<img width="3795" height="1050" alt="image" src="https://github.com/user-attachments/assets/78540719-df92-40d3-bded-46d7992e888b" />

### Impact Estimates
Displays estimated reductions in POA volume, cleanup effort levels, and expected benefit areas.
This section provides planning guidance before any cleanup work is performed.
<img width="3790" height="1117" alt="image" src="https://github.com/user-attachments/assets/20b09fa2-8a0a-4d30-8ea8-8f519eb62caa" />

### Export
Allows all analysis results to be exported for reporting, stakeholder review, or further analysis outside of XrmToolBox.
<img width="3550" height="1450" alt="image" src="https://github.com/user-attachments/assets/2ef45f4d-33ff-431a-b214-72fe971045b9" />

## Typical Use Cases
- Investigating large principalobjectaccess tables
- Identifying performance issues caused by excessive sharing
- Reviewing inherited access growth
- Planning business unit or team restructuring
- Evaluating the impact of record ownership models
- Supporting security cleanup initiatives
- Preparing for Dataverse storage optimization efforts

## Requirements
- XrmToolBox
- Microsoft Dataverse or Dynamics 365 environment
- Appropriate read access to security-related entities and metadata

## Version History
### Version 1.2026.1.3
* Added configurable scan depth presets for Quick, Standard, Deep, Full, and Custom scans
* Added deterministic POA paging for more consistent scan results
* Added entity, principal, and record-level POA hotspot analysis
* Added cleanup candidate review with advisory validation guidance
* Added cleanup impact scenarios
* Added detail dialogs with copy actions
* Added baseline snapshot output
* Added export preview output
* Added CSV/text export support
* Improved summary, scan quality, and log output
* Updated README, package metadata, NuGet release notes, and XrmToolBox packaging alignment

### Version 1.2026.1.2
* Updated analysis execution to use the XrmToolBox WorkAsync pattern
* Improved host responsiveness during Dataverse retrieval and hotspot enrichment
* Preserved summary, hotspot, candidate, impact, baseline snapshot, export preview, and log workflows
* Refined package documentation for Tool Library resubmission

### Version 1.2026.1.1
* Updated analysis execution to use the XrmToolBox asynchronous execution pattern
* Improved responsiveness during Dataverse retrieval and enrichment
* Preserved hotspot, candidate, impact, baseline snapshot, export preview, and log workflows
* Refined package metadata and Tool Library submission readiness

### Version 1.2026.1.0
* Initial public release
* Added POA hotspot analysis
* Added cleanup candidate recommendations
* Added cleanup impact estimation
* Added CSV export support
* Added prioritization guidance for cleanup planning
