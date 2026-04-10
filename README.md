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
<img width="3745" height="1625" alt="image" src="https://github.com/user-attachments/assets/8c33f67a-4517-490d-afed-64a250762036" />

### Cleanup Candidates
Displays potential cleanup targets based on inactive records, inherited access patterns, business unit changes, ownership concentrations, or other high-volume scenarios.
This section is intended to help administrators prioritize which access records or security structures should be reviewed first.
<img width="3755" height="1792" alt="image" src="https://github.com/user-attachments/assets/983d75f2-57b0-4618-b9b9-e4d602a5e4ce" />

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
### Version 1.2026.1.0
- Initial public release
- Added POA hotspot analysis
- Added cleanup candidate recommendations
- Added cleanup impact estimation
- Added CSV export support
- Added prioritization guidance for cleanup planning
