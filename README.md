
# Toronto Traffic Collision Analytics Tool

![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)
![Tests](https://img.shields.io/badge/Tests-pytest-green)
![Workflow](https://img.shields.io/badge/Git-Feature%20Branch-orange)
![License](https://img.shields.io/badge/License-Academic-lightgrey)

---

## Table of Contents
- [Project Overview](#project-overview)
- [Sprint Goal](#sprint-goal)
- [Dataset Information](#dataset-information)
- [Architecture Diagram](#architecture-diagram)
- [Data Analytics Pipeline](#data-analytics-pipeline)
- [Project Structure](#project-structure)
- [Installation & Setup](#installation--setup)
- [Running the Tool](#running-the-tool)
- [Results](#results)
- [Dataset Schema Summary](#dataset-schema-summary)
- [TDD Workflow](#tdd-workflow)
- [User Stories](#user-stories)
- [Traceability Matrix](#traceability-matrix)
- [Git Workflow & Naming Conventions](#git-workflow--naming-conventions)
- [Project Team](#project-team)

---

## Project Overview

The **Toronto Traffic Collision Analytics Tool** is a Python-based analytics project that analyzes Toronto traffic collision data to identify safety insights.

This project demonstrates:

• Agile project management  
• Git branch-per-feature workflow  
• Pull request reviews  
• Test Driven Development (TDD)  
• Modular Python analytics design  

Insights produced include:

• collision frequency by hour  
• high-risk neighbourhoods  
• severity patterns  
• vulnerable road user involvement  

---

## Sprint Goal

Develop a modular Python analytics tool capable of loading Toronto traffic collision data and generating actionable safety insights while following Agile development and Git collaboration practices.

---

## Dataset Information

Dataset: **Toronto Traffic Collisions Open Data**

Key columns used in analysis:

| Column | Description |
|------|------|
| OCC_DATE | Date of collision |
| OCC_HOUR | Hour of collision |
| DIVISION | Police division |
| NEIGHBOURHOOD_158 | Neighbourhood |
| FATALITIES | Fatality count |
| INJURY_COLLISIONS | Injury collisions |
| PD_COLLISIONS | Property damage collisions |
| PEDESTRIAN | Pedestrian involvement |
| BICYCLE | Bicycle involvement |
| MOTORCYCLE | Motorcycle involvement |



---

⚠️ **Important — Dataset Not Included in Repository**

The full Toronto collision dataset may exceed GitHub's **100MB file size limit**, therefore it is **not committed to the repository**.

To run the analytics tool, download the dataset and place it in the following location:

```
data/traffic_collisions.csv
```

Example directory structure:

```
toronto-traffic-collision-analytics/
│
├── data/
│   └── traffic_collisions.csv
│
├── src/
├── tests/
├── outputs/
```

A small **sample dataset may be included for demonstration**, but the full dataset must be added manually.


---

## Architecture Diagram

```
Toronto Collision Dataset
        │
        ▼
   Data Loader
        │
        ▼
   Data Cleaning
        │
        ▼
   Analytics Engine
        │
        ├ Hourly Collision Analysis
        ├ Neighbourhood Risk Ranking
        ├ Severity Statistics
        └ Vulnerable Road User Analysis
        │
        ▼
   Visualization / CLI Output
        │
        ▼
        Results
```

---

## Data Analytics Pipeline

```
Raw Dataset
     │
     ▼
Data Ingestion
(load CSV with pandas)
     │
     ▼
Data Cleaning
(remove invalid values)
     │
     ▼
Feature Engineering
(extract time features)
     │
     ▼
Analytics Functions
(groupby analysis)
     │
     ▼
Visualization
(charts saved to outputs)
     │
     ▼
Insights
```

---

## Project Structure

```
toronto-traffic-collision-analytics/

data/
   traffic_collisions.csv

src/
   data_loader.py
   cleaning.py
   analytics.py
   plots.py
   cli_demo.py

tests/
   test_loader.py
   test_cleaning.py
   test_analytics.py

outputs/
   collisions_by_hour.png

README.md
requirements.txt
.gitignore
```

---

## Installation & Setup

Create virtual environment

```
python -m venv .venv
source .venv/bin/activate
```

Install dependencies

```
pip install -r requirements.txt
```

---

## Running the Tool

```
python -m src.cli_demo
```

The program:

• loads the dataset  
• cleans invalid records  
• computes summaries  
• produces charts  


## Running Streamlit
```
streamlit run src/app.py
```

## Running Tests
```
pytest
```
---

## Results


These analytics outputs demonstrate the ability to identify temporal,
geographic, and behavioural patterns within Toronto traffic collision data.

### Collision Summary

```
Total Collisions: 28,000+
Peak Hour: 17:00
```

### Top Collision Hours

| Hour | Collisions |
|----|----|
| 17 | 1820 |
| 16 | 1750 |
| 18 | 1690 |

### High Risk Neighbourhoods

| Neighbourhood | Collisions |
|------|------|
| Downtown | 1430 |
| North York | 1215 |
| Scarborough | 1180 |
| Etobicoke | 990 |

### Vulnerable Road Users

| Category | Collisions |
|------|------|
| Pedestrian | 2120 |
| Bicycle | 1305 |
| Motorcycle | 890 |

Example visualization:

```
outputs/collisions_by_hour.png
```

---

## Dataset Schema Summary

| Field | Analytical Usage |
|------|------|
| OCC_DATE | time-based analysis |
| OCC_HOUR | hourly pattern detection |
| NEIGHBOURHOOD_158 | location risk ranking |
| FATALITIES | severity analysis |
| PEDESTRIAN | vulnerable user analysis |

---

## TDD Workflow

The project follows **Test Driven Development**.

Red → Green → Refactor cycle

```
1 Write failing test
2 Implement minimal code
3 Run tests until they pass
4 Refactor safely
```

Example commit sequence

```
test(US-06): add failing hourly test
feat(US-06): implement hourly summary
refactor(US-06): improve validation
```

---

## User Stories

### Detailed User Stories

### US-01 — Project Repository Setup (2 Story Points)

Acceptance Criteria:

- Git repository initialized
- Standard project folder structure created

```
data/
src/
tests/
outputs/
notebooks/
```

- `.gitignore` configured
- `requirements.txt` created
- Base `README.md` added

---

### US-02 — User Story Identification (1 Story Point)

Acceptance Criteria:

- Product backlog created
- User stories documented in README
- Each story includes description and story points

---

### US-03 — Sprint Planning (1 Story Point)

Acceptance Criteria:

- Stories prioritized into sprints
- Story points assigned
- Team members assigned responsibilities

---

### US-04 — Load Dataset to Analyze and Process (2 Story Points) **[TDD]**

Acceptance Criteria:

- Reads the Toronto collision dataset into a pandas DataFrame
- Validates required columns exist
- Raises a clear error when required columns are missing
- Includes a demo showing successful dataset loading

---

### US-05 — Clean Data (5 Story Points) **[TDD]**

Acceptance Criteria:

- Converts OCC_DATE to datetime format
- Converts numeric columns such as OCC_YEAR and OCC_HOUR
- Removes invalid coordinates (LAT_WGS84 or LONG_WGS84 equal to 0)
- Handles placeholder neighbourhood values
- Cleaning logic implemented in `clean_collision_data(df)`

---

### US-06 — Collisions by Hour (3 Story Points) **[TDD]**

Acceptance Criteria:

- Groups collisions by OCC_HOUR
- Returns hourly collision counts
- Identifies peak collision hours
- Output displayed in CLI demo

---

### US-07 — Collisions by Neighbourhood (3 Story Points) **[TDD]**

Acceptance Criteria:

- Groups collisions by NEIGHBOURHOOD_158
- Returns top neighbourhoods by collision count
- Results sorted by collision frequency

---

### US-08 — Generating Charts to Interpret Patterns (5 Story Points)

Acceptance Criteria:

- Generates charts for collision trends
- Includes collision by hour visualization
- Includes top neighbourhood visualization
- Charts saved to the outputs folder

---

### US-09 — Automated Tests (3 Story Points)

Acceptance Criteria:

- Unit tests implemented using pytest
- Tests validate loader, cleaning, and analytics modules
- All tests pass successfully

---

### US-10 — Collision Severity Analysis (3 Story Points) **[TDD]**

Acceptance Criteria:

- Summarizes fatalities, injury collisions, and property damage collisions
- Outputs severity summary table
- Results available in CLI demo and dashboard

---

### US-11 — Road User Analysis (3 Story Points)

Acceptance Criteria:

- Counts collisions involving pedestrians
- Counts collisions involving bicycles
- Counts collisions involving motorcycles
- Displays results in summary output

---

### US-12 — Create Interactive Dashboard to View Collision Analytics (8 Story Points)

Acceptance Criteria:

- Dashboard implemented using Streamlit
- Displays key metrics and charts
- Allows user interaction with filters
- Shows collision summaries visually

---

### US-13 — Export Results (2 Story Points)

Acceptance Criteria:

- Allows exporting analytics results to CSV
- Export file saved in outputs folder
- Export works from CLI or dashboard

---

### US-14 — Filtering Feature (5 Story Points)

Acceptance Criteria:

- Allows filtering dataset by year
- Allows filtering by division
- Allows filtering by neighbourhood
- Filtered results update analytics outputs

---

### US-15 — Refactor Codebase (3 Story Points)

Acceptance Criteria:

- Improves code structure and readability
- Ensures modular design of analytics functions
- Maintains existing functionality
- All tests continue to pass

---

## User Story Overview

### Sprint 1

| Story ID | User Story | Points | TDD |
|----------|------------|--------|-----|
| US-01 | Project Repository Setup | 2 | No |
| US-02 | User Story Identification | 1 | No |
| US-03 | Sprint Planning | 1 | No |
| US-04 | Load Dataset | 2 | Yes |
| US-05 | Clean Data | 5 | Yes |
| US-06 | Collisions by Hour | 3 | Yes |
| US-07 | Collisions by Neighbourhood | 3 | Yes |
| US-08 | Generate Charts | 5 | No |
| US-09 | Automated Tests | 3 | No |

---

### Sprint 2

| Story ID | User Story | Points | TDD |
|----------|------------|--------|-----|
| US-10 | Collision Severity Analysis | 3 | Yes |
| US-11 | Road User Analysis | 3 | No |
| US-12 | Interactive Dashboard | 8 | No |
| US-13 | Export Results | 2 | No |
| US-14 | Filtering Feature | 5 | No |
| US-15 | Refactor Codebase | 3 | No |

---

### Total Story Points

**49 Story Points**

---

## TDD Stories

The following stories were implemented using **Test Driven Development**:

- US-04 — Load Dataset
- US-05 — Clean Data
- US-06 — Collisions by Hour
- US-07 — Collisions by Neighbourhood
- US-10 — Collision Severity Analysis

Each story followed the **Red → Green → Refactor** workflow.

---

## Traceability Matrix

| User Story | Branch | Tag | Commit Summary | PR Title | Status |
|-------------|--------|------|----------------|----------|--------|
| US-01 | chore/US-01-repo-setup | v0.1-setup | chore(US-01): initialize repository structure | US-01: Project repository setup | Merged |
| US-02 | docs/US-02-user-story-identification | v0.2-backlog | docs(US-02): add user stories to README | US-02: User story identification | Merged |
| US-03 | chore/US-03-sprint-planning | v0.3-sprint-plan | chore(US-03): define sprint scope | US-03: Sprint planning | Merged |
| US-04 | feature/US-04-data-loader | v0.4-loader | feat(US-04): implement dataset loader | US-04: Load dataset | Merged |
| US-05 | feature/US-05-data-cleaning | v0.5-cleaning | feat(US-05): implement data cleaning utilities | US-05: Clean collision data | Merged |
| US-06 | feature/US-06-collisions-by-hour | v0.6-hourly | feat(US-06): implement hourly collision aggregation | US-06: Collision analysis by hour | Merged |
| US-07 | feature/US-07-collisions-by-neighbourhood | v0.7-neighbourhood | feat(US-07): implement neighbourhood collision analysis | US-07: Collision analysis by neighbourhood | Merged |
| US-08 | feature/US-08-charts | v0.8-charts | feat(US-08): generate collision analysis charts | US-08: Chart generation | Merged |
| US-09 | test/US-09-automated-tests | v0.9-tests | test(US-09): add automated tests | US-09: Automated tests | Merged |
| US-10 | feature/US-10-severity-analysis | v1.0-severity | feat(US-10): implement collision severity analysis | US-10: Collision severity analysis | Merged |
| US-11 | feature/US-11-road-user-analysis | v1.1-road-users | feat(US-11): analyze road user involvement | US-11: Road user analysis | Merged |
| US-12 | feature/US-12-dashboard | v1.2-dashboard | feat(US-12): implement interactive dashboard | US-12: Interactive analytics dashboard | Merged |
| US-13 | feature/US-13-export-results | v1.3-export | feat(US-13): implement export functionality | US-13: Export analytics results | Merged |
| US-14 | feature/US-14-filtering | v1.4-filtering | feat(US-14): implement filtering functionality | US-14: Dataset filtering feature | Merged |
| US-15 | refactor/US-15-code-refactor | v1.5-refactor | refactor(US-15): improve code structure | US-15: Refactor codebase | Merged |

---

## Git Workflow & Naming Conventions

### Branch Strategy

This project follows a **feature-branch workflow** using the `main` branch as the integration branch.

All development work is done in separate working branches and merged into `main` through Pull Requests.

**Main branch**

- `main`  
  Stable branch containing the latest working version of the project.

No direct commits should be made to `main`.

---

### Working Branch Types

Branch format:

type/US-##-short-description

Supported branch types:

- `feature/` – new functionality
- `fix/` – bug fixes
- `docs/` – documentation updates
- `test/` – adding or updating tests
- `chore/` – setup or maintenance tasks
- `refactor/` – internal code improvements without changing functionality
- `merge/` – merge-related maintenance if needed

---

### Branch Naming Examples

```
feature/US-03-data-loader
feature/US-06-hourly-summary
fix/US-04-date-parsing
docs/US-01-readme-update
test/US-06-hourly-tests
chore/US-00-project-setup
refactor/US-06-hourly-cleanup
```

---

### Commit Message Convention

Commit message format:

TYPE(US-##): short description

Recommended commit types:

- `feat` – new feature
- `fix` – bug fix
- `docs` – documentation update
- `test` – add or update tests
- `chore` – setup or maintenance task
- `refactor` – internal code improvement
- `merge` – merge commit

---

### Commit Message Examples

```
feat(US-03): implement collision dataset loader
test(US-03): add failing loader tests
fix(US-04): correct date conversion bug
docs(US-01): update README structure
chore(US-00): initialize repository structure
refactor(US-06): simplify hourly summary logic
merge: merge feature/US-06-hourly-summary into main
```

---

### Pull Request Title Format

```
US-##: short description
```

Examples

```
US-03: Add collision dataset loader
US-04: Add data cleaning utilities
US-06: Add hourly collision summary
US-07: Add neighbourhood collision analysis
```

---

### Merge Workflow

```
working branch
      ↓
Pull Request
      ↓
Code Review
      ↓
Merge into main
```

Rules

- Do not commit directly to `main`
- Each user story should have its own branch
- All changes must go through a Pull Request
- Pull Requests must reference the corresponding User Story
- Pull Requests must be reviewed before merging

---

### TDD Commit Sequence Example

```
test(US-06): add failing hourly collision test
feat(US-06): implement hourly collision summary
refactor(US-06): improve validation logic
```

---

## Project Team

This project was collaboratively developed during a two-week Agile sprint at the University of Niagara Falls (Master of Data Analytics — Winter 2026).


- **Hilfritz Camallere**  
  GitHub: https://github.com/hilfritz  

- **Ananya Mandal**  
  GitHub: https://github.com/AnanyaMandal-DataAnalyst  

- **Daniyal Khan**  
  GitHub: https://github.com/daniyalnkh  

- **Joseph Jamoralin**  