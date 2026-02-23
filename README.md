# blackroad-data-pipeline

![CI](https://github.com/BlackRoad-Labs/blackroad-data-pipeline/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![License](https://img.shields.io/badge/license-proprietary-red)

ETL data pipeline framework for the BlackRoad OS platform.

## Features
- Sources: CSV, JSON, SQLite, inline
- Transforms: filter, map, aggregate, join, sort, deduplicate, select, rename
- Sinks: stdout, JSON, CSV, SQLite
- Dependency graph execution
- Schema validation

## Usage
```bash
python main.py create "My Pipeline"
python main.py run <pipeline-id>
python main.py list
python main.py validate <source-id>
```

## Testing
```bash
python -m pytest test_data_pipeline.py -v
```
