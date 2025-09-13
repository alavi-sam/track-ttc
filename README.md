# Track TTC

Real-time TTC data ETL pipeline using Apache Airflow, PostgreSQL, and MinIO.

## Overview

This project ingests real-time GTFS data from TTC's API, stores it in MinIO, and processes it into PostgreSQL for analysis. The pipeline joins real-time data with static GTFS data to provide comprehensive transit information.

## Architecture

- **Data Source**: [TTC GTFS-RT API](https://bustime.ttc.ca/gtfsrt/)
- **Storage**: MinIO (object storage)
- **Database**: PostgreSQL
- **Orchestration**: Apache Airflow
- **Containerization**: Docker & Docker Compose

## Directory Structure

```
track-ttc/
├── dags/                    # Airflow DAGs
├── plugins/                 # Custom Airflow components
├── src/                     # ETL business logic
├── sql/                     # Database scripts
├── data/                    # Static GTFS data
├── docker/                  # Docker configurations
├── config/                  # Configuration files
└── tests/                   # Unit and integration tests
```

## Prerequisites

- Docker & Docker Compose
- Python 3.9+
- Git

## Quick Start

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd track-ttc
   ```

2. **Set up environment**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

3. **Start services**
   ```bash
   docker-compose up -d
   ```

4. **Access services**
   - Airflow: http://localhost:8080
   - MinIO Console: http://localhost:9001
   - PostgreSQL: localhost:5432

## Environment Variables

Required environment variables in `.env`:

```
POSTGRES_USER=postgres
POSTGRES_PASSWORD=SimplePassword
POSTGRES_DB=trackTTC
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
```

## Development

### Running Tests
```bash
python -m pytest tests/
```

### Database Connection Test
```bash
python test.py
```

## Data Pipeline

1. **Extract**: Fetch GTFS-RT data from TTC API
2. **Load**: Store raw data in MinIO
3. **Transform**: Process and join with static GTFS data
4. **Load**: Insert processed data into PostgreSQL

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

MIT License