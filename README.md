# Azure Data Pipeline

A real-time Python automation for moving data to Azure cloud using FastAPI.

## Overview

This project provides a robust, scalable solution for data ingestion, transformation, and upload to Azure cloud services. It supports various data sources (APIs, databases, files) and destinations (Blob Storage, Event Hubs).

## Features

- **Real-time data processing** with FastAPI
- **Multiple data sources**: API endpoints, databases, files
- **Multiple destinations**: Azure Blob Storage, Azure Event Hubs
- **Data transformation**: Filter, select, rename, aggregate, and custom transformations
- **Asynchronous processing**: Non-blocking I/O for high throughput
- **Job tracking**: Monitor processing status and progress
- **Metrics and monitoring**: Prometheus integration and logging
- **Containerization**: Docker support for easy deployment

## Prerequisites

- Python 3.8+
- Azure account with:
  - Blob Storage account
  - Event Hubs namespace
  - Table Storage account (for job tracking)

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/BisiOlaYemi/azure-data-pipeline.git
   cd azure-data-pipeline
   ```

2. Create a virtual environment:
   ```bash
   python -m venv venv
   source env/bin/activate  # For Windows: env\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Create a `.env` file from the example:
   ```bash
   cp .env.example .env
   ```

5. Edit the `.env` file with your Azure connection strings and settings.

## Usage

### Starting the server

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Or use the start script:

```bash
chmod +x scripts/start_server.sh
./scripts/start_server.sh
```

### Using the API

The API provides several endpoints for data processing:

1. **Data Ingestion**
   ```
   POST /api/v1/ingest
   ```
   Example payload:
   ```json
   {
     "source_type": "api",
     "source_url": "",
     "source_params": {"limit": 1000},
     "transformations": [
       {
         "type": "filter",
         "condition": "value > 100"
       },
       {
         "type": "select",
         "columns": ["id", "name", "value"]
       }
     ],
     "destination": "blob:my-container/processed/data.csv",
     "file_format": "csv"
   }
   ```

2. **File Upload**
   ```
   POST /api/v1/ingest/file
   ```
   Form data:
   - `file`: The file to upload
   - `destination`: The Azure blob storage path (e.g., "my-container/path/file.csv")

3. **Check Job Status**
   ```
   GET /api/v1/status/{job_id}
   ```

4. **Cancel Job**
   ```
   POST /api/v1/cancel/{job_id}
   ```

### Swagger Documentation

The API documentation is available at:

```
http://localhost:8000/docs
```

## Deployment

### Using Docker

1. Build the Docker image:
   ```bash
   docker build -t azure-data-pipeline .
   ```

2. Run the container:
   ```bash
   docker run -d -p 8000:8000 -p 9090:9090 --env-file .env azure-data-pipeline
   ```

### Using Azure Container Instances

Use the deployment script:

```bash
chmod +x scripts/deploy.sh
./scripts/deploy.sh --aci
```

## Monitoring

Prometheus metrics are available at:

```
http://localhost:9090
```

Log files are stored in the `logs` directory.

## Testing

Run the tests:

```bash
pytest
```

## License

MIT

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.