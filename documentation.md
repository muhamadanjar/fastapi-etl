# FastAPI ETL - API Documentation

Complete guide untuk menggunakan FastAPI ETL API dari upload file sampai monitoring hasil.

---

## Table of Contents

1. [Getting Started](#getting-started)
2. [Authentication](#authentication)
3. [File Upload & Processing](#file-upload--processing)
4. [ETL Job Management](#etl-job-management)
5. [Job Dependencies](#job-dependencies)
6. [Monitoring & Status](#monitoring--status)
7. [Error Management](#error-management)
8. [Data Quality](#data-quality)
9. [Complete Workflow Examples](#complete-workflow-examples)

---

## Getting Started

### Base URL
```
http://localhost:8000/api/v1
```

### Prerequisites
- API running on port 8000
- Database migrated: `alembic upgrade head`
- Celery worker running: `celery -A app.tasks.celery_app worker --loglevel=info`

---

## Authentication

### Login
```bash
POST /auth/login
Content-Type: application/json

{
  "username": "your_username",
  "password": "your_password"
}
```

**Response**:
```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "bearer",
  "user_id": "uuid-here"
}
```

**Use token in subsequent requests**:
```bash
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

---

## File Upload & Processing

### Step 1: Upload File

```bash
POST /files/upload
Authorization: Bearer {token}
Content-Type: multipart/form-data

file: [your-file.csv]
```

**Example with curl**:
```bash
curl -X POST http://localhost:8000/api/v1/files/upload \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -F "file=@/path/to/data.csv"
```

**Response**:
```json
{
  "file_id": "550e8400-e29b-41d4-a716-446655440000",
  "filename": "data.csv",
  "file_type": "csv",
  "file_size": 1024000,
  "file_path": "/uploads/data.csv",
  "processing_status": "PENDING",
  "uploaded_at": "2025-11-22T10:00:00"
}
```

**Supported File Types**:
- CSV (`.csv`)
- Excel (`.xlsx`, `.xls`)
- JSON (`.json`)
- XML (`.xml`)

---

### Step 2: Process Uploaded File

Processing configuration varies by file type. Below are examples for each supported format.

#### 2.1 Process CSV File

```bash
POST /files/{file_id}/process
Authorization: Bearer {token}
Content-Type: application/json

{
  "processing_config": {
    "delimiter": ",",           # Delimiter character (default: ",")
    "encoding": "utf-8",        # File encoding (default: "utf-8")
    "skip_rows": 0,             # Number of rows to skip (default: 0)
    "chunk_size": 1000,         # Records per chunk (default: 1000)
    "has_header": true,         # First row is header (default: true)
    "quote_char": "\"",         # Quote character (default: "\"")
    "escape_char": "\\",        # Escape character (default: "\\")
    "null_values": ["", "NULL", "null", "N/A"]  # Values to treat as null
  }
}
```

**CSV Example with Different Delimiters**:

```bash
# Semicolon-separated (common in European formats)
{
  "processing_config": {
    "delimiter": ";",
    "encoding": "utf-8"
  }
}

# Tab-separated (TSV)
{
  "processing_config": {
    "delimiter": "\t",
    "encoding": "utf-8"
  }
}

# Pipe-separated
{
  "processing_config": {
    "delimiter": "|",
    "encoding": "utf-8"
  }
}
```

**CSV with Custom Encoding**:
```bash
# For files with special characters
{
  "processing_config": {
    "delimiter": ",",
    "encoding": "latin-1"  # or "iso-8859-1", "windows-1252"
  }
}
```

---

#### 2.2 Process Excel File

```bash
POST /files/{file_id}/process
Authorization: Bearer {token}
Content-Type: application/json

{
  "processing_config": {
    "sheet_name": "Sheet1",     # Sheet to process (default: first sheet)
    "skip_rows": 0,             # Rows to skip from top
    "skip_footer": 0,           # Rows to skip from bottom
    "use_column_names": true,   # Use first row as column names
    "chunk_size": 1000,         # Records per chunk
    "date_format": "%Y-%m-%d",  # Date parsing format
    "columns": ["A", "B", "C"]  # Specific columns to read (optional)
  }
}
```

**Excel Examples**:

```bash
# Process specific sheet by name
{
  "processing_config": {
    "sheet_name": "Customer Data",
    "skip_rows": 2  # Skip first 2 rows (e.g., title and empty row)
  }
}

# Process sheet by index (0-based)
{
  "processing_config": {
    "sheet_name": 0,  # First sheet
    "use_column_names": true
  }
}

# Process multiple sheets
{
  "processing_config": {
    "sheet_name": ["Sheet1", "Sheet2"],  # Process multiple sheets
    "chunk_size": 500
  }
}

# Read specific columns only
{
  "processing_config": {
    "sheet_name": "Sales",
    "columns": ["A", "C", "E", "G"],  # Read columns A, C, E, G only
    "use_column_names": true
  }
}
```

---

#### 2.3 Process JSON File

```bash
POST /files/{file_id}/process
Authorization: Bearer {token}
Content-Type: application/json

{
  "processing_config": {
    "json_path": "$",           # JSONPath expression (default: root)
    "array_path": "data",       # Path to array of records
    "chunk_size": 1000,         # Records per chunk
    "flatten_nested": true,     # Flatten nested objects
    "max_depth": 3,             # Max nesting depth to flatten
    "date_fields": ["created_at", "updated_at"],  # Fields to parse as dates
    "encoding": "utf-8"
  }
}
```

**JSON Structure Examples**:

**Simple Array**:
```json
// File: customers.json
[
  {"id": 1, "name": "John", "email": "john@example.com"},
  {"id": 2, "name": "Jane", "email": "jane@example.com"}
]
```
```bash
# Processing config
{
  "processing_config": {
    "json_path": "$",  # Root is already an array
    "chunk_size": 1000
  }
}
```

**Nested Object with Array**:
```json
// File: api_response.json
{
  "status": "success",
  "data": {
    "customers": [
      {"id": 1, "name": "John"},
      {"id": 2, "name": "Jane"}
    ]
  }
}
```
```bash
# Processing config
{
  "processing_config": {
    "array_path": "data.customers",  # Path to the array
    "chunk_size": 1000
  }
}
```

**Deeply Nested Objects**:
```json
// File: complex.json
{
  "results": [
    {
      "user": {
        "id": 1,
        "profile": {
          "name": "John",
          "address": {
            "city": "Jakarta",
            "country": "Indonesia"
          }
        }
      }
    }
  ]
}
```
```bash
# Processing config with flattening
{
  "processing_config": {
    "array_path": "results",
    "flatten_nested": true,
    "max_depth": 3  # Will create: user_id, user_profile_name, user_profile_address_city
  }
}
```

**JSON Lines (NDJSON)**:
```json
// File: logs.jsonl (each line is a JSON object)
{"timestamp": "2025-11-23T10:00:00", "level": "INFO", "message": "Started"}
{"timestamp": "2025-11-23T10:00:01", "level": "ERROR", "message": "Failed"}
```
```bash
# Processing config
{
  "processing_config": {
    "format": "jsonlines",  # Specify JSONL format
    "chunk_size": 5000,
    "date_fields": ["timestamp"]
  }
}
```

---

#### 2.4 Process XML File

```bash
POST /files/{file_id}/process
Authorization: Bearer {token}
Content-Type: application/json

{
  "processing_config": {
    "root_element": "records",      # Root element containing records
    "record_element": "record",     # Element representing each record
    "chunk_size": 1000,             # Records per chunk
    "namespace_aware": false,       # Handle XML namespaces
    "flatten_attributes": true,     # Include XML attributes as fields
    "text_field_name": "_text",     # Field name for element text content
    "encoding": "utf-8"
  }
}
```

**XML Structure Examples**:

**Simple XML**:
```xml
<!-- File: customers.xml -->
<?xml version="1.0" encoding="UTF-8"?>
<customers>
  <customer>
    <id>1</id>
    <name>John Doe</name>
    <email>john@example.com</email>
  </customer>
  <customer>
    <id>2</id>
    <name>Jane Smith</name>
    <email>jane@example.com</email>
  </customer>
</customers>
```
```bash
# Processing config
{
  "processing_config": {
    "root_element": "customers",
    "record_element": "customer",
    "chunk_size": 1000
  }
}
```

**XML with Attributes**:
```xml
<!-- File: products.xml -->
<?xml version="1.0"?>
<products>
  <product id="1" category="electronics">
    <name>Laptop</name>
    <price currency="USD">999.99</price>
  </product>
  <product id="2" category="books">
    <name>Python Guide</name>
    <price currency="USD">49.99</price>
  </product>
</products>
```
```bash
# Processing config with attributes
{
  "processing_config": {
    "root_element": "products",
    "record_element": "product",
    "flatten_attributes": true  # Will create: id, category, name, price, price_currency
  }
}
```

**XML with Namespaces**:
```xml
<!-- File: soap_response.xml -->
<?xml version="1.0"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <m:GetCustomersResponse xmlns:m="http://example.com/customers">
      <m:Customer>
        <m:ID>1</m:ID>
        <m:Name>John</m:Name>
      </m:Customer>
    </m:GetCustomersResponse>
  </soap:Body>
</soap:Envelope>
```
```bash
# Processing config with namespaces
{
  "processing_config": {
    "root_element": "{http://example.com/customers}GetCustomersResponse",
    "record_element": "{http://example.com/customers}Customer",
    "namespace_aware": true,
    "namespaces": {
      "soap": "http://schemas.xmlsoap.org/soap/envelope/",
      "m": "http://example.com/customers"
    }
  }
}
```

**Nested XML**:
```xml
<!-- File: orders.xml -->
<orders>
  <order>
    <id>1</id>
    <customer>
      <name>John</name>
      <email>john@example.com</email>
    </customer>
    <items>
      <item>
        <product>Laptop</product>
        <quantity>1</quantity>
      </item>
    </items>
  </order>
</orders>
```
```bash
# Processing config for nested XML
{
  "processing_config": {
    "root_element": "orders",
    "record_element": "order",
    "flatten_nested": true,  # Will flatten customer and items
    "max_depth": 2
  }
}
```

---

### Complete Processing Examples by Format

#### Example 1: CSV with Custom Settings

```bash
curl -X POST http://localhost:8000/api/v1/files/$FILE_ID/process \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "processing_config": {
      "delimiter": ";",
      "encoding": "latin-1",
      "skip_rows": 1,
      "has_header": true,
      "null_values": ["", "N/A", "-"]
    }
  }'
```

#### Example 2: Excel Multi-Sheet

```bash
curl -X POST http://localhost:8000/api/v1/files/$FILE_ID/process \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "processing_config": {
      "sheet_name": "Sales Data",
      "skip_rows": 2,
      "use_column_names": true,
      "date_format": "%d/%m/%Y"
    }
  }'
```

#### Example 3: JSON API Response

```bash
curl -X POST http://localhost:8000/api/v1/files/$FILE_ID/process \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "processing_config": {
      "array_path": "data.results",
      "flatten_nested": true,
      "max_depth": 3,
      "date_fields": ["created_at", "updated_at"]
    }
  }'
```

#### Example 4: XML SOAP Response

```bash
curl -X POST http://localhost:8000/api/v1/files/$FILE_ID/process \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "processing_config": {
      "root_element": "{http://example.com}Response",
      "record_element": "{http://example.com}Record",
      "namespace_aware": true,
      "flatten_attributes": true
    }
  }'
```

---

### Step 3: Check File Processing Status

```bash
GET /files/{file_id}
Authorization: Bearer {token}
```

**Response**:
```json
{
  "file_id": "550e8400-e29b-41d4-a716-446655440000",
  "filename": "data.csv",
  "file_type": "csv",
  "processing_status": "COMPLETED",
  "records_processed": 5000,
  "records_successful": 4950,
  "records_failed": 50,
  "processed_at": "2025-11-22T10:05:00",
  "processing_metadata": {
    "delimiter_used": ",",
    "encoding_detected": "utf-8",
    "columns_found": 15,
    "data_types": {
      "id": "integer",
      "name": "string",
      "email": "string",
      "created_at": "datetime"
    }
  }
}
```

**Processing Status Values**:
- `PENDING` - Waiting to be processed
- `PROCESSING` - Currently processing
- `COMPLETED` - Successfully processed
- `FAILED` - Processing failed

**Format-Specific Metadata**:

**CSV**:
```json
{
  "processing_metadata": {
    "delimiter_used": ",",
    "encoding_detected": "utf-8",
    "rows_total": 5000,
    "columns_count": 15,
    "header_row": ["id", "name", "email", "phone", "address"]
  }
}
```

**Excel**:
```json
{
  "processing_metadata": {
    "sheets_processed": ["Sheet1", "Sheet2"],
    "total_sheets": 3,
    "rows_per_sheet": {"Sheet1": 3000, "Sheet2": 2000},
    "columns_count": 12
  }
}
```

**JSON**:
```json
{
  "processing_metadata": {
    "json_structure": "nested_array",
    "array_path_used": "data.customers",
    "objects_found": 5000,
    "max_nesting_depth": 3,
    "flattened_fields": 25
  }
}
```

**XML**:
```json
{
  "processing_metadata": {
    "root_element": "customers",
    "record_element": "customer",
    "records_found": 5000,
    "namespaces_detected": ["soap", "xsi"],
    "attributes_flattened": true
  }
}
```

---

## ETL Job Management

### Step 1: Create ETL Job

```bash
POST /jobs
Authorization: Bearer {token}
Content-Type: application/json

{
  "job_name": "Customer Data ETL",
  "job_type": "FULL_ETL",
  "description": "Extract, transform, and load customer data",
  "source_type": "FILE",
  "source_config": {
    "file_pattern": "customers_*.csv",
    "directory": "/data/customers"
  },
  "transformation_rules": [
    {
      "rule_type": "FIELD_MAPPING",
      "source_field": "customer_name",
      "target_field": "name",
      "transformation": "UPPERCASE"
    }
  ],
  "schedule": "0 2 * * *",
  "is_active": true
}
```

**Job Types**:
- `EXTRACT` - Extract data only
- `TRANSFORM` - Transform data only
- `LOAD` - Load data only
- `FULL_ETL` - Complete ETL pipeline

**Response**:
```json
{
  "job_id": "660e8400-e29b-41d4-a716-446655440000",
  "job_name": "Customer Data ETL",
  "job_type": "FULL_ETL",
  "status": "created",
  "created_at": "2025-11-22T10:10:00"
}
```

---

### Step 2: Execute ETL Job

```bash
POST /jobs/{job_id}/execute
Authorization: Bearer {token}
Content-Type: application/json

{
  "parameters": {
    "batch_size": 1000,
    "parallel_processing": true
  }
}
```

**Example**:
```bash
curl -X POST http://localhost:8000/api/v1/jobs/660e8400-e29b-41d4-a716-446655440000/execute \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "parameters": {
      "batch_size": 1000
    }
  }'
```

**Response**:
```json
{
  "execution_id": "770e8400-e29b-41d4-a716-446655440000",
  "job_id": "660e8400-e29b-41d4-a716-446655440000",
  "batch_id": "batch-123",
  "status": "started",
  "task_id": "celery-task-456",
  "dependencies_checked": true,
  "dependencies_met": 2
}
```

---

### Step 3: Get Job Execution Status

```bash
GET /jobs/{job_id}/executions/{execution_id}
Authorization: Bearer {token}
```

**Response**:
```json
{
  "execution_id": "770e8400-e29b-41d4-a716-446655440000",
  "job_id": "660e8400-e29b-41d4-a716-446655440000",
  "status": "SUCCESS",
  "start_time": "2025-11-22T10:15:00",
  "end_time": "2025-11-22T10:20:00",
  "duration_seconds": 300,
  "records_processed": 10000,
  "records_successful": 9950,
  "records_failed": 50,
  "performance_metrics": {
    "records_per_second": 33.3,
    "memory_usage_mb": 256,
    "cpu_usage_percent": 45
  }
}
```

**Execution Status Values**:
- `PENDING` - Waiting to start
- `RUNNING` - Currently executing
- `SUCCESS` - Completed successfully
- `FAILED` - Execution failed
- `CANCELLED` - Cancelled by user

---

### Step 4: List All Jobs

```bash
GET /jobs?status=active&limit=10&offset=0
Authorization: Bearer {token}
```

**Response**:
```json
{
  "jobs": [
    {
      "job_id": "660e8400-e29b-41d4-a716-446655440000",
      "job_name": "Customer Data ETL",
      "job_type": "FULL_ETL",
      "is_active": true,
      "last_execution": "2025-11-22T10:20:00",
      "success_rate": 98.5
    }
  ],
  "total": 1,
  "limit": 10,
  "offset": 0
}
```

---

## Job Dependencies

### Step 1: Add Job Dependency

```bash
POST /jobs/{child_job_id}/dependencies
Authorization: Bearer {token}
Content-Type: application/json

{
  "parent_job_id": "660e8400-e29b-41d4-a716-446655440000",
  "dependency_type": "SUCCESS",
  "description": "Customer data must be extracted before transformation"
}
```

**Dependency Types**:
- `SUCCESS` - Parent must complete successfully
- `COMPLETION` - Parent must complete (success or failed)
- `DATA_AVAILABILITY` - Parent must produce data

**Response**:
```json
{
  "success": true,
  "message": "Dependency added successfully",
  "data": {
    "dependency_id": "880e8400-e29b-41d4-a716-446655440000",
    "parent_job_id": "660e8400-e29b-41d4-a716-446655440000",
    "parent_job_name": "Extract Customer Data",
    "child_job_id": "990e8400-e29b-41d4-a716-446655440000",
    "child_job_name": "Transform Customer Data",
    "dependency_type": "SUCCESS"
  }
}
```

---

### Step 2: Check Dependencies Status

```bash
GET /jobs/{job_id}/dependencies/check
Authorization: Bearer {token}
```

**Response**:
```json
{
  "success": true,
  "message": "All dependencies met",
  "data": {
    "dependencies_met": true,
    "total_dependencies": 2,
    "met_dependencies": 2,
    "unmet_dependencies": []
  }
}
```

**If dependencies not met**:
```json
{
  "success": true,
  "message": "Some dependencies not met",
  "data": {
    "dependencies_met": false,
    "total_dependencies": 2,
    "met_dependencies": 1,
    "unmet_dependencies": [
      {
        "parent_job_id": "660e8400-e29b-41d4-a716-446655440000",
        "parent_job_name": "Extract Customer Data",
        "dependency_type": "SUCCESS",
        "reason": "Parent job has never been executed",
        "latest_status": null
      }
    ]
  }
}
```

---

### Step 3: View Dependency Tree

```bash
GET /jobs/{job_id}/dependency-tree?max_depth=5
Authorization: Bearer {token}
```

**Response**:
```json
{
  "success": true,
  "message": "Dependency tree retrieved successfully",
  "data": {
    "root_job_id": "990e8400-e29b-41d4-a716-446655440000",
    "tree": {
      "job_id": "990e8400-e29b-41d4-a716-446655440000",
      "job_name": "Transform Customer Data",
      "job_type": "TRANSFORM",
      "depth": 0,
      "parents": [
        {
          "dependency_type": "SUCCESS",
          "job_id": "660e8400-e29b-41d4-a716-446655440000",
          "job_name": "Extract Customer Data",
          "job_type": "EXTRACT",
          "depth": 1,
          "parents": []
        }
      ]
    },
    "total_jobs_in_tree": 2
  }
}
```

---

### Step 4: Get Executable Jobs

```bash
GET /jobs/executable
Authorization: Bearer {token}
```

**Response**:
```json
{
  "success": true,
  "message": "Found 3 executable jobs",
  "data": {
    "executable_jobs": [
      {
        "job_id": "660e8400-e29b-41d4-a716-446655440000",
        "job_name": "Extract Customer Data",
        "job_type": "EXTRACT",
        "total_dependencies": 0
      },
      {
        "job_id": "aa0e8400-e29b-41d4-a716-446655440000",
        "job_name": "Load Product Data",
        "job_type": "LOAD",
        "total_dependencies": 1
      }
    ],
    "total": 2
  }
}
```

---

## Monitoring & Status

### Get Job Execution History

```bash
GET /jobs/{job_id}/executions?limit=10&offset=0
Authorization: Bearer {token}
```

**Response**:
```json
{
  "executions": [
    {
      "execution_id": "770e8400-e29b-41d4-a716-446655440000",
      "status": "SUCCESS",
      "start_time": "2025-11-22T10:15:00",
      "end_time": "2025-11-22T10:20:00",
      "records_processed": 10000,
      "records_successful": 9950,
      "records_failed": 50
    }
  ],
  "total": 1,
  "limit": 10,
  "offset": 0
}
```

---

### Get System Monitoring

```bash
GET /monitoring/system-status
Authorization: Bearer {token}
```

**Response**:
```json
{
  "status": "healthy",
  "celery_workers": 4,
  "active_tasks": 2,
  "pending_tasks": 5,
  "database_connections": 10,
  "redis_status": "connected",
  "uptime_seconds": 86400
}
```

---

## Error Management

### Step 1: Get Errors for Execution

```bash
GET /errors/executions/{execution_id}/errors?limit=10
Authorization: Bearer {token}
```

**Response**:
```json
{
  "success": true,
  "message": "Retrieved 2 errors for execution",
  "data": {
    "errors": [
      {
        "error_id": "bb0e8400-e29b-41d4-a716-446655440000",
        "job_execution_id": "770e8400-e29b-41d4-a716-446655440000",
        "error_type": "VALIDATION_ERROR",
        "error_severity": "MEDIUM",
        "error_message": "Invalid email format",
        "occurred_at": "2025-11-22T10:16:30",
        "is_resolved": false
      }
    ],
    "total_count": 2,
    "limit": 10,
    "offset": 0
  }
}
```

---

### Step 2: Get Error Summary

```bash
GET /errors/summary?days=7
Authorization: Bearer {token}
```

**Response**:
```json
{
  "success": true,
  "message": "Error summary for last 7 days",
  "data": {
    "period_days": 7,
    "total_errors": 150,
    "resolved_errors": 120,
    "unresolved_errors": 30,
    "resolution_rate": 80.0,
    "by_severity": {
      "LOW": 50,
      "MEDIUM": 70,
      "HIGH": 25,
      "CRITICAL": 5
    },
    "by_type": {
      "VALIDATION_ERROR": 80,
      "PROCESSING_ERROR": 40,
      "DATABASE_ERROR": 20,
      "NETWORK_ERROR": 10
    }
  }
}
```

---

### Step 3: Resolve Error

```bash
PATCH /errors/{error_id}/resolve
Authorization: Bearer {token}
Content-Type: application/json

{
  "resolved_by": "user-id-here"
}
```

**Response**:
```json
{
  "success": true,
  "message": "Error marked as resolved",
  "data": {
    "error_id": "bb0e8400-e29b-41d4-a716-446655440000",
    "is_resolved": true,
    "resolved_at": "2025-11-22T11:00:00",
    "resolved_by": "user-id-here"
  }
}
```

---

## Data Quality

### Run Quality Check

```bash
POST /data-quality/check
Authorization: Bearer {token}
Content-Type: application/json

{
  "entity_type": "customer",
  "check_config": {
    "completeness": true,
    "accuracy": true,
    "consistency": true
  }
}
```

**Response**:
```json
{
  "check_id": "cc0e8400-e29b-41d4-a716-446655440000",
  "entity_type": "customer",
  "status": "completed",
  "total_records": 10000,
  "passed_records": 9800,
  "failed_records": 200,
  "quality_score": 98.0,
  "issues": [
    {
      "rule": "email_format",
      "severity": "MEDIUM",
      "failed_count": 150
    }
  ]
}
```

---

## Complete Workflow Examples

### Example 1: Simple File Upload & Processing

```bash
# 1. Login
TOKEN=$(curl -X POST http://localhost:8000/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"admin123"}' \
  | jq -r '.access_token')

# 2. Upload file
FILE_ID=$(curl -X POST http://localhost:8000/api/v1/files/upload \
  -H "Authorization: Bearer $TOKEN" \
  -F "file=@data.csv" \
  | jq -r '.file_id')

# 3. Process file
curl -X POST http://localhost:8000/api/v1/files/$FILE_ID/process \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"processing_config":{"delimiter":","}}'

# 4. Check status
curl -X GET http://localhost:8000/api/v1/files/$FILE_ID \
  -H "Authorization: Bearer $TOKEN"
```

---

### Example 2: ETL Job with Dependencies

```bash
# 1. Create parent job (Extract)
PARENT_JOB=$(curl -X POST http://localhost:8000/api/v1/jobs \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "job_name": "Extract Customer Data",
    "job_type": "EXTRACT",
    "source_type": "DATABASE"
  }' | jq -r '.job_id')

# 2. Create child job (Transform)
CHILD_JOB=$(curl -X POST http://localhost:8000/api/v1/jobs \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "job_name": "Transform Customer Data",
    "job_type": "TRANSFORM"
  }' | jq -r '.job_id')

# 3. Add dependency
curl -X POST http://localhost:8000/api/v1/jobs/$CHILD_JOB/dependencies \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{
    \"parent_job_id\": \"$PARENT_JOB\",
    \"dependency_type\": \"SUCCESS\"
  }"

# 4. Execute parent job
curl -X POST http://localhost:8000/api/v1/jobs/$PARENT_JOB/execute \
  -H "Authorization: Bearer $TOKEN"

# 5. Child job will auto-trigger when parent completes!
# Check child job executions
curl -X GET http://localhost:8000/api/v1/jobs/$CHILD_JOB/executions \
  -H "Authorization: Bearer $TOKEN"
```

---

### Example 3: Monitor Job with Error Handling

```bash
# 1. Execute job
EXECUTION_ID=$(curl -X POST http://localhost:8000/api/v1/jobs/$JOB_ID/execute \
  -H "Authorization: Bearer $TOKEN" \
  | jq -r '.execution_id')

# 2. Poll for status
while true; do
  STATUS=$(curl -s http://localhost:8000/api/v1/jobs/$JOB_ID/executions/$EXECUTION_ID \
    -H "Authorization: Bearer $TOKEN" \
    | jq -r '.status')
  
  echo "Status: $STATUS"
  
  if [ "$STATUS" = "SUCCESS" ] || [ "$STATUS" = "FAILED" ]; then
    break
  fi
  
  sleep 5
done

# 3. If failed, get errors
if [ "$STATUS" = "FAILED" ]; then
  curl -X GET http://localhost:8000/api/v1/errors/executions/$EXECUTION_ID/errors \
    -H "Authorization: Bearer $TOKEN"
fi

# 4. Get performance metrics
curl -X GET http://localhost:8000/api/v1/jobs/$JOB_ID/executions/$EXECUTION_ID \
  -H "Authorization: Bearer $TOKEN" \
  | jq '.performance_metrics'
```

---

## Error Codes

| Code | Description |
|------|-------------|
| 200 | Success |
| 201 | Created |
| 400 | Bad Request - Invalid parameters |
| 401 | Unauthorized - Invalid or missing token |
| 403 | Forbidden - Insufficient permissions |
| 404 | Not Found - Resource doesn't exist |
| 409 | Conflict - Resource already exists |
| 422 | Unprocessable Entity - Validation error |
| 500 | Internal Server Error |

---

## Rate Limits

- **File Upload**: 100 requests/hour
- **Job Execution**: 1000 requests/hour
- **API Queries**: 5000 requests/hour

---

## Best Practices

### 1. Always Check Dependencies
```bash
# Before executing a job
GET /jobs/{job_id}/dependencies/check
```

### 2. Monitor Long-Running Jobs
```bash
# Poll every 5-10 seconds
GET /jobs/{job_id}/executions/{execution_id}
```

### 3. Handle Errors Gracefully
```bash
# Always check for errors after execution
GET /errors/executions/{execution_id}/errors
```

### 4. Use Pagination
```bash
# For large result sets
GET /jobs?limit=50&offset=0
```

### 5. Clean Up Old Files
```bash
# Delete processed files
DELETE /files/{file_id}
```

---

## Support

For issues or questions:
- GitHub: [github.com/muhamadanjar/fastapi-etl](https://github.com)
- Email: arvanria@gmail.com
- Documentation: [docs.example.com](https://docs.example.com)
