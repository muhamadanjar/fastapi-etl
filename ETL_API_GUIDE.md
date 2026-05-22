# ETL API - Panduan Penggunaan Lengkap

Dokumentasi step-by-step untuk menggunakan ETL API. API ini mengimplementasikan 8-phase data pipeline: Authentication → Job Creation → Execution Trigger → Extract → Transform → Load → Post-Processing → Monitoring.

---

## Daftar Isi

1. [Prerequisites & Setup](#1-prerequisites--setup)
2. [Autentikasi](#2-autentikasi)
3. [Upload File](#3-upload-file)
4. [Buat ETL Job](#4-buat-etl-job)
   - 4.1 [Konsep: Job Definition vs File](#41-konsep-job-definition-vs-file)
   - 4.2 [Strategi File Selection di job_config](#42-strategi-file-selection-di-job_config)
   - 4.3 [Buat Job](#43-buat-job)
5. [Konfigurasi Transformation Rules](#5-konfigurasi-transformation-rules)
6. [Konfigurasi Field Mappings](#6-konfigurasi-field-mappings)
7. [Setup Quality Rules](#7-setup-quality-rules)
8. [Job Dependencies (Opsional)](#8-job-dependencies-opsional)
9. [Eksekusi Job](#9-eksekusi-job)
   - 9.1 [Eksekusi Scheduled (Criteria-Based)](#91-eksekusi-scheduled-criteria-based)
   - 9.2 [Eksekusi dengan File Spesifik (Manual)](#92-eksekusi-dengan-file-spesifik-manual)
   - 9.3 [Full ETL Job](#93-full-etl-job-extract--transform--load-sekaligus)
10. [Monitor Eksekusi](#10-monitor-eksekusi)
11. [Lihat Hasil](#11-lihat-hasil)
12. [Penanganan Error](#12-penanganan-error)
13. [Scheduling Job (Opsional)](#13-scheduling-job-opsional)
14. [Conflict Resolution Strategies](#conflict-resolution-strategies-phase-load)
15. [Entity Deduplication](#entity-deduplication-phase-load)
16. [Referensi Endpoint Lengkap](#14-referensi-endpoint-lengkap)

---

## 1. Prerequisites & Setup

### Environment Variables

Pastikan variabel berikut sudah dikonfigurasi:

```bash
# Database
DATABASE_URL=postgresql://user:password@localhost:5432/etl_db

# Cache (Redis)
REDIS_URL=redis://localhost:6379/0
REDIS_HOST=localhost
REDIS_PORT=6379

# Task Queue (Celery)
CELERY_BROKER_URL=redis://localhost:6379/1
CELERY_RESULT_BACKEND=redis://localhost:6379/2

# JWT Authentication
JWT_SECRET_KEY=your-secret-key-here
JWT_ALGORITHM=HS256

# Email Notifications
EMAIL_SMTP_HOST=smtp.gmail.com
EMAIL_SMTP_PORT=587
EMAIL_SMTP_USER=your-email@gmail.com
EMAIL_SMTP_PASSWORD=your-app-password

# Quality Threshold (%)
QUALITY_THRESHOLD=80.0

# Debug Mode
DEBUG=true
```

### Service Dependencies

Pastikan services berikut running:

1. **PostgreSQL** (port 5432)
   ```bash
   psql -U postgres -d etl_db
   ```

2. **Redis** (port 6379)
   ```bash
   redis-cli ping  # Should return PONG
   ```

3. **Celery Worker** (untuk async tasks)
   ```bash
   celery -A app.tasks worker --loglevel=info --queues=etl,monitoring,cleanup,default
   ```

4. **ETL API Server** (port 8000 dev / 8007 docker)
   ```bash
   python -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
   ```

### Base URL

```
Development:  http://localhost:8000
Docker:       http://localhost:8007
Production:   https://api.example.com
```

Swagger docs (development only):
```
http://localhost:8000/docs
```

---

## 2. Autentikasi

Semua endpoint (kecuali `/docs`) memerlukan JWT token dalam header `Authorization`.

### Validasi Token

```http
GET /auth/me
Authorization: Bearer <JWT_TOKEN>
```

**Response (200 OK):**
```json
{
  "user_id": "uuid-user-id",
  "email": "user@example.com",
  "is_active": true,
  "permissions": ["read:jobs", "write:jobs", "read:files", "write:files"]
}
```

**Error (401 Unauthorized):**
```json
{
  "detail": "Invalid or expired token"
}
```

### Header Format

Setiap request harus include header:
```
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

Token didapat dari `usermanagement_api` (service terpisah). Hubungi tim auth untuk mendapat token.

---

## 3. Upload File

### 3.1 Upload File Tunggal (< 100MB)

```http
POST /files/upload
Authorization: Bearer <TOKEN>
Content-Type: multipart/form-data

file=@/path/to/data.csv
source_name=customers_initial_load (optional)
```

**Response (201 Created):**
```json
{
  "file_id": "550e8400-e29b-41d4-a716-446655440000",
  "original_filename": "data.csv",
  "file_size": 2048576,
  "file_type": "csv",
  "upload_date": "2026-05-21T10:30:00Z",
  "processing_status": "pending",
  "column_count": 15,
  "row_count": null,
  "storage_path": "s3://bucket/uploads/550e8400-e29b-41d4-a716-446655440000.csv"
}
```

### 3.2 Upload File Besar (Chunked Upload)

Untuk file > 100MB, gunakan chunked upload.

**Step 1: Buat session upload**

```http
POST /files/upload/session
Authorization: Bearer <TOKEN>
Content-Type: application/json

{
  "filename": "large_data.csv",
  "file_size": 5368709120,
  "chunk_size": 5242880
}
```

**Response:**
```json
{
  "session_id": "session-uuid",
  "total_chunks": 1024,
  "chunk_size": 5242880,
  "expires_at": "2026-05-22T10:30:00Z"
}
```

**Step 2: Upload chunk per chunk**

```http
POST /files/upload/{session_id}/0
Authorization: Bearer <TOKEN>
Content-Type: application/octet-stream

<binary chunk data>
```

**Response per chunk:**
```json
{
  "session_id": "session-uuid",
  "chunk_index": 0,
  "status": "received",
  "received_chunks": 1,
  "total_chunks": 1024
}
```

Ulangi untuk semua chunk hingga `received_chunks == total_chunks`.

**Step 3: Finalisasi**

Ketika semua chunk ter-upload, sistem otomatis merge & processing dimulai. Monitor status dengan:

```http
GET /files/{file_id}/processing-status
Authorization: Bearer <TOKEN>
```

### 3.3 Batch Upload (Multiple Files)

```http
POST /files/batch-upload
Authorization: Bearer <TOKEN>
Content-Type: multipart/form-data

files=@file1.csv files=@file2.xlsx files=@file3.json
```

**Response:**
```json
{
  "batch_id": "batch-uuid",
  "files": [
    {
      "file_id": "uuid1",
      "original_filename": "file1.csv",
      "status": "pending"
    },
    {
      "file_id": "uuid2",
      "original_filename": "file2.xlsx",
      "status": "pending"
    }
  ]
}
```

### 3.4 Cek Status Processing File

```http
GET /files/{file_id}/processing-status
Authorization: Bearer <TOKEN>
```

**Response:**
```json
{
  "file_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "processing",
  "phase": "parsing",
  "progress_percent": 45,
  "errors": [],
  "warnings": ["encoding_detected: utf-8"],
  "estimated_completion": "2026-05-21T10:35:00Z"
}
```

Status values: `pending`, `processing`, `success`, `failed`

### 3.5 Preview Data Dari File

```http
GET /files/{file_id}/preview?rows=10
Authorization: Bearer <TOKEN>
```

**Response:**
```json
{
  "file_id": "550e8400-e29b-41d4-a716-446655440000",
  "rows": [
    {
      "id": "1",
      "name": "John Doe",
      "email": "john@example.com",
      "status": "active"
    }
  ],
  "total_rows": 150000,
  "columns": [
    {"name": "id", "type": "string", "null_count": 0},
    {"name": "name", "type": "string", "null_count": 5},
    {"name": "email", "type": "string", "null_count": 2},
    {"name": "status", "type": "string", "null_count": 0}
  ]
}
```

---

## 4. Buat ETL Job

`EtlJob` adalah **job definition (template)** — mendefinisikan HOW memproses data, bukan file spesifik mana yang diproses. File yang diproses ditentukan saat execution, bukan saat job creation.

### 4.1 Konsep: Job Definition vs File

```
EtlJob          = Template/resep (bagaimana memproses)
FileRegistry    = Data (file yang diupload)
JobExecution    = Runtime (saat job dijalankan dengan file tertentu)
```

`EtlJob` tidak memiliki relasi langsung ke `FileRegistry` karena:
- Scheduled job tidak tahu file mana yang akan ada saat runtime
- Job yang sama bisa digunakan untuk berbagai batch file berbeda
- File selection harus dinamis per execution

### 4.2 Strategi File Selection di job_config

`job_config` di `EtlJob` menyimpan **kriteria seleksi file**, bukan file_id literal:

**Scheduled job (proses semua file PENDING sesuai kriteria):**
```json
{
  "job_config": {
    "source_type": "FILE",
    "file_type": "CSV",
    "source_system": "SalesForce",
    "processing_status": "PENDING",
    "entity_type": "CUSTOMER",
    "batch_size": 1000,
    "key_fields": ["id", "email"],
    "conflict_resolution_strategy": "newer_wins"
  }
}
```

Saat execution, sistem query `FileRegistry` dengan kriteria tersebut untuk mendapat `file_ids`.

**Manual/one-time job (proses file spesifik):**
Pass `file_ids` explicit saat trigger execution — lihat [Section 9.2](#92-eksekusi-dengan-file-spesifik-manual).

### 4.3 Buat Job

```http
POST /jobs/
Authorization: Bearer <TOKEN>
Content-Type: application/json

{
  "job_name": "Daily Customer Load",
  "job_type": "EXTRACT",
  "job_category": "FILE_PROCESSING",
  "source_type": "FILE",
  "target_schema": "staging",
  "target_table": "customers",
  "schedule_expression": "0 2 * * *",
  "is_active": true,
  "job_config": {
    "file_type": "CSV",
    "source_system": "SalesForce",
    "entity_type": "CUSTOMER",
    "batch_size": 1000,
    "key_fields": ["id", "email"],
    "similarity_threshold": 0.85,
    "conflict_resolution_strategy": "newer_wins"
  }
}
```

**Response (201 Created):**
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "job_name": "Daily Customer Load",
  "job_type": "EXTRACT",
  "job_category": "FILE_PROCESSING",
  "source_type": "FILE",
  "target_schema": "staging",
  "target_table": "customers",
  "schedule_expression": "0 2 * * *",
  "is_active": true,
  "created_at": "2026-05-21T10:00:00Z"
}
```

**Notes:**
- `transformation_rules`, `field_mappings`, `quality_rules` dibuat terpisah via endpoint masing-masing
- `job_type`: `EXTRACT`, `TRANSFORM`, `LOAD`, `VALIDATE`
- `job_category`: `FILE_PROCESSING`, `DATA_CLEANING`, `AGGREGATION`, `VALIDATION`, `EXPORT`
- `source_type`: `FILE`, `API`, `DATABASE`, `STREAM`

### job_config Field Reference

| Field | Tipe | Deskripsi |
|-------|------|-----------|
| `file_type` | string | Filter file by type: `CSV`, `EXCEL`, `JSON`, `XML` |
| `source_system` | string | Filter by source system name |
| `entity_type` | string | Target entity type (e.g. `CUSTOMER`, `PRODUCT`) |
| `batch_size` | int | Jumlah records per batch (default: 1000) |
| `key_fields` | array | Fields untuk entity hash & dedup (e.g. `["id", "email"]`) |
| `similarity_threshold` | float | Threshold fuzzy match untuk dedup (default: 0.85) |
| `conflict_resolution_strategy` | string | `newer_wins`, `score_based`, `conservative`, `merge` |

### Job Status Values

- `active` — Job siap dijalankan
- `inactive` — Job tidak aktif
- `paused` — Job di-pause (bisa di-resume)
- `archived` — Job di-archive

---

## 5. Konfigurasi Transformation Rules

Transformation rules mendefinisikan bagaimana data akan ditransformasi (cleansing, normalization).

### Tipe Rule

1. **Direct Mapping** — Copy nilai dari source ke target
2. **Calculated Field** — Hitung nilai baru dari formula
3. **Lookup** — Lookup nilai dari tabel referensi
4. **Constant** — Set nilai konstan

### Contoh: Direct Mapping

```json
{
  "rule_type": "direct",
  "source_field": "customer_name",
  "target_field": "name",
  "transformations": [
    {
      "type": "trim",
      "params": {}
    },
    {
      "type": "lowercase"
    }
  ]
}
```

### Contoh: Calculated Field

```json
{
  "rule_type": "calculated",
  "target_field": "full_address",
  "expression": "CONCAT(street, ', ', city, ', ', state, ' ', zip)",
  "source_fields": ["street", "city", "state", "zip"]
}
```

### Contoh: Lookup Field

```json
{
  "rule_type": "lookup",
  "source_field": "country_code",
  "target_field": "country_name",
  "lookup_table": "country_reference",
  "lookup_key": "code",
  "lookup_value": "name",
  "default_value": "Unknown"
}
```

### Contoh: Constant Field

```json
{
  "rule_type": "constant",
  "target_field": "data_source",
  "value": "DAILY_EXPORT",
  "data_type": "string"
}
```

### Transformation Types

- `trim` — Trim whitespace
- `uppercase` — Uppercase
- `lowercase` — Lowercase
- `replace` — Replace string (params: `old`, `new`)
- `regex_replace` — Regex replace (params: `pattern`, `replacement`)
- `substring` — Extract substring (params: `start`, `length`)
- `date_format` — Format date (params: `format_from`, `format_to`)
- `round` — Round number (params: `decimals`)
- `null_to_default` — Replace null (params: `default_value`)

### Tambah Transformation Rule ke Job

```http
POST /jobs/{job_id}/transformation-rules
Authorization: Bearer <TOKEN>
Content-Type: application/json

{
  "rule_type": "direct",
  "source_field": "customer_name",
  "target_field": "name",
  "transformations": [
    {"type": "trim"},
    {"type": "lowercase"}
  ]
}
```

**Response:**
```json
{
  "rule_id": "rule-uuid-001",
  "job_id": "job-uuid-001",
  "rule_type": "direct",
  "source_field": "customer_name",
  "target_field": "name",
  "transformations": [...],
  "created_at": "2026-05-21T10:05:00Z"
}
```

---

## 6. Konfigurasi Field Mappings

Field mapping menentukan pemetaan antara kolom source file dengan target entity fields.

### Tipe Mapping

1. **Direct** — Mapping langsung 1:1
2. **Calculated** — Hitung dari beberapa field
3. **Lookup** — Lookup dari tabel referensi
4. **Constant** — Set nilai konstan

### Contoh Request

```http
POST /jobs/{job_id}/field-mappings
Authorization: Bearer <TOKEN>
Content-Type: application/json

{
  "source_field": "email_address",
  "target_field": "email",
  "mapping_type": "direct",
  "data_type": "string",
  "is_required": true,
  "is_unique": false
}
```

**Response:**
```json
{
  "mapping_id": "mapping-uuid-001",
  "job_id": "job-uuid-001",
  "source_field": "email_address",
  "target_field": "email",
  "mapping_type": "direct",
  "data_type": "string",
  "is_required": true,
  "is_unique": false,
  "created_at": "2026-05-21T10:05:00Z"
}
```

### Mapping Type Details

**Direct Mapping:**
```json
{
  "source_field": "first_name",
  "target_field": "first_name",
  "mapping_type": "direct"
}
```

**Calculated Mapping:**
```json
{
  "source_fields": ["first_name", "last_name"],
  "target_field": "full_name",
  "mapping_type": "calculated",
  "expression": "CONCAT(first_name, ' ', last_name)"
}
```

**Lookup Mapping:**
```json
{
  "source_field": "country_code",
  "target_field": "country_id",
  "mapping_type": "lookup",
  "lookup_table": "countries",
  "lookup_key": "code",
  "lookup_value": "id",
  "default_value": null
}
```

**Constant Mapping:**
```json
{
  "target_field": "import_source",
  "mapping_type": "constant",
  "value": "BULK_IMPORT_2026_05"
}
```

---

## 7. Setup Quality Rules

Quality rules mendefinisikan validasi data yang harus dipenuhi sebelum load ke database.

### Rule Types

1. **Completeness** — Cek field tidak null/empty
2. **Uniqueness** — Cek nilai unik
3. **Validity** — Cek format valid (regex, email, phone, etc)
4. **Range** — Cek nilai dalam range (numerik, date)
5. **Consistency** — Cek consistency antar fields

### Contoh: Completeness Rule

```http
POST /jobs/{job_id}/quality-rules
Authorization: Bearer <TOKEN>
Content-Type: application/json

{
  "rule_type": "completeness",
  "rule_name": "Email must not be empty",
  "target_field": "email",
  "severity": "error"
}
```

### Contoh: Uniqueness Rule

```json
{
  "rule_type": "uniqueness",
  "rule_name": "Email must be unique",
  "target_field": "email",
  "severity": "error",
  "scope": "within_batch"
}
```

Scope: `within_batch` (unique dalam batch saja) atau `global` (unique dengan data existing)

### Contoh: Validity Rule

```json
{
  "rule_type": "validity",
  "rule_name": "Email format valid",
  "target_field": "email",
  "validation_type": "email",
  "severity": "error"
}
```

Validation types: `email`, `phone`, `regex`, `numeric`, `date`, `url`, `ipaddress`

### Contoh: Range Rule

```json
{
  "rule_type": "range",
  "rule_name": "Age between 18 and 120",
  "target_field": "age",
  "min_value": 18,
  "max_value": 120,
  "severity": "warning"
}
```

### Contoh: Consistency Rule

```json
{
  "rule_type": "consistency",
  "rule_name": "Start date before end date",
  "source_fields": ["start_date", "end_date"],
  "condition": "start_date < end_date",
  "severity": "error"
}
```

### Response

```json
{
  "rule_id": "qrule-uuid-001",
  "job_id": "job-uuid-001",
  "rule_type": "completeness",
  "rule_name": "Email must not be empty",
  "target_field": "email",
  "severity": "error",
  "created_at": "2026-05-21T10:05:00Z"
}
```

### Severity Levels

- `error` — Record ditolak jika rule fail
- `warning` — Record tetap di-load tapi dicatat warning
- `info` — Hanya informasi, tidak mempengaruhi load

---

## 8. Job Dependencies (Opsional)

Setup dependency jika job ini harus menunggu job lain selesai lebih dulu.

### Tambah Dependency

```http
POST /jobs/{job_id}/dependencies
Authorization: Bearer <TOKEN>
Content-Type: application/json

{
  "parent_job_id": "parent-job-uuid",
  "dependency_type": "must_complete"
}
```

**Response:**
```json
{
  "dependency_id": "dep-uuid-001",
  "job_id": "job-uuid-001",
  "parent_job_id": "parent-job-uuid",
  "dependency_type": "must_complete",
  "status": "pending",
  "created_at": "2026-05-21T10:05:00Z"
}
```

### Dependency Types

- `must_complete` — Parent job harus complete dengan success
- `must_complete_or_fail` — Parent job complete (success or fail)
- `must_not_fail` — Parent job tidak boleh fail

### Cek Dependency Status

```http
GET /jobs/{job_id}/dependencies/check
Authorization: Bearer <TOKEN>
```

**Response:**
```json
{
  "job_id": "job-uuid-001",
  "all_dependencies_met": true,
  "dependencies": [
    {
      "dependency_id": "dep-uuid-001",
      "parent_job_id": "parent-job-uuid",
      "status": "satisfied",
      "reason": "Parent job execution #3 completed successfully"
    }
  ]
}
```

### View Dependency Tree

```http
GET /jobs/{job_id}/dependency-tree
Authorization: Bearer <TOKEN>
```

**Response:**
```json
{
  "job_id": "job-uuid-001",
  "job_name": "Daily Customer Load",
  "dependencies": [
    {
      "parent_job_id": "parent-uuid-001",
      "parent_name": "Load Countries",
      "dependency_type": "must_complete"
    }
  ],
  "dependents": [
    {
      "child_job_id": "child-uuid-001",
      "child_name": "Aggregate Customers",
      "dependency_type": "must_complete"
    }
  ]
}
```

---

## 9. Eksekusi Job

Trigger job untuk dijalankan melalui ETL pipeline. Ada dua cara: scheduled (pakai kriteria dari job_config) atau manual (pass file_ids explicit).

### 9.1 Eksekusi Scheduled (Criteria-Based)

Sistem otomatis query `FileRegistry` berdasarkan kriteria di `job_config`:

```http
POST /jobs/{job_id}/execute
Authorization: Bearer <TOKEN>
Content-Type: application/json

{
  "execution_label": "Daily Run 2026-05-21"
}
```

Sistem akan:
1. Baca `job_config` dari job → ambil filter criteria (`file_type`, `source_system`, dll.)
2. Query `FileRegistry` WHERE `processing_status='PENDING'` AND criteria match
3. Proses semua file yang ditemukan

### 9.2 Eksekusi dengan File Spesifik (Manual)

Untuk memproses file tertentu saja, pass `file_ids` di `parameters`:

```http
POST /jobs/{job_id}/execute
Authorization: Bearer <TOKEN>
Content-Type: application/json

{
  "execution_label": "Reprocess Batch 2026-05-21",
  "parameters": {
    "file_ids": [
      "550e8400-e29b-41d4-a716-446655440000",
      "660f9500-f30c-52e5-b827-557766551111"
    ]
  }
}
```

`parameters.file_ids` override criteria-based lookup di `job_config`.

**Response (202 Accepted):**
```json
{
  "execution_id": "exec-uuid-001",
  "job_id": "job-uuid-001",
  "status": "queued",
  "phase": "queued",
  "progress_percent": 0,
  "celery_task_id": "celery-task-uuid",
  "queued_at": "2026-05-21T10:10:00Z",
  "estimated_start": "2026-05-21T10:10:05Z"
}
```

### 9.3 Full ETL Job (Extract + Transform + Load sekaligus)

Untuk `job_type: FULL_ETL`, pass config per phase:

```http
POST /jobs/{job_id}/execute
Authorization: Bearer <TOKEN>
Content-Type: application/json

{
  "parameters": {
    "extract": {
      "file_ids": ["uuid1", "uuid2"]
    },
    "transform": {
      "entity_type": "CUSTOMER",
      "batch_size": 1000
    },
    "load": {
      "entity_type": "CUSTOMER",
      "key_fields": ["id", "email"],
      "conflict_resolution_strategy": "newer_wins"
    }
  }
}
```

### 8-Phase ETL Workflow

Execution berjalan secara asynchronous melalui 8 phase:

```
1. AUTHENTICATION ─┐
2. JOB CREATION   │
3. EXTRACT        │ Celery Queue
4. TRANSFORM      │ (Async Tasks)
5. LOAD           │
6. POST-PROCESS   │
7. VALIDATION     │
8. COMPLETION ────┘
```

Setiap phase publish event ke Redis. Monitoring dapat follow real-time progress.

### Cek Execution Status

```http
GET /jobs/{job_id}/executions/{execution_id}
Authorization: Bearer <TOKEN>
```

**Response:**
```json
{
  "execution_id": "exec-uuid-001",
  "job_id": "job-uuid-001",
  "status": "running",
  "phase": "transform",
  "progress_percent": 35,
  "started_at": "2026-05-21T10:10:05Z",
  "current_phase_start": "2026-05-21T10:12:00Z",
  "estimated_completion": "2026-05-21T10:20:00Z",
  "metrics": {
    "records_extracted": 150000,
    "records_processed": 125000,
    "records_failed": 500,
    "processing_speed": 15000,
    "memory_usage_mb": 256
  }
}
```

### Stop Execution

```http
POST /jobs/{job_id}/stop
Authorization: Bearer <TOKEN>
```

**Response:**
```json
{
  "execution_id": "exec-uuid-001",
  "job_id": "job-uuid-001",
  "status": "stopped",
  "stopped_at": "2026-05-21T10:15:30Z",
  "records_processed_before_stop": 125000
}
```

### Restart Job

```http
POST /jobs/{job_id}/restart
Authorization: Bearer <TOKEN>
```

Restart akan membuat execution baru dengan status `queued`.

---

## 10. Monitor Eksekusi

### Real-time Dashboard

```http
GET /monitoring/dashboard
Authorization: Bearer <TOKEN>
```

**Response:**
```json
{
  "summary": {
    "total_jobs": 25,
    "active_jobs": 3,
    "completed_today": 18,
    "failed_today": 2
  },
  "active_executions": [
    {
      "execution_id": "exec-uuid-001",
      "job_id": "job-uuid-001",
      "job_name": "Daily Customer Load",
      "status": "running",
      "phase": "transform",
      "progress_percent": 35,
      "started_at": "2026-05-21T10:10:05Z",
      "estimated_completion": "2026-05-21T10:20:00Z"
    }
  ],
  "recent_completions": [
    {
      "execution_id": "exec-uuid-002",
      "job_id": "job-uuid-002",
      "job_name": "Daily Products Load",
      "status": "completed",
      "completed_at": "2026-05-21T09:45:00Z",
      "duration_seconds": 300,
      "records_loaded": 50000
    }
  ]
}
```

### List Job Executions

```http
GET /jobs/{job_id}/executions?limit=10&offset=0
Authorization: Bearer <TOKEN>
```

**Response:**
```json
{
  "total": 145,
  "executions": [
    {
      "execution_id": "exec-uuid-001",
      "status": "completed",
      "started_at": "2026-05-21T10:10:00Z",
      "completed_at": "2026-05-21T10:18:00Z",
      "duration_seconds": 480,
      "records_extracted": 150000,
      "records_loaded": 147500,
      "records_failed": 2500,
      "quality_pass_rate": 98.3
    }
  ]
}
```

### Active Jobs Monitoring

```http
GET /monitoring/active-jobs
Authorization: Bearer <TOKEN>
```

**Response:**
```json
{
  "active_count": 3,
  "jobs": [
    {
      "execution_id": "exec-uuid-001",
      "job_name": "Daily Customer Load",
      "phase": "transform",
      "progress_percent": 45,
      "started_at": "2026-05-21T10:10:00Z",
      "elapsed_seconds": 300,
      "current_speed": 12500,
      "estimated_remaining_seconds": 420
    }
  ]
}
```

### Health Check

```http
GET /health
Authorization: Bearer <TOKEN>
```

**Response (200 OK):**
```json
{
  "status": "healthy",
  "timestamp": "2026-05-21T10:15:00Z"
}
```

### Detailed Health Check

```http
GET /health/detailed
Authorization: Bearer <TOKEN>
```

**Response:**
```json
{
  "status": "healthy",
  "components": {
    "database": {
      "status": "healthy",
      "response_time_ms": 5
    },
    "redis": {
      "status": "healthy",
      "response_time_ms": 2
    },
    "celery": {
      "status": "healthy",
      "active_workers": 4,
      "queued_tasks": 12
    },
    "storage": {
      "status": "healthy",
      "available_gb": 450
    }
  }
}
```

---

## 11. Lihat Hasil

Setelah execution complete, lihat hasil data yang berhasil di-load dan yang failed.

### Lihat Entities (Data Berhasil)

```http
GET /entities?job_id={job_id}&execution_id={execution_id}&page=1&page_size=50
Authorization: Bearer <TOKEN>
```

**Response:**
```json
{
  "total": 147500,
  "page": 1,
  "entities": [
    {
      "entity_id": "entity-uuid-001",
      "job_id": "job-uuid-001",
      "execution_id": "exec-uuid-001",
      "source_record_id": "1",
      "entity_data": {
        "id": "cust-1001",
        "name": "John Doe",
        "email": "john@example.com",
        "status": "active",
        "created_at": "2026-05-21T10:18:00Z"
      },
      "entity_hash": "md5hash...",
      "quality_score": 100,
      "loaded_at": "2026-05-21T10:18:00Z"
    }
  ]
}
```

### Lihat Rejected Records (Data Gagal)

```http
GET /rejected-records?job_id={job_id}&execution_id={execution_id}&page=1
Authorization: Bearer <TOKEN>
```

**Response:**
```json
{
  "total": 2500,
  "page": 1,
  "rejected_records": [
    {
      "rejected_record_id": "reject-uuid-001",
      "job_id": "job-uuid-001",
      "execution_id": "exec-uuid-001",
      "source_record_id": "15",
      "raw_data": {
        "id": "15",
        "name": "Jane Smith",
        "email": "invalid_email",
        "status": "active"
      },
      "rejection_reasons": [
        {
          "field": "email",
          "rule_name": "Email format valid",
          "reason": "Invalid email format"
        }
      ],
      "rejection_type": "quality",
      "rejected_at": "2026-05-21T10:18:00Z"
    }
  ]
}
```

### Lihat Data Quality Report

```http
GET /data-quality?job_id={job_id}&execution_id={execution_id}
Authorization: Bearer <TOKEN>
```

**Response:**
```json
{
  "job_id": "job-uuid-001",
  "execution_id": "exec-uuid-001",
  "report_date": "2026-05-21",
  "quality_metrics": {
    "total_records": 150000,
    "passed_records": 147500,
    "failed_records": 2500,
    "pass_rate_percent": 98.33,
    "average_quality_score": 98.5,
    "quality_threshold_percent": 80.0,
    "threshold_met": true
  },
  "quality_by_field": [
    {
      "field_name": "email",
      "passed": 147400,
      "failed": 600,
      "fail_reasons": ["Invalid format", "Duplicate"]
    },
    {
      "field_name": "phone",
      "passed": 147500,
      "failed": 0
    }
  ],
  "quality_by_rule": [
    {
      "rule_name": "Email format valid",
      "rule_type": "validity",
      "passed": 147400,
      "failed": 600
    }
  ],
  "recommendations": [
    "Improve email validation in source system",
    "Add phone number validation rule"
  ]
}
```

### Lihat Data Lineage

```http
GET /entities/{entity_id}/lineage
Authorization: Bearer <TOKEN>
```

**Response:**
```json
{
  "entity_id": "entity-uuid-001",
  "source_record": {
    "file_id": "file-uuid-001",
    "raw_record_id": "raw-record-uuid-001",
    "raw_data": {...}
  },
  "transformations_applied": [
    {
      "rule_id": "rule-uuid-001",
      "rule_name": "Trim whitespace",
      "field": "name",
      "before": "  John Doe  ",
      "after": "John Doe"
    }
  ],
  "quality_checks": [
    {
      "rule_id": "qrule-uuid-001",
      "rule_name": "Email format valid",
      "status": "passed"
    }
  ],
  "deduplication_result": {
    "match_type": "fuzzy_match",
    "matched_with_entity_id": null,
    "conflict_resolution_strategy": "newer_wins",
    "merge_score": 0.92
  }
}
```

---

## 12. Penanganan Error

Jika execution fail, lihat error dan recovery options.

### Lihat Error Log

```http
GET /errors?job_id={job_id}&execution_id={execution_id}&severity=error
Authorization: Bearer <TOKEN>
```

**Response:**
```json
{
  "total": 5,
  "errors": [
    {
      "error_id": "err-uuid-001",
      "job_id": "job-uuid-001",
      "execution_id": "exec-uuid-001",
      "phase": "transform",
      "severity": "error",
      "error_code": "QUALITY_THRESHOLD_NOT_MET",
      "error_message": "Quality pass rate 78% is below threshold 80%",
      "timestamp": "2026-05-21T10:18:00Z",
      "context": {
        "pass_rate": 78.5,
        "threshold": 80.0,
        "failed_records": 32500
      },
      "resolution_options": [
        {
          "option": "review_rejected_records",
          "description": "Review and fix rejected records in source"
        },
        {
          "option": "lower_quality_threshold",
          "description": "Lower quality threshold and retry"
        }
      ]
    }
  ]
}
```

### Retry/Reprocess File

Jika ada error di extract phase, re-process file:

```http
POST /files/{file_id}/reprocess
Authorization: Bearer <TOKEN>
Content-Type: application/json

{
  "clear_previous_data": false
}
```

**Response:**
```json
{
  "file_id": "file-uuid-001",
  "status": "reprocessing",
  "reprocess_id": "reprocess-uuid-001"
}
```

### Restart Job

Jika ada error di job execution, restart job:

```http
POST /jobs/{job_id}/restart
Authorization: Bearer <TOKEN>
```

---

## 13. Scheduling Job (Opsional)

Schedule job untuk berjalan otomatis pada waktu tertentu.

### Setup Schedule

```http
POST /jobs/{job_id}/schedule
Authorization: Bearer <TOKEN>
Content-Type: application/json

{
  "cron_expression": "0 2 * * *",
  "timezone": "Asia/Jakarta",
  "start_date": "2026-05-21",
  "end_date": "2026-12-31",
  "enabled": true,
  "notify_on_failure": true,
  "notification_channels": ["email", "slack"],
  "max_concurrent_executions": 1
}
```

**Response:**
```json
{
  "schedule_id": "schedule-uuid-001",
  "job_id": "job-uuid-001",
  "cron_expression": "0 2 * * *",
  "description": "Every day at 02:00 AM (Asia/Jakarta)",
  "timezone": "Asia/Jakarta",
  "start_date": "2026-05-21",
  "end_date": "2026-12-31",
  "enabled": true,
  "next_run": "2026-05-22T02:00:00+07:00",
  "created_at": "2026-05-21T10:20:00Z"
}
```

### Cron Expression Examples

```
0 2 * * *          # Every day at 02:00 AM
0 */6 * * *        # Every 6 hours
0 9,17 * * *       # At 09:00 AM and 05:00 PM
0 0 * * 1          # Every Monday at 00:00 (midnight)
0 0 1 * *          # First day of every month at 00:00
```

### Cek Schedule Status

```http
GET /jobs/{job_id}/schedule
Authorization: Bearer <TOKEN>
```

**Response:**
```json
{
  "schedule_id": "schedule-uuid-001",
  "job_id": "job-uuid-001",
  "cron_expression": "0 2 * * *",
  "enabled": true,
  "next_run": "2026-05-22T02:00:00+07:00",
  "last_run": "2026-05-21T02:00:00+07:00",
  "upcoming_runs": [
    "2026-05-22T02:00:00+07:00",
    "2026-05-23T02:00:00+07:00",
    "2026-05-24T02:00:00+07:00"
  ]
}
```

### Disable Schedule

```http
DELETE /jobs/{job_id}/schedule
Authorization: Bearer <TOKEN>
```

---

## 14. Referensi Endpoint Lengkap

| Kategori | Method | Endpoint | Deskripsi |
|----------|--------|----------|-----------|
| **Auth** | GET | `/auth/me` | Validasi token & info user |
| **Files** | POST | `/files/upload` | Upload file tunggal |
| | POST | `/files/upload/session` | Buat chunked upload session |
| | POST | `/files/upload/{session_id}/{chunk}` | Upload chunk |
| | GET | `/files` | List files |
| | GET | `/files/{file_id}` | Detail file |
| | GET | `/files/{file_id}/processing-status` | Cek status processing |
| | GET | `/files/{file_id}/preview` | Preview data |
| | POST | `/files/{file_id}/reprocess` | Re-process file |
| | POST | `/files/batch-upload` | Upload multiple files |
| | DELETE | `/files/{file_id}` | Delete file |
| **Jobs** | POST | `/jobs` | Buat job baru |
| | GET | `/jobs` | List jobs |
| | GET | `/jobs/{job_id}` | Detail job |
| | PUT | `/jobs/{job_id}` | Update job |
| | DELETE | `/jobs/{job_id}` | Delete job |
| | POST | `/jobs/{job_id}/execute` | Execute job |
| | POST | `/jobs/{job_id}/stop` | Stop execution |
| | POST | `/jobs/{job_id}/restart` | Restart job |
| | GET | `/jobs/{job_id}/executions` | List executions |
| | GET | `/jobs/{job_id}/executions/{execution_id}` | Detail execution |
| **Dependencies** | POST | `/jobs/{job_id}/dependencies` | Tambah dependency |
| | GET | `/jobs/{job_id}/dependencies` | List dependencies |
| | GET | `/jobs/{job_id}/dependencies/check` | Cek dependency status |
| | GET | `/jobs/{job_id}/dependency-tree` | View dependency tree |
| | DELETE | `/jobs/{job_id}/dependencies/{dep_id}` | Hapus dependency |
| **Scheduling** | POST | `/jobs/{job_id}/schedule` | Setup schedule |
| | GET | `/jobs/{job_id}/schedule` | Detail schedule |
| | DELETE | `/jobs/{job_id}/schedule` | Delete schedule |
| **Monitoring** | GET | `/monitoring/dashboard` | Dashboard |
| | GET | `/monitoring/active-jobs` | Active jobs |
| | GET | `/monitoring/health` | Health status |
| | GET | `/monitoring/health/detailed` | Detailed health |
| | GET | `/monitoring/metrics` | System metrics |
| | GET | `/monitoring/job-performance` | Job performance stats |
| | GET | `/monitoring/data-quality-trends` | Quality trends |
| | GET | `/monitoring/alerts` | Active alerts |
| | POST | `/monitoring/alerts/{alert_id}/dismiss` | Dismiss alert |
| **Results** | GET | `/entities` | List entities (loaded data) |
| | GET | `/entities/{entity_id}` | Detail entity |
| | GET | `/entities/{entity_id}/lineage` | Entity lineage |
| | GET | `/rejected-records` | List rejected records |
| | GET | `/rejected-records/{record_id}` | Detail rejected record |
| | GET | `/data-quality` | Quality report |
| **Error Mgmt** | GET | `/errors` | List errors |
| | GET | `/errors/{error_id}` | Detail error |

---

## Troubleshooting

### Job Stuck di Phase Tertentu

1. Cek Celery worker status
   ```bash
   celery -A app.tasks inspect active
   ```

2. Lihat error log
   ```http
   GET /errors?job_id={job_id}
   ```

3. Cek resource (CPU, memory, disk)
   ```http
   GET /monitoring/health/detailed
   ```

### Records Tidak Di-load (Quality Threshold)

1. Review rejected records
   ```http
   GET /rejected-records?job_id={job_id}&execution_id={execution_id}
   ```

2. Lihat quality rules yang fail
   ```http
   GET /data-quality?job_id={job_id}&execution_id={execution_id}
   ```

3. Adjust quality rules atau fix source data

### File Upload Fails

1. Cek file size (max 5GB dengan chunked upload)
2. Cek supported formats (CSV, JSON, XML, Excel, API)
3. Lihat processing status
   ```http
   GET /files/{file_id}/processing-status
   ```

---

## Conflict Resolution Strategies (Phase Load)

Saat entity sudah ada di database dan ada data baru yang match, sistem resolve conflict dengan strategy berikut:

| Strategy | Behavior |
|----------|----------|
| `newer_wins` | Data baru selalu menang (default) |
| `score_based` | Confidence ≥ 0.9 → data baru menang; else merge selektif |
| `conservative` | Data existing selalu menang (no update) |
| `merge` | Recursive merge: dict digabung, list di-unique, scalar prefer existing |

**Kapan pakai:**
- `newer_wins` — source data selalu lebih update dari yang di DB
- `conservative` — jangan overwrite data yang sudah dikurasi manual
- `score_based` — hanya update jika data baru high-confidence
- `merge` — enrichment use case: gabung data dari banyak source

---

## Entity Deduplication (Phase Load)

Sistem detect duplikat via dua mekanisme:

**1. Exact Match (Hash)**
- Hitung `entity_hash = MD5(key_fields values)`
- Jika hash sama → `confidence_score = 1.0`, record di-skip (sudah ada)

**2. Fuzzy Match**
- Jika hash berbeda tapi similarity > `similarity_threshold`
- Record di-mark `is_duplicate=True`, `duplicate_count` di entitas asli di-increment
- Relasi `duplicate_of` diinsert ke `entity_relationships`

`key_fields` di `job_config` menentukan field mana yang dipakai untuk hash. Pilih field yang bersifat identifier unik (e.g. `["id", "email"]`).

---

## Best Practices

1. **Preview data sebelum create job** — pastikan kolom dan format sesuai ekspektasi
2. **Mulai dengan `job_type: EXTRACT`** untuk test parsing, baru lanjut TRANSFORM dan LOAD
3. **Setup quality rules** sesuai business requirement sebelum eksekusi massal
4. **Gunakan criteria-based job_config** untuk scheduled jobs, bukan hardcode file_ids
5. **Gunakan `parameters.file_ids`** untuk manual reprocessing file spesifik
6. **Pilih `key_fields` yang tepat** — harus identifier unik untuk dedup benar
7. **Monitor execution pertama** sebelum schedule recurring
8. **Set `similarity_threshold` 0.85–0.95** — terlalu rendah banyak false duplicate, terlalu tinggi miss banyak duplicate
9. **Review rejected records** setelah setiap run untuk identify data issues di source
10. **Schedule off-peak hours** (misal `0 2 * * *`) untuk job berat

