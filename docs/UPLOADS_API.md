# ETL API - File Uploads Documentation

**Last Updated:** 2026-07-02  
**Status:** ✅ Production Ready  
**Version:** 1.1.0

---

## Overview

ETL API provides two file upload strategies:
1. **Regular Upload** — For small to medium files (< 100 MB recommended)
2. **Chunked Upload** — For large files with resume capability

Both methods register files in `raw_data.file_registry` and trigger optional auto-processing.

---

## 1. Regular File Upload

### Endpoint
```
POST /api/v1/files/upload
```

### Request

**Multipart Form Data:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `file` | File | ✅ | File to upload (CSV, EXCEL, JSON, XML) |
| `source_system` | string | ✅ | Source system identifier (e.g., "SalesForce", "SAP") |
| `batch_id` | string | ❌ | Batch identifier for grouping related files |
| `metadata` | JSON | ❌ | Additional metadata (max 1000 chars) |

**Supported File Types:**
- CSV: `text/csv`
- Excel: `application/vnd.ms-excel` (XLS), `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet` (XLSX)
- JSON: `application/json`
- XML: `application/xml`, `text/xml`

### Response

**Status:** `200 OK`
```json
{
  "status": "success",
  "data": {
    "file_id": "550e8400-e29b-41d4-a716-446655440000",
    "file_name": "customers.csv",
    "file_type": "CSV",
    "file_size": 1024000,
    "batch_id": "batch_001",
    "processing_status": "PENDING",
    "upload_date": "2026-07-02T10:30:00Z"
  },
  "metas": {
    "message": "File uploaded successfully"
  }
}
```

### Example: cURL

```bash
curl -X POST http://localhost:8007/api/v1/files/upload \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -F "file=@customers.csv" \
  -F "source_system=SalesForce" \
  -F "batch_id=batch_001"
```

### Example: Python

```python
import requests
import json

url = "http://localhost:8007/api/v1/files/upload"
headers = {"Authorization": "Bearer YOUR_JWT_TOKEN"}

with open("customers.csv", "rb") as f:
    files = {
        "file": ("customers.csv", f, "text/csv")
    }
    data = {
        "source_system": "SalesForce",
        "batch_id": "batch_001",
        "metadata": json.dumps({"department": "sales"})
    }
    response = requests.post(url, headers=headers, files=files, data=data)
    print(response.json())
```

### Example: JavaScript (Fetch API)

```javascript
const formData = new FormData();
const fileInput = document.getElementById('fileInput');
formData.append('file', fileInput.files[0]);
formData.append('source_system', 'SalesForce');
formData.append('batch_id', 'batch_001');
formData.append('metadata', JSON.stringify({ department: 'sales' }));

fetch('http://localhost:8007/api/v1/files/upload', {
  method: 'POST',
  headers: {
    'Authorization': 'Bearer YOUR_JWT_TOKEN'
  },
  body: formData
})
  .then(res => res.json())
  .then(data => console.log('Upload successful:', data.data.file_id))
  .catch(err => console.error('Upload failed:', err));
```

### Error Responses

**400 Bad Request** — Invalid file type or missing required fields
```json
{
  "status": "error",
  "data": null,
  "metas": {
    "message": "File customers.csv has unsupported type application/x-msdownload"
  }
}
```

**413 Payload Too Large** — File exceeds size limit
```json
{
  "status": "error",
  "data": null,
  "metas": {
    "message": "File size exceeds maximum allowed (500 MB)"
  }
}
```

**401 Unauthorized** — Missing or invalid token
```json
{
  "status": "error",
  "data": null,
  "metas": {
    "message": "Not authenticated"
  }
}
```

---

## 2. Batch File Upload

Upload multiple files in a single request.

### Endpoint
```
POST /api/v1/files/batch-upload
```

### Request

**Multipart Form Data:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `files` | File[] | ✅ | Array of files to upload |
| `source_system` | string | ✅ | Source system identifier |
| `batch_id` | string | ❌ | Batch identifier for grouping files |

### Response

**Status:** `200 OK`
```json
{
  "status": "success",
  "data": {
    "batch_id": "batch_001",
    "total_files": 3,
    "uploaded": [
      {
        "file_id": "550e8400-e29b-41d4-a716-446655440001",
        "file_name": "customers.csv",
        "file_size": 1024000,
        "status": "success"
      },
      {
        "file_id": "550e8400-e29b-41d4-a716-446655440002",
        "file_name": "products.xlsx",
        "file_size": 512000,
        "status": "success"
      },
      {
        "file_id": "550e8400-e29b-41d4-a716-446655440003",
        "file_name": "orders.json",
        "file_size": 256000,
        "status": "success"
      }
    ],
    "failed": []
  },
  "metas": {
    "message": "Batch upload completed: 3/3 files uploaded"
  }
}
```

### Example: cURL

```bash
curl -X POST http://localhost:8007/api/v1/files/batch-upload \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -F "files=@customers.csv" \
  -F "files=@products.xlsx" \
  -F "files=@orders.json" \
  -F "source_system=SalesForce" \
  -F "batch_id=batch_001"
```

### Example: Python

```python
import requests

url = "http://localhost:8007/api/v1/files/batch-upload"
headers = {"Authorization": "Bearer YOUR_JWT_TOKEN"}

files = [
    ("files", ("customers.csv", open("customers.csv", "rb"), "text/csv")),
    ("files", ("products.xlsx", open("products.xlsx", "rb"), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")),
    ("files", ("orders.json", open("orders.json", "rb"), "application/json"))
]

data = {
    "source_system": "SalesForce",
    "batch_id": "batch_001"
}

response = requests.post(url, headers=headers, files=files, data=data)
print(response.json())

# Close files
for _, (filename, file_obj, _) in files:
    file_obj.close()
```

---

## 3. Chunked File Upload

For large files (> 100 MB), use chunked upload with resume capability.

### Step 1: Initiate Upload Session

**Endpoint:**
```
POST /api/v1/files/upload/session
```

**Request:**
```json
{
  "file_name": "large_dataset.csv",
  "file_size": 524288000,
  "file_type": "CSV",
  "source_system": "SalesForce",
  "batch_id": "batch_001",
  "metadata": "{\"department\": \"sales\"}"
}
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "session_id": "660f9410-f40d-42e5-b827-557766551111",
    "chunk_size": 5242880,
    "total_chunks": 100,
    "expires_at": "2026-07-03T10:30:00Z",
    "status": "pending"
  },
  "metas": {
    "message": "Upload session initiated"
  }
}
```

**Parameters:**
| Parameter | Type | Range | Notes |
|-----------|------|-------|-------|
| `file_name` | string | Max 255 chars | Original filename with extension |
| `file_size` | integer | >= 1 byte | Total file size in bytes |
| `file_type` | enum | CSV, EXCEL, JSON, XML, API | Normalized to uppercase |
| `source_system` | string | Max 100 chars | System identifier |
| `batch_id` | string | Max 50 chars | Optional batch grouping |
| `metadata` | string | Max 1000 chars | JSON string only |

**Chunk Size Calculation:**
- Default chunk size: 5 MB (5,242,880 bytes)
- `total_chunks = ceil(file_size / chunk_size)`
- Example: 500 MB file → 100 chunks of 5 MB each

### Step 2: Upload Chunks

**Endpoint:**
```
POST /api/v1/files/upload/{session_id}/{chunk_index}
```

**Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `session_id` | UUID (path) | Session ID from Step 1 |
| `chunk_index` | integer (path) | Chunk index (0-based) |

**Request Body:**
Raw binary chunk data (no multipart required)

**Headers (recommended):**
```
Content-Type: application/octet-stream
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "session_id": "660f9410-f40d-42e5-b827-557766551111",
    "status": "uploading",
    "received_bytes": 10485760,
    "uploaded_chunks": 2,
    "total_chunks": 100,
    "progress_percent": 2.0
  },
  "metas": {
    "message": "Chunk 2 uploaded"
  }
}
```

### Step 3: Get Session Status (For Resume)

Before resuming, check which chunks were already received.

**Endpoint:**
```
GET /api/v1/files/upload/session/{session_id}
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "session_id": "660f9410-f40d-42e5-b827-557766551111",
    "status": "uploading",
    "file_name": "large_dataset.csv",
    "file_size": 524288000,
    "received_bytes": 10485760,
    "uploaded_chunks": 2,
    "total_chunks": 100,
    "chunk_size": 5242880,
    "chunk_map": {
      "0": true,
      "1": true
    },
    "progress_percent": 2.0,
    "file_registry_id": null,
    "expires_at": "2026-07-03T10:30:00Z"
  },
  "metas": {
    "message": "Session status retrieved"
  }
}
```

**`chunk_map` Explanation:**
- Key: chunk index (as string)
- Value: `true` if chunk received, `false` if missing
- Example: `{"0": true, "1": true, "2": false}` = chunks 0, 1 uploaded; skip chunk 2 on resume

---

## 4. Chunked Upload Workflow

### Complete Example: Python

```python
import requests
import time
from pathlib import Path

BASE_URL = "http://localhost:8007/api/v1/files"
JWT_TOKEN = "YOUR_JWT_TOKEN"
headers = {"Authorization": f"Bearer {JWT_TOKEN}"}

# Configuration
FILE_PATH = "large_dataset.csv"
CHUNK_SIZE = 5 * 1024 * 1024  # 5 MB
SOURCE_SYSTEM = "SalesForce"
BATCH_ID = "batch_001"

file_size = Path(FILE_PATH).stat().st_size
total_chunks = (file_size + CHUNK_SIZE - 1) // CHUNK_SIZE

print(f"File size: {file_size} bytes, Total chunks: {total_chunks}")

# Step 1: Initiate session
print("\n[Step 1] Initiating upload session...")
init_response = requests.post(
    f"{BASE_URL}/upload/session",
    headers=headers,
    json={
        "file_name": Path(FILE_PATH).name,
        "file_size": file_size,
        "file_type": "CSV",
        "source_system": SOURCE_SYSTEM,
        "batch_id": BATCH_ID
    }
)
session_data = init_response.json()["data"]
session_id = session_data["session_id"]
print(f"Session ID: {session_id}")
print(f"Chunk size: {session_data['chunk_size']} bytes")

# Step 2: Upload chunks with resume capability
print("\n[Step 2] Uploading chunks...")
with open(FILE_PATH, "rb") as f:
    for chunk_index in range(total_chunks):
        # Check status before uploading (useful for resume)
        if chunk_index % 10 == 0:
            status_resp = requests.get(
                f"{BASE_URL}/upload/session/{session_id}",
                headers=headers
            )
            status_data = status_resp.json()["data"]
            print(f"Progress: {status_data['progress_percent']:.1f}% ({status_data['uploaded_chunks']}/{total_chunks})")

        # Seek to chunk position
        f.seek(chunk_index * CHUNK_SIZE)
        chunk_data = f.read(CHUNK_SIZE)

        # Upload chunk
        upload_resp = requests.post(
            f"{BASE_URL}/upload/{session_id}/{chunk_index}",
            headers=headers,
            data=chunk_data,
            headers={**headers, "Content-Type": "application/octet-stream"}
        )

        if upload_resp.status_code != 200:
            print(f"Chunk {chunk_index} failed: {upload_resp.text}")
            break
        else:
            chunk_result = upload_resp.json()["data"]
            print(f"Chunk {chunk_index}: {chunk_result['progress_percent']:.1f}% complete")

# Step 3: Verify completion
print("\n[Step 3] Verifying upload completion...")
final_status = requests.get(
    f"{BASE_URL}/upload/session/{session_id}",
    headers=headers
).json()["data"]

print(f"Final status: {final_status['status']}")
print(f"File ID: {final_status.get('file_registry_id')}")
print(f"Progress: {final_status['progress_percent']:.1f}%")

if final_status.get('file_registry_id'):
    print(f"\n✅ Upload complete! File ID: {final_status['file_registry_id']}")
else:
    print(f"\n⚠️ Upload in progress or incomplete")
```

### Resume Upload (After Interruption)

```python
import requests
from pathlib import Path

BASE_URL = "http://localhost:8007/api/v1/files"
JWT_TOKEN = "YOUR_JWT_TOKEN"
headers = {"Authorization": f"Bearer {JWT_TOKEN}"}

FILE_PATH = "large_dataset.csv"
SESSION_ID = "660f9410-f40d-42e5-b827-557766551111"  # From previous attempt
CHUNK_SIZE = 5 * 1024 * 1024

# Get current status
status_resp = requests.get(
    f"{BASE_URL}/upload/session/{SESSION_ID}",
    headers=headers
)
status_data = status_resp.json()["data"]
chunk_map = status_data.get("chunk_map", {})
total_chunks = status_data["total_chunks"]

print(f"Resuming session: {SESSION_ID}")
print(f"Already uploaded chunks: {len([c for c in chunk_map.values() if c])}/{total_chunks}")

# Upload only missing chunks
with open(FILE_PATH, "rb") as f:
    for chunk_index in range(total_chunks):
        if chunk_map.get(str(chunk_index)):
            print(f"Chunk {chunk_index}: Already uploaded, skipping")
            continue

        # Upload missing chunk
        f.seek(chunk_index * CHUNK_SIZE)
        chunk_data = f.read(CHUNK_SIZE)

        upload_resp = requests.post(
            f"{BASE_URL}/upload/{SESSION_ID}/{chunk_index}",
            headers={**headers, "Content-Type": "application/octet-stream"},
            data=chunk_data
        )

        if upload_resp.status_code == 200:
            print(f"Chunk {chunk_index}: Uploaded")
        else:
            print(f"Chunk {chunk_index}: Failed - {upload_resp.text}")

print("✅ Resume complete!")
```

### JavaScript Example

```javascript
const BASE_URL = "http://localhost:8007/api/v1/files";
const JWT_TOKEN = "YOUR_JWT_TOKEN";
const CHUNK_SIZE = 5 * 1024 * 1024; // 5 MB

async function uploadFileChunked(file, sourceSystem, batchId) {
  const headers = { "Authorization": `Bearer ${JWT_TOKEN}` };
  const totalChunks = Math.ceil(file.size / CHUNK_SIZE);

  // Step 1: Initiate session
  console.log("Initiating upload session...");
  const initRes = await fetch(`${BASE_URL}/upload/session`, {
    method: "POST",
    headers: { ...headers, "Content-Type": "application/json" },
    body: JSON.stringify({
      file_name: file.name,
      file_size: file.size,
      file_type: "CSV",
      source_system: sourceSystem,
      batch_id: batchId
    })
  });

  const { data: sessionData } = await initRes.json();
  const sessionId = sessionData.session_id;
  console.log(`Session ID: ${sessionId}`);

  // Step 2: Upload chunks
  console.log(`Uploading ${totalChunks} chunks...`);
  for (let chunkIndex = 0; chunkIndex < totalChunks; chunkIndex++) {
    const start = chunkIndex * CHUNK_SIZE;
    const end = Math.min(start + CHUNK_SIZE, file.size);
    const chunk = file.slice(start, end);

    const uploadRes = await fetch(
      `${BASE_URL}/upload/${sessionId}/${chunkIndex}`,
      {
        method: "POST",
        headers: { ...headers, "Content-Type": "application/octet-stream" },
        body: chunk
      }
    );

    const { data: chunkData } = await uploadRes.json();
    console.log(
      `Chunk ${chunkIndex}/${totalChunks}: ${chunkData.progress_percent.toFixed(1)}%`
    );
  }

  // Step 3: Get final status
  const statusRes = await fetch(
    `${BASE_URL}/upload/session/${sessionId}`,
    { headers }
  );
  const { data: finalStatus } = await statusRes.json();

  console.log(`Upload complete! File ID: ${finalStatus.file_registry_id}`);
  return finalStatus.file_registry_id;
}

// Usage
const fileInput = document.getElementById('fileInput');
fileInput.addEventListener('change', async (e) => {
  const file = e.target.files[0];
  if (file) {
    const fileId = await uploadFileChunked(
      file,
      "SalesForce",
      "batch_001"
    );
    console.log(`Uploaded file: ${fileId}`);
  }
});
```

---

## 5. Upload Session Management

### Session Expiration

- **Default TTL:** 24 hours
- **Automatic cleanup:** Expired sessions deleted from database
- **Resume:** Only possible within TTL

### Chunk Constraints

- **Minimum chunk size:** 1 byte
- **Maximum chunk size:** Limited by available memory (typically 100 MB)
- **Default chunk size:** 5 MB (from API server)

### Storage Locations

- **Upload directory:** `/app/storage/uploads/`
- **Temp chunks:** `/app/storage/temp/uploads/{session_id}/`
- **Final file:** `/app/storage/uploads/{file_id}/{file_name}`

---

## 6. Auto-Processing Files

Both upload methods support auto-processing via trigger after registration.

### Enable Auto-Processing

**Request parameter (regular upload):**
```
POST /api/v1/files/upload?auto_process=true
```

**Response includes processing status:**
```json
{
  "data": {
    "file_id": "550e8400-e29b-41d4-a716-446655440000",
    "processing_status": "QUEUED"
  }
}
```

### Processing Stages

1. **PENDING** → File registered, awaiting processing
2. **QUEUED** → Celery task queued
3. **PROCESSING** → Extract phase running
4. **COMPLETED** → All phases done
5. **FAILED** → Error during processing

### Check Processing Status

```
GET /api/v1/files/{file_id}
```

Response includes `processing_status`, progress %, and detailed status.

---

## 7. Error Handling & Troubleshooting

### Common Upload Errors

**Error: Unsupported file type**
```
Status: 400
Message: "File customers.csv has unsupported type application/x-msdownload"
```
**Solution:** Use one of: CSV, EXCEL, JSON, XML

**Error: File too large**
```
Status: 413
Message: "File size exceeds maximum allowed (500 MB)"
```
**Solution:** Use chunked upload, or contact admin to increase limit

**Error: Session expired**
```
Status: 410
Message: "Upload session expired"
```
**Solution:** Initiate new upload session

**Error: Chunk out of order**
```
Status: 400
Message: "Chunk index 5 invalid (total chunks: 3)"
```
**Solution:** Verify total_chunks and chunk_index calculations

### Debug Logging

Enable debug logging to inspect upload flow:

```bash
# Docker
docker-compose logs -f fastapi | grep -i "upload"

# Local
tail -f logs/app.log | grep -i "upload"
```

### Storage Cleanup

Manually remove old upload sessions:

```python
# In manage.py or app context
from app.application.services.file_service import FileService

file_service = FileService(db)
deleted_count = await file_service.cleanup_expired_sessions()
print(f"Cleaned up {deleted_count} expired sessions")
```

---

## 8. API Quick Reference

### Endpoints Summary

| Method | Endpoint | Purpose |
|--------|----------|---------|
| POST | `/upload` | Single file upload |
| POST | `/batch-upload` | Multiple files upload |
| POST | `/upload/session` | Initiate chunked upload |
| POST | `/upload/{session_id}/{chunk_index}` | Upload chunk |
| GET | `/upload/session/{session_id}` | Get session status |

### Authentication

All endpoints require JWT token:
```
Authorization: Bearer {JWT_TOKEN}
```

### Response Format

All responses follow standard format:
```json
{
  "status": "success|error",
  "data": { /* endpoint-specific data */ },
  "metas": {
    "message": "Human-readable message"
  }
}
```

---

## 9. Best Practices

1. **Regular uploads:** Use for files < 100 MB
2. **Large files:** Use chunked upload for > 100 MB
3. **Resume capability:** Always implement chunk status check before uploading
4. **Batch processing:** Group related files with same `batch_id`
5. **Metadata:** Include source_system for audit trail
6. **Error handling:** Implement retry logic with exponential backoff
7. **Monitoring:** Track `upload_date` and `processing_status` for SLA compliance
8. **Session cleanup:** Monitor expired sessions to prevent disk bloat

---

## 10. Production Deployment Checklist

Before going live:

- [ ] Configure storage path (writable, sufficient disk space)
- [ ] Set max file size limit in config
- [ ] Configure session TTL (24h recommended)
- [ ] Enable CORS for browser uploads
- [ ] Set up monitoring for upload failures
- [ ] Configure email notifications on errors
- [ ] Test chunked upload with large files (> 500 MB)
- [ ] Set up automated cleanup job for expired sessions
- [ ] Document source_system values for team
- [ ] Brief team on batch_id naming convention

---

## 📞 Support

For issues, refer to:
- **Setup:** `CLAUDE.md` (project context)
- **CLI:** `CLI_GUIDE.md` (command reference)
- **Architecture:** `SEQUENCE.md` (data flow)
- **Operations:** `PRODUCTION_READINESS.md`
