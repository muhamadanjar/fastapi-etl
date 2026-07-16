# ETL API Documentation

Welcome to the FastAPI ETL Service documentation. This guide helps you understand, use, and operate the ETL system.

## 📚 Documentation Map

### Getting Started
- **[QUICK_REFERENCE.md](./QUICK_REFERENCE.md)** — 5-min overview of key components, CLI commands, and architecture
- **[CLI_GUIDE.md](./CLI_GUIDE.md)** — Complete command reference for `python manage.py`

### Feature Guides
- **[UPLOADS_API.md](./UPLOADS_API.md)** ✨ NEW — File upload API (regular + chunked uploads with resume)
- **[SEQUENCE.md](./SEQUENCE.md)** — Complete ETL workflow & data flow diagrams
- **[CELERY.md](./CELERY.md)** — Celery worker configuration & task patterns

### Production & Operations
- **[PRODUCTION_READINESS.md](./PRODUCTION_READINESS.md)** — Deployment checklist, monitoring, and runbooks

---

## 🚀 Quick Start

### 1. Start Development Server
```bash
python manage.py runserver --reload
```

### 2. Upload a File
```bash
curl -X POST http://localhost:8007/api/v1/files/upload \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -F "file=@data.csv" \
  -F "source_system=SalesForce"
```

### 3. Check Processing Status
```bash
curl http://localhost:8007/api/v1/files/{file_id} \
  -H "Authorization: Bearer YOUR_TOKEN"
```

See **[UPLOADS_API.md](./UPLOADS_API.md)** for complete examples.

---

## 📖 Documentation by Role

### Data Engineers
Start with:
1. [QUICK_REFERENCE.md](./QUICK_REFERENCE.md) — understand architecture (5 min)
2. [SEQUENCE.md](./SEQUENCE.md) — learn ETL workflow (15 min)
3. [UPLOADS_API.md](./UPLOADS_API.md) — master file uploads (10 min)

### DevOps / Operations
Start with:
1. [QUICK_REFERENCE.md](./QUICK_REFERENCE.md) — key components (5 min)
2. [PRODUCTION_READINESS.md](./PRODUCTION_READINESS.md) — deployment & monitoring (20 min)
3. [CLI_GUIDE.md](./CLI_GUIDE.md) — operational commands (10 min)

### Backend Developers
Start with:
1. [QUICK_REFERENCE.md](./QUICK_REFERENCE.md) — architecture overview (5 min)
2. [SEQUENCE.md](./SEQUENCE.md) — data flow & phases (20 min)
3. [CELERY.md](./CELERY.md) — task patterns (15 min)
4. [UPLOADS_API.md](./UPLOADS_API.md) — upload implementation (10 min)

### Frontend Developers (Integrating Uploads)
Start with:
1. [UPLOADS_API.md](./UPLOADS_API.md) — API reference (10 min)
   - Regular upload example (JavaScript Fetch)
   - Chunked upload with resume (JavaScript)
   - Progress tracking

### Integration Partners
Start with:
1. [QUICK_REFERENCE.md](./QUICK_REFERENCE.md) — overview (5 min)
2. [UPLOADS_API.md](./UPLOADS_API.md) — file upload endpoints (15 min)
3. [SEQUENCE.md](./SEQUENCE.md) — full ETL workflow (20 min)

---

## 🎯 Common Tasks

### Upload a Large File
→ [UPLOADS_API.md § 3. Chunked File Upload](./UPLOADS_API.md#3-chunked-file-upload)

### Resume Interrupted Upload
→ [UPLOADS_API.md § 4. Resume Upload](./UPLOADS_API.md#resume-upload-after-interruption)

### Monitor Job Processing
→ [PRODUCTION_READINESS.md § Monitoring](./PRODUCTION_READINESS.md)

### Configure Celery Workers
→ [CELERY.md § Worker Configuration](./CELERY.md)

### Debug Upload Failures
→ [UPLOADS_API.md § 7. Error Handling](./UPLOADS_API.md#7-error-handling--troubleshooting)

### Set Up Production Deployment
→ [PRODUCTION_READINESS.md § Deployment](./PRODUCTION_READINESS.md)

---

## 📊 System Architecture

```
User Request
    ↓
[Phase 1] Authentication (JWT)
    ↓
[Phase 2] Job Creation (Transaction)
    ↓
[Phase 3] File Upload (Regular or Chunked)
    ↓
[Phase 4] Execute Trigger (Celery Queue)
    ↓
Celery Task Worker
    ├─ [Phase 5] Extract (Multi-format)
    ├─ [Phase 6] Transform (Clean, Map, Validate)
    ├─ [Phase 7] Load (Entity Matching, Dedup)
    │
    └─ [Phase 8] Post-Processing
        ├─ Calculate Metrics
        ├─ Check Quality
        ├─ Trigger Children
        ├─ Publish Events
        └─ Send Notifications
    ↓
[Phase 9] Monitoring (Dashboards)
```

For detailed flow, see [SEQUENCE.md](./SEQUENCE.md).

---

## 🔑 Key Features

| Feature | Doc | Status |
|---------|-----|--------|
| Regular file upload | [UPLOADS_API.md](./UPLOADS_API.md) | ✅ |
| Chunked upload with resume | [UPLOADS_API.md](./UPLOADS_API.md) | ✅ |
| Batch file upload | [UPLOADS_API.md](./UPLOADS_API.md) | ✅ |
| Multi-format support (CSV, Excel, JSON, XML) | [SEQUENCE.md](./SEQUENCE.md) | ✅ |
| Data validation & cleaning | [SEQUENCE.md](./SEQUENCE.md) | ✅ |
| Field mapping & transformation | [SEQUENCE.md](./SEQUENCE.md) | ✅ |
| Entity matching & deduplication | [SEQUENCE.md](./SEQUENCE.md) | ✅ |
| Job dependencies & orchestration | [QUICK_REFERENCE.md](./QUICK_REFERENCE.md) | ✅ |
| Quality threshold & alerts | [QUICK_REFERENCE.md](./QUICK_REFERENCE.md) | ✅ |
| Async task processing (Celery) | [CELERY.md](./CELERY.md) | ✅ |

---

## 🛠 Configuration

### Environment Variables

**Core:**
```bash
DATABASE_URL=postgresql://...
REDIS_URL=redis://...
CELERY_BROKER_URL=redis://...
```

**File Upload:**
```bash
STORAGE_PATH=/app/storage/uploads
MAX_FILE_SIZE=524288000  # 500 MB
UPLOAD_SESSION_TTL=86400  # 24 hours
```

**Notifications:**
```bash
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
QUALITY_THRESHOLD=80.0
```

See `app/core/config.py` for full list.

---

## 📞 Support & Debugging

### Health Check
```bash
curl http://localhost:8007/health
```

### View Logs
```bash
# Docker
docker-compose logs -f fastapi

# Local
tail -f logs/app.log
```

### Common Issues

| Issue | Solution |
|-------|----------|
| Upload fails with "unsupported type" | Check [UPLOADS_API.md § Supported File Types](./UPLOADS_API.md#supported-file-types) |
| Chunks not resuming | Verify session hasn't expired (24h TTL) |
| Job not processing | Check Celery worker status: `docker-compose logs celery` |
| High memory usage | Monitor chunked uploads, increase swap space |
| Database connection errors | Verify DATABASE_URL and PostgreSQL service |

### Get Help

1. Check relevant documentation section
2. Review logs with grep for errors:
   ```bash
   docker-compose logs | grep ERROR
   ```
3. Run health checks:
   ```bash
   python manage.py task stats
   python manage.py worker status
   ```

---

## 📈 Version & Status

| Component | Version | Status |
|-----------|---------|--------|
| API | 1.1.0 | ✅ Production Ready |
| Upload Feature | 1.0.0 | ✅ Production Ready |
| Celery Integration | 1.0.0 | ✅ Production Ready |
| Monitoring | 1.0.0 | ✅ Production Ready |

**Last Updated:** 2026-07-02

---

## 🗺 Documentation Navigation

- [QUICK_REFERENCE.md](./QUICK_REFERENCE.md) — Key files, CLI, phases overview
- [CLI_GUIDE.md](./CLI_GUIDE.md) — Command reference
- [UPLOADS_API.md](./UPLOADS_API.md) — File upload guide (NEW)
- [SEQUENCE.md](./SEQUENCE.md) — ETL workflow & data flow
- [CELERY.md](./CELERY.md) — Celery configuration & tasks
- [PRODUCTION_READINESS.md](./PRODUCTION_READINESS.md) — Deployment & operations
