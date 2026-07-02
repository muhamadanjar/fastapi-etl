# ETL API — CLI Command Guide

**Service:** `services/etl_api`  
**Entry point:** `python manage.py`  
**Framework:** Typer + Rich + Auto-discovery  

---

## Daftar Isi

1. [Quick Start](#quick-start)
2. [Built-in Commands](#built-in-commands)
   - [runserver](#runserver)
   - [shell](#shell)
   - [flower](#flower)
3. [Custom Commands](#custom-commands)
   - [clear-cache](#clear-cache)
   - [migrate](#migrate)
   - [seed](#seed)
4. [Worker Commands](#worker-commands)
5. [Task Commands](#task-commands)
6. [Cheat Sheet](#cheat-sheet)
7. [Menambah Command Baru](#menambah-command-baru)

---

## Quick Start

```bash
cd services/etl_api
source venv/bin/activate

# Lihat semua command
python manage.py --help

# Help untuk command spesifik
python manage.py worker --help
python manage.py seed --help
```

---

## Built-in Commands

### runserver

Menjalankan FastAPI development server.

```bash
python manage.py runserver                          # http://127.0.0.1:8000
python manage.py runserver --host 0.0.0.0 --port 8080
python manage.py runserver --reload                 # Auto-reload
```

### shell

Interactive Python shell dengan app context lengkap.

```bash
python manage.py shell
```

**Variable yang tersedia:** `app`, `db`, `cache`, `User`, `EtlJob`, `JobExecution`, `FileRegistry`, `DataSource`, `SystemConfig`, `ErrorLog`, `ProcessedEntity`

```python
# Contoh di dalam shell:
db.query(User).all()
db.query(EtlJob).filter(EtlJob.status == 'active').all()
cache.get('key')
```

> Gunakan IPython jika terinstall (auto-detect). Fallback ke Python shell biasa.

### flower

Celery Flower monitoring dashboard.

```bash
python manage.py flower                    # http://localhost:5555
python manage.py flower --port 6666
```

---

## Custom Commands

### clear-cache

Membersihkan Redis cache.

```bash
# Hapus dengan pattern
python manage.py clear-cache --pattern "auth:*"
python manage.py clear-cache -p "job:*"

# Hapus semua cache
python manage.py clear-cache --flush-all

# Dry run (preview tanpa eksekusi)
python manage.py clear-cache --pattern "file:*" --dry-run
python manage.py clear-cache --flush-all --dry-run
```

| Option | Keterangan |
|---|---|
| `--pattern`, `-p` | Pattern Redis key |
| `--flush-all` | Hapus SEMUA cache |
| `--dry-run` | Preview tanpa eksekusi |

### migrate

Database migrations via Alembic (dengan fallback jika Alembic tidak terinstall).

```bash
python manage.py migrate                   # Run semua pending
python manage.py migrate --check           # Cek pending (exit 1 jika ada)
python manage.py migrate --fake            # Fake migrations
python manage.py migrate etl_control       # App spesifik
```

| Option | Keterangan |
|---|---|
| `--check` | Cek pending migrations |
| `--fake` | Tandai tanpa eksekusi SQL |
| `[APP_NAME]` | (Positional) Nama app |

### seed

Mengisi database dengan data dummy menggunakan Faker.

```bash
python manage.py seed                              # Semua model, 10 record
python manage.py seed --count 50                   # 50 record per model
python manage.py seed --model users --count 100    # 100 users
python manage.py seed -m jobs                      # ETL jobs
python manage.py seed -m files                     # File entries
python manage.py seed --flush --count 20           # Hapus dulu baru seed
```

**Model yang bisa di-seed:**

| Model | Data yang dibuat |
|---|---|
| `users` | username, email, name, password (hashed `password123`), random superuser/verified |
| `jobs` | name, description, job_type, status, schedule, config JSON |
| `files` | filename, file_path, file_size, mime_type, status, metadata |

| Option | Keterangan |
|---|---|
| `--model`, `-m` | `users`, `jobs`, `files` (default: semua) |
| `--count`, `-c` | Jumlah record (default: 10) |
| `--flush` | Hapus data existing dulu |

---

## Worker Commands

Kelompok command di bawah `python manage.py worker <subcommand>`.

```bash
python manage.py worker --help
```

### worker start

```bash
python manage.py worker start                          # Semua worker types
python manage.py worker start -t email                 # Email worker
python manage.py worker start -t default -d            # Background
python manage.py worker start --dry-run                # Preview
```

**Worker types:** `default`, `email`, `data_sync`, `priority`, `all`

### worker stop

```bash
python manage.py worker stop --all-workers
python manage.py worker stop -n celery@default
```

### worker restart

```bash
python manage.py worker restart -t email
python manage.py worker restart --worker-type default
```

### worker status

```bash
python manage.py worker status
python manage.py worker status --format json
```

### worker scale

```bash
python manage.py worker scale -t default -c 8
python manage.py worker scale --worker-type email --concurrency 4
```

### worker queues

```bash
python manage.py worker queues
python manage.py worker queues --format json
```

### worker purge

```bash
python manage.py worker purge -q etl
python manage.py worker purge --queue default --force    # Skip konfirmasi
```

### worker beat

```bash
python manage.py worker beat
python manage.py worker beat --detach
python manage.py worker beat --dry-run
```

### worker flower

```bash
python manage.py worker flower --port 6666
```

### worker systemd

```bash
python manage.py worker systemd -t default
python manage.py worker systemd -t email --output /etc/systemd/system/etl-email.service
```

### worker docker-compose

```bash
python manage.py worker docker-compose
python manage.py worker docker-compose --output custom-workers.yml
```

---

## Task Commands

Kelompok command di bawah `python manage.py task <subcommand>`.

### task list

```bash
python manage.py task list                     # Default 20 tasks
python manage.py task list --limit 50          # Custom limit
python manage.py task list --status FAILURE    # Filter by status
python manage.py task list --format json       # JSON output
```

### task show

```bash
python manage.py task show <task-id>
python manage.py task show <task-id> --format json
```

### task cancel

```bash
python manage.py task cancel <task-id>
python manage.py task cancel <task-id> --force    # Skip konfirmasi
```

### task stats

```bash
python manage.py task stats
python manage.py task stats --format json
```

---

## Cheat Sheet

```bash
# ─── Development ──────────────────────────────────────
python manage.py runserver                        # :8000
python manage.py runserver --reload               # Auto-reload
python manage.py shell                            # Interactive shell

# ─── Database ────────────────────────────────────────
python manage.py migrate                          # Run migrations
python manage.py migrate --check                  # Cek pending
python manage.py seed                             # Seed 10 records
python manage.py seed -m users -c 100             # 100 users
python manage.py seed --flush                     # Flush + seed

# ─── Cache ───────────────────────────────────────────
python manage.py clear-cache -p "auth:*"          # Pattern
python manage.py clear-cache --flush-all           # All cache
python manage.py clear-cache -p "*" --dry-run      # Preview

# ─── Workers ─────────────────────────────────────────
python manage.py worker start                     # Start all
python manage.py worker start -t email            # Email only
python manage.py worker status                    # Status
python manage.py worker stop --all-workers        # Stop all
python manage.py worker restart -t default        # Restart
python manage.py worker scale -t default -c 4     # Scale
python manage.py worker purge -q etl              # Purge queue
python manage.py worker beat                      # Beat scheduler
python manage.py worker docker-compose            # Docker compose

# ─── Tasks ───────────────────────────────────────────
python manage.py task list                        # Recent tasks
python manage.py task show <id>                   # Detail
python manage.py task cancel <id>                 # Cancel
python manage.py task stats                       # Stats

# ─── Monitoring ─────────────────────────────────────
python manage.py flower                           # Dashboard :5555
python manage.py worker queues                    # Queue info
```

---

## Menambah Command Baru

1. Buat file di `commands/my_command.py`:

```python
from commands.base import BaseCommand
import typer

class Command(BaseCommand):
    help = "Deskripsi command"

    def add_arguments(self):
        return {
            'name': typer.Option('World', '--name', '-n', help='Nama'),
            'verbose': typer.Option(False, '--verbose', '-v', help='Verbose'),
        }

    def handle(self, name: str, verbose: bool, **options):
        self.print_header(f"Hello, {name}!")
        if verbose:
            self.info("Verbose mode enabled")
        self.success("Done!")
```

2. Command langsung tersedia: `python manage.py my-command --name ETL`

**Untuk command dengan subcommands (seperti `worker`):**

Buat beberapa class `BaseCommand` dalam satu file, dengan nama `WorkerXxxCommand`. Auto-discovery akan otomatis mendaftarkannya sebagai Typer group.

```python
class WorkerStartCommand(BaseCommand):
    help = "Start workers"
    # ...

class WorkerStopCommand(BaseCommand):
    help = "Stop workers"
    # ...
```

---

## Arsitektur

```
manage.py  ──Typer App──┬── Auto-discovery ── commands/*.py
                        │       ├── clear_cache.py   → clear-cache
                        │       ├── migrate.py       → migrate
                        │       ├── seed.py          → seed
                        │       ├── worker.py        → worker (group, 11 cmd)
                        │       └── task.py          → task (group, 4 cmd)
                        │
                        └── Built-in ── runserver, shell, flower
```

---

## Referensi

- [CELERY.md](./CELERY.md) — Celery worker guide (raw + manage.py)
- [QUICK_REFERENCE.md](./QUICK_REFERENCE.md) — Quick reference
- [PRODUCTION_READINESS.md](./PRODUCTION_READINESS.md) — Production deployment
