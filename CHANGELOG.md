# ETL Changelog

## [1.1.0] - 2026-07-02

### CLI Refactor — Typer + Rich

- **BREAKING**: Hapus `app/interfaces/cli/` (argparse + click campuran)
- **NEW**: `manage.py` entry point (Django-style, Typer app)
- **NEW**: `commands/` directory dengan auto-discovery
- **NEW**: `commands/base.py` — BaseCommand dengan typer + rich
- **NEW**: 17+ commands terintegrasi:
  - `runserver`, `shell`, `flower` (built-in)
  - `clear-cache`, `migrate`, `seed` (custom)
  - `worker start/stop/restart/status/scale/queues/purge/beat/flower/systemd/docker-compose`
  - `task list/show/cancel/stats`
- **CHANGED**: `clear-cache` — dari argparse ke typer, tambah `--dry-run`
- **CHANGED**: `migrate` — dari placeholder ke Alembic real integration + fallback
- **CHANGED**: `seed` — dari print loop kosong ke Faker + real DB insert (users, jobs, files)
- **CHANGED**: `worker` — dari click ke typer group (11 subcommands)
- **NEW**: `task` — command group baru untuk task management (sebelumnya campur di worker.py)
- **CHANGED**: Output format dari ANSI escape codes ke Rich (Panel, Text, colors)

### Documentation

- **NEW**: `docs/CLI_GUIDE.md` — panduan lengkap CLI commands
- **UPDATED**: `readme.md` — tambah section CLI Management Commands, update struktur folder
- **UPDATED**: `docs/QUICK_REFERENCE.md` — tambah CLI section, update version
- **UPDATED**: `docs/CELERY.md` — tambah `manage.py worker` dan `manage.py task` references

### Dependencies Added

- `typer` — CLI framework
- `rich` — Terminal formatting

---