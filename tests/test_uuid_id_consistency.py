"""
Smoke test: UUID ID-type consistency for fastapi-etl.

Guards the int->UUID normalization (and phantom `.execution_id` -> `.id` fix)
so a regression can't silently reintroduce an integer ID or a broken attribute
access.

Run with:  pytest tests/test_uuid_id_consistency.py -q
(Confined to this file/dir so the unrelated broken
 app/infrastructure/background/__init__.py import is never collected.)
"""
import ast
import os
import re
import uuid

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
APP_DIR = os.path.join(REPO_ROOT, "app")


# ---------------------------------------------------------------------------
# 1) Every edited module still imports cleanly (catches NameError / SyntaxError
#    from a dropped paren or a misplaced `from uuid import UUID`).
# ---------------------------------------------------------------------------
EDITED_MODULES = [
    "app.schemas.entity_schemas",
    "app.schemas.quality_schemas",
    "app.schemas.job_schemas",
    "app.schemas.data_quality_schema",
    "app.schemas.transformation",
    "app.schemas.response_schemas",
    "app.schemas.file_upload",
    "app.infrastructure.db.models.etl_control.etl_jobs",
    "app.infrastructure.db.models.etl_control.job_executions",
    "app.infrastructure.db.models.etl_control.quality_rules",
    "app.infrastructure.db.models.staging.standardized_data",
    "app.infrastructure.db.models.raw_data.raw_records",
    "app.infrastructure.db.models.raw_data.column_structure",
    "app.infrastructure.db.models.transformation.field_mappings",
    "app.interfaces.http.routes.data_quality",
    "app.interfaces.http.routes.entities",
    "app.application.services.etl_service",
    "app.application.services.monitoring_service",
    "app.application.services.data_quality_service",
    "app.application.services.entity_service",
    "app.application.services.auth_service",
    "app.application.services.notification_service",
    "app.application.services.file_service",
    "app.core.response_examples",
    "app.utils.security",
]


@pytest.mark.parametrize("module_name", EDITED_MODULES)
def test_edited_module_imports(module_name):
    __import__(module_name)


# ---------------------------------------------------------------------------
# 2) Source scan: no int-typed PK/FK ID annotations remain anywhere in app/
#    (the broken `background` package is excluded on purpose).
# ---------------------------------------------------------------------------
_ID_NAME_RE = re.compile(r"(?i)(id$|_id$|_ids$|entity_from|entity_to)")
# Legitimately-integer ID (QualityCheckResult.check_id is an Integer PK by design).
_WHITELIST = {"check_id"}


def _iter_app_sources():
    for root, dirs, files in os.walk(APP_DIR):
        # skip the pre-existing broken package + caches
        rel = os.path.relpath(root, REPO_ROOT)
        if rel.startswith("app/infrastructure/background"):
            continue
        if "__pycache__" in dirs:
            dirs.remove("__pycache__")
        for fn in files:
            if fn.endswith(".py"):
                yield os.path.join(root, fn)


def _int_id_violations():
    """Return list of (file, line_no, text) where an int ID annotation slipped in."""
    violations = []
    for path in _iter_app_sources():
        with open(path) as fh:
            for i, line in enumerate(fh, 1):
                # Only scan annotated declarations, not comments / strings.
                m = re.search(r"(\w+)\s*:\s*(Optional\[int\]|List\[int\]|int)\b", line)
                if not m:
                    continue
                name = m.group(1)
                if name in _WHITELIST:
                    continue
                if _ID_NAME_RE.search(name):
                    violations.append((os.path.relpath(path, REPO_ROOT), i, line.strip()))
    return violations


def test_no_int_id_annotations():
    violations = _int_id_violations()
    assert not violations, (
        "Found integer-typed ID annotations (should be UUID):\n"
        + "\n".join(f"  {f}:{ln}  {t}" for f, ln, t in violations)
    )


# ---------------------------------------------------------------------------
# 2b) No str-typed ID *parameters* in the service / route layers. DB PKs are
#     UUID (BaseModel.id = uuid4), so an `alert_id: str` / `rule_id: str`
#     parameter is an inconsistency. Scoped to services + http routes only.
#     `etl.py` is excluded: it uses a legacy, pre-existing-broken repo path
#     (ETLResult is unimportable) that is out of scope for the UUID work.
# ---------------------------------------------------------------------------
_STR_ID_DIRS = ("app/application/services", "app/interfaces/http/routes")
_STR_ID_EXCLUDE_FILES = {"app/interfaces/http/routes/etl.py"}


def test_no_str_id_params_in_services_and_routes():
    bad = []
    for path in _iter_app_sources():
        rel = os.path.relpath(path, REPO_ROOT)
        if not rel.startswith(_STR_ID_DIRS):
            continue
        if rel in _STR_ID_EXCLUDE_FILES:
            continue
        with open(path) as fh:
            for i, line in enumerate(fh, 1):
                m = re.search(r"(\w+)\s*:\s*str\b", line)
                if not m:
                    continue
                name = m.group(1)
                if name in _WHITELIST:
                    continue
                if _ID_NAME_RE.search(name):
                    bad.append((rel, i, line.strip()))
    assert not bad, (
        "Found str-typed ID parameters (should be UUID):\n"
        + "\n".join(f"  {f}:{ln}  {t}" for f, ln, t in bad)
    )


# ---------------------------------------------------------------------------
# 3) Phantom `.execution_id` access on ORM instances is gone.
#    JobExecution / EtlJob have PK `id`, never `.execution_id`.
# ---------------------------------------------------------------------------
_PHANTOM_RE = re.compile(
    r"\.(latest_execution|execution|job_execution|job)\.execution_id\b"
)


def test_no_phantom_execution_id_access():
    bad = []
    for path in _iter_app_sources():
        with open(path) as fh:
            for i, line in enumerate(fh, 1):
                if _PHANTOM_RE.search(line):
                    bad.append((os.path.relpath(path, REPO_ROOT), i, line.strip()))
    assert not bad, (
        "Found phantom `.execution_id` access on ORM instances (use `.id`):\n"
        + "\n".join(f"  {f}:{ln}  {t}" for f, ln, t in bad)
    )


# ---------------------------------------------------------------------------
# 4) The corrected reads DO use `.id` (positive guard so the fix can't vanish).
# ---------------------------------------------------------------------------
def test_corrected_id_reads_present():
    need = [
        "app/application/services/etl_service.py",
        "app/application/services/monitoring_service.py",
        "app/application/services/data_quality_service.py",
        "app/application/services/job_orchestration_service.py",
    ]
    found = False
    for rel in need:
        path = os.path.join(REPO_ROOT, rel)
        with open(path) as fh:
            for line in fh:
                if re.search(r"\b(execution|latest_execution|job_execution|job)\.id\b", line):
                    found = True
                    break
        if found:
            break
    assert found, "Expected at least one corrected `.id` read on JobExecution/EtlJob instances."


# ---------------------------------------------------------------------------
# 5) Model PKs are actually UUID (not just annotations on schemas).
# ---------------------------------------------------------------------------
def test_job_execution_pk_is_uuid():
    from app.infrastructure.db.models.etl_control.job_executions import JobExecution
    from app.infrastructure.db.models.etl_control.etl_jobs import EtlJob

    for model in (JobExecution, EtlJob):
        field = model.model_fields["id"]
        ann = getattr(field, "annotation", None)
        assert ann is uuid.UUID, f"{model.__name__}.id annotation is {ann!r}, expected UUID"
