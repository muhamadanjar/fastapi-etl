from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any, Dict, List
from uuid import UUID

from sqlmodel import Session, select

from app.application.services.credential_cipher import CredentialCipher
from app.application.services.dataset_engine import (
    infer_schema,
    join_records,
    union_records,
)
from app.application.services.ingestion_reader import read_api_records, read_file_records
from app.infrastructure.db.models.workspace import (
    CredentialType,
    DataSourceCredential,
    Dataset,
    DatasetKind,
    DatasetRecord,
    RecipeOperation,
    Run,
    RunInput,
    RunKind,
    RunStatus,
    SourceKind,
)
from app.infrastructure.upload_artifact_client import UploadArtifactClient
from app.schemas.workspace import CredentialWrite


class RunExecutionError(RuntimeError):
    code = "RUN_EXECUTION_FAILED"


def _claim(session: Session, run_id: UUID, expected_kind: RunKind) -> Run | None:
    run = session.exec(
        select(Run).where(Run.id == run_id).with_for_update()
    ).first()
    if not run:
        raise RunExecutionError("Run not found")
    if run.kind != expected_kind:
        raise RunExecutionError(f"Run is not a {expected_kind.value} Run")
    if run.status != RunStatus.PENDING:
        return None
    run.status = RunStatus.RUNNING
    run.started_at = datetime.utcnow()
    session.add(run)
    session.commit()
    session.refresh(run)
    return run


def _credential_for_run(session: Session, run: Run) -> CredentialWrite | None:
    if not run.source_id or not run.credential_version:
        return None
    row = session.exec(
        select(DataSourceCredential).where(
            DataSourceCredential.source_id == run.source_id,
            DataSourceCredential.version == run.credential_version,
        )
    ).first()
    if not row or row.credential_type == CredentialType.NONE:
        return None
    if not row.encrypted_secret:
        raise RunExecutionError("Source Credential is incomplete")
    return CredentialWrite(
        credential_type=row.credential_type,
        secret=CredentialCipher().decrypt(row.encrypted_secret),
        header_name=row.header_name,
    )


def _store_dataset(
    session: Session,
    *,
    run: Run,
    name: str,
    kind: DatasetKind,
    records: List[Dict[str, Any]],
    size_bytes: int,
) -> Dataset:
    dataset = Dataset(
        name=name,
        kind=kind,
        dataset_schema=infer_schema(records),
        record_count=len(records),
        size_bytes=size_bytes,
        source_id=run.source_id if kind == DatasetKind.RAW else None,
        produced_by_run_id=run.id,
        created_by_actor_id=run.actor_id,
    )
    session.add(dataset)
    session.flush()
    for position, payload in enumerate(records):
        session.add(
            DatasetRecord(dataset_id=dataset.id, position=position, payload=payload)
        )
    run.status = RunStatus.SUCCEEDED
    run.records_processed = len(records)
    run.output_size_bytes = size_bytes
    run.completed_at = datetime.utcnow()
    session.add(run)
    session.commit()
    session.refresh(dataset)
    return dataset


def _fail(session: Session, run_id: UUID, exc: Exception) -> None:
    session.rollback()
    run = session.get(Run, run_id)
    if not run or run.status == RunStatus.SUCCEEDED:
        return
    run.status = RunStatus.FAILED
    run.completed_at = datetime.utcnow()
    run.error_code = getattr(exc, "code", "RUN_EXECUTION_FAILED")
    run.error_details = {"message": str(exc)[:1000]}
    session.add(run)
    session.commit()


def execute_ingestion(session: Session, run_id: UUID) -> UUID | None:
    run = _claim(session, run_id, RunKind.INGESTION)
    if run is None:
        return None
    try:
        snapshot = run.definition_snapshot
        source_name = str(snapshot["name"])
        source_kind = SourceKind(snapshot["kind"])
        if source_kind == SourceKind.FILE:
            if not run.artifact_id:
                raise RunExecutionError("File Run has no artifact_id")
            client = UploadArtifactClient()
            artifact = asyncio.run(client.metadata(run.artifact_id))
            filename = str(artifact["filename"])
            with client.materialize(run.artifact_id, filename) as path:
                records, size_bytes = read_file_records(path)
        else:
            credential = _credential_for_run(session, run)
            records, size_bytes = read_api_records(snapshot["config"], credential)
        dataset = _store_dataset(
            session,
            run=run,
            name=f"{source_name} / {run.created_at:%Y-%m-%d %H:%M:%S}",
            kind=DatasetKind.RAW,
            records=records,
            size_bytes=size_bytes,
        )
        return dataset.id
    except Exception as exc:
        _fail(session, run_id, exc)
        raise


def _load_inputs(session: Session, run_id: UUID) -> List[tuple[str, List[Dict[str, Any]]]]:
    inputs = session.exec(
        select(RunInput).where(RunInput.run_id == run_id).order_by(RunInput.position)
    ).all()
    result = []
    for item in inputs:
        rows = session.exec(
            select(DatasetRecord)
            .where(DatasetRecord.dataset_id == item.dataset_id)
            .order_by(DatasetRecord.position)
        ).all()
        result.append((item.alias, [row.payload for row in rows]))
    return result


def execute_transformation(session: Session, run_id: UUID) -> UUID | None:
    run = _claim(session, run_id, RunKind.TRANSFORMATION)
    if run is None:
        return None
    try:
        inputs = _load_inputs(session, run.id)
        operation = RecipeOperation(run.definition_snapshot["operation"])
        definition = run.definition_snapshot["definition"]
        if operation == RecipeOperation.JOIN:
            records, size_bytes = join_records(inputs, definition)
        else:
            records, size_bytes = union_records(inputs, definition)
        dataset = _store_dataset(
            session,
            run=run,
            name=f"{run.definition_snapshot['name']} / {run.created_at:%Y-%m-%d %H:%M:%S}",
            kind=DatasetKind.DERIVED,
            records=records,
            size_bytes=size_bytes,
        )
        return dataset.id
    except Exception as exc:
        _fail(session, run_id, exc)
        raise
