import os
from uuid import uuid4

import pytest
from sqlmodel import Session, create_engine, select

from app.application.services.dataset_engine import infer_schema, json_size
from app.application.services.run_executor import execute_transformation
from app.application.services.workspace_service import WorkspaceService
from app.infrastructure.db.models.workspace import (
    DataSource,
    Dataset,
    DatasetKind,
    DatasetRecord,
    RecipeOperation,
    Run,
    RunStatus,
    SelectorKind,
    SourceKind,
)
from app.schemas.workspace import (
    DataRecipeCreate,
    DataSourceCreate,
    RecipeRunCreate,
    SelectorInput,
)


DATABASE_URL = os.getenv("ETL_INTEGRATION_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    not DATABASE_URL,
    reason="ETL_INTEGRATION_DATABASE_URL is not configured",
)


def _raw_dataset(session: Session, source_id, name: str, records: list[dict]) -> Dataset:
    dataset = Dataset(
        name=name,
        kind=DatasetKind.RAW,
        dataset_schema=infer_schema(records),
        record_count=len(records),
        size_bytes=sum(json_size(record) for record in records),
        source_id=source_id,
        created_by_actor_id="integration-test",
    )
    session.add(dataset)
    session.flush()
    for position, payload in enumerate(records):
        session.add(DatasetRecord(dataset_id=dataset.id, position=position, payload=payload))
    session.commit()
    session.refresh(dataset)
    return dataset


def test_recipe_run_creates_immutable_derived_dataset():
    engine = create_engine(DATABASE_URL)
    suffix = uuid4().hex[:10]
    with Session(engine) as session:
        service = WorkspaceService(session)
        customers = service.create_source(
            DataSourceCreate(name=f"customers-{suffix}", kind=SourceKind.FILE),
            "integration-test",
        )
        orders = service.create_source(
            DataSourceCreate(name=f"orders-{suffix}", kind=SourceKind.FILE),
            "integration-test",
        )
        customer_data = _raw_dataset(
            session,
            customers.id,
            f"customers-raw-{suffix}",
            [{"customer_id": 1, "name": "A"}, {"customer_id": 2, "name": "B"}],
        )
        order_data = _raw_dataset(
            session,
            orders.id,
            f"orders-raw-{suffix}",
            [{"customer_id": 1, "amount": 10}, {"customer_id": 1, "amount": 20}],
        )
        recipe = service.create_recipe(
            DataRecipeCreate(
                name=f"customer-orders-{suffix}",
                operation=RecipeOperation.JOIN,
                inputs=[
                    SelectorInput(
                        alias="customers",
                        selector_kind=SelectorKind.SOURCE,
                        source_id=customers.id,
                    ),
                    SelectorInput(
                        alias="orders",
                        selector_kind=SelectorKind.SOURCE,
                        source_id=orders.id,
                    ),
                ],
                definition={
                    "join_type": "LEFT",
                    "left_keys": ["customer_id"],
                    "right_keys": ["customer_id"],
                    "mappings": [
                        {
                            "source_alias": "customers",
                            "source_column": "name",
                            "target_column": "customer_name",
                        },
                        {
                            "source_alias": "orders",
                            "source_column": "amount",
                            "target_column": "amount",
                        },
                    ],
                },
            ),
            "integration-test",
        )
        run = service.create_transformation_run(
            recipe.id, RecipeRunCreate(), "integration-test"
        )

        output_id = execute_transformation(session, run.id)

        output = session.get(Dataset, output_id)
        completed_run = session.get(Run, run.id)
        rows = session.exec(
            select(DatasetRecord)
            .where(DatasetRecord.dataset_id == output_id)
            .order_by(DatasetRecord.position)
        ).all()
        assert output.kind == DatasetKind.DERIVED
        assert output.record_count == 3
        assert [row.payload for row in rows] == [
            {"customer_name": "A", "amount": 10},
            {"customer_name": "A", "amount": 20},
            {"customer_name": "B", "amount": None},
        ]
        assert completed_run.status == RunStatus.SUCCEEDED
        assert completed_run.records_processed == 3
        assert service.dataset_lineage(output.id)["inputs"] == [
            {"alias": "customers", "dataset_id": str(customer_data.id)},
            {"alias": "orders", "dataset_id": str(order_data.id)},
        ]
