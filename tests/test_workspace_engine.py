from uuid import uuid4

import pytest
from pydantic import ValidationError

from app.application.services.dataset_engine import (
    DatasetLimitExceeded,
    SchemaDriftError,
    bounded_records,
    infer_schema,
    join_records,
    union_records,
)
from app.schemas.workspace import DataRecipeCreate, DataSourceCreate


def test_file_source_rejects_api_credentials():
    with pytest.raises(ValidationError):
        DataSourceCreate.model_validate(
            {
                "name": "monthly-sales",
                "kind": "FILE",
                "config": {},
                "credential": {
                    "credential_type": "BEARER",
                    "secret": "hidden",
                },
            }
        )


def test_api_source_rejects_credentials_in_public_config():
    with pytest.raises(ValidationError, match="Authentication headers"):
        DataSourceCreate.model_validate(
            {
                "name": "unsafe-header",
                "kind": "API",
                "config": {
                    "url": "https://example.test/data",
                    "headers": {"Authorization": "Bearer secret"},
                },
            }
        )

    with pytest.raises(ValidationError, match="Authentication query parameters"):
        DataSourceCreate.model_validate(
            {
                "name": "unsafe-query",
                "kind": "API",
                "config": {
                    "url": "https://example.test/data",
                    "query": {"token": "secret"},
                },
            }
        )


def test_join_requires_exactly_two_recipe_inputs():
    with pytest.raises(ValidationError):
        DataRecipeCreate.model_validate(
            {
                "name": "bad-join",
                "operation": "JOIN",
                "inputs": [
                    {
                        "alias": "one",
                        "selector_kind": "DATASET",
                        "dataset_id": uuid4(),
                    }
                ],
                "definition": {
                    "join_type": "INNER",
                    "left_keys": ["id"],
                    "right_keys": ["id"],
                },
            }
        )


def test_union_mapping_requires_same_targets_for_every_alias():
    with pytest.raises(ValidationError, match="identical target columns"):
        DataRecipeCreate.model_validate(
            {
                "name": "bad-union",
                "operation": "UNION",
                "inputs": [
                    {"alias": "a", "selector_kind": "DATASET", "dataset_id": uuid4()},
                    {"alias": "b", "selector_kind": "DATASET", "dataset_id": uuid4()},
                ],
                "definition": {
                    "mappings": [
                        {
                            "source_alias": "a",
                            "source_column": "id",
                            "target_column": "id",
                        }
                    ]
                },
            }
        )


def test_left_join_uses_sql_null_semantics_and_expands_duplicates():
    rows, _ = join_records(
        [
            (
                "left",
                [
                    {"id": 1, "name": "A"},
                    {"id": 1, "name": "B"},
                    {"id": None, "name": "No key"},
                ],
            ),
            ("right", [{"id": 1, "value": 10}, {"id": 1, "value": 20}, {"id": None, "value": 99}]),
        ],
        {
            "join_type": "LEFT",
            "left_keys": ["id"],
            "right_keys": ["id"],
            "mappings": [
                {"source_alias": "left", "source_column": "name", "target_column": "name"},
                {"source_alias": "right", "source_column": "value", "target_column": "value"},
            ],
        },
    )

    assert rows == [
        {"name": "A", "value": 10},
        {"name": "A", "value": 20},
        {"name": "B", "value": 10},
        {"name": "B", "value": 20},
        {"name": "No key", "value": None},
    ]


def test_union_is_strict_after_mapping():
    rows, _ = union_records(
        [("a", [{"id": 1}]), ("b", [{"external_id": 2}])],
        {
            "mappings": [
                {"source_alias": "a", "source_column": "id", "target_column": "id", "cast": "NUMBER"},
                {"source_alias": "b", "source_column": "external_id", "target_column": "id", "cast": "NUMBER"},
            ]
        },
    )
    assert rows == [{"id": 1}, {"id": 2}]


def test_union_rejects_different_schemas_without_mapping():
    with pytest.raises(SchemaDriftError):
        union_records(
            [("a", [{"id": 1}]), ("b", [{"external_id": 2}])],
            {},
        )


def test_dataset_limits_fail_before_partial_result_is_returned():
    with pytest.raises(DatasetLimitExceeded):
        bounded_records([{"id": 1}, {"id": 2}], max_records=1)


def test_schema_inference_preserves_order_and_numeric_compatibility():
    assert infer_schema([{"id": 1, "value": None}, {"id": 2, "value": 1.5}]) == {
        "columns": [
            {"name": "id", "type": "INTEGER", "nullable": False},
            {"name": "value", "type": "NUMBER", "nullable": True},
        ]
    }
