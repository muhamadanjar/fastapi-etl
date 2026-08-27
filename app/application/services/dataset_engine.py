from __future__ import annotations

import json
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Sequence

from app.schemas.workspace import (
    CastType,
    ColumnMappingSpec,
    DatasetFilterSpec,
    FilterOperator,
    JoinDefinition,
    UnionDefinition,
)


MAX_DATASET_RECORDS = 100_000
MAX_DATASET_BYTES = 100 * 1024 * 1024


class DatasetEngineError(ValueError):
    code = "TRANSFORMATION_ERROR"


class DatasetLimitExceeded(DatasetEngineError):
    code = "DATASET_LIMIT_EXCEEDED"


class SchemaDriftError(DatasetEngineError):
    code = "SCHEMA_DRIFT"


def json_size(record: Mapping[str, Any]) -> int:
    return len(
        json.dumps(record, ensure_ascii=False, separators=(",", ":"), default=str).encode(
            "utf-8"
        )
    )


def bounded_records(
    records: Iterable[Dict[str, Any]],
    *,
    max_records: int = MAX_DATASET_RECORDS,
    max_bytes: int = MAX_DATASET_BYTES,
) -> tuple[List[Dict[str, Any]], int]:
    materialized: List[Dict[str, Any]] = []
    total_bytes = 0
    for record in records:
        if len(materialized) >= max_records:
            raise DatasetLimitExceeded(f"Dataset exceeds {max_records} records")
        total_bytes += json_size(record)
        if total_bytes > max_bytes:
            raise DatasetLimitExceeded(f"Dataset exceeds {max_bytes} parsed bytes")
        materialized.append(record)
    return materialized, total_bytes


def _value_type(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int) and not isinstance(value, bool):
        return "INTEGER"
    if isinstance(value, (float, Decimal)):
        return "NUMBER"
    if isinstance(value, (datetime, date)):
        return "DATE"
    if isinstance(value, dict):
        return "OBJECT"
    if isinstance(value, list):
        return "ARRAY"
    return "STRING"


def infer_schema(records: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    order: List[str] = []
    types: Dict[str, set[str]] = defaultdict(set)
    for record in records:
        for name, value in record.items():
            if name not in types:
                order.append(name)
            types[name].add(_value_type(value))

    columns = []
    for name in order:
        observed = types[name]
        nullable = "NULL" in observed or any(name not in row for row in records)
        concrete = observed - {"NULL"}
        if not concrete:
            column_type = "STRING"
        elif concrete <= {"INTEGER", "NUMBER"}:
            column_type = "NUMBER" if "NUMBER" in concrete else "INTEGER"
        elif len(concrete) == 1:
            column_type = next(iter(concrete))
        else:
            column_type = "MIXED"
        columns.append({"name": name, "type": column_type, "nullable": nullable})
    return {"columns": columns}


def schema_columns(schema: Mapping[str, Any]) -> Dict[str, str]:
    return {
        str(column["name"]): str(column["type"])
        for column in schema.get("columns", [])
        if isinstance(column, Mapping) and "name" in column and "type" in column
    }


def _cast(value: Any, target: CastType | None) -> Any:
    if target is None or value is None:
        return value
    try:
        if target == CastType.STRING:
            return str(value)
        if target == CastType.NUMBER:
            number = Decimal(str(value))
            return int(number) if number == number.to_integral_value() else float(number)
        if target == CastType.BOOLEAN:
            if isinstance(value, bool):
                return value
            normalized = str(value).strip().lower()
            if normalized in {"true", "1", "yes", "y"}:
                return True
            if normalized in {"false", "0", "no", "n"}:
                return False
            raise ValueError(f"Cannot cast {value!r} to boolean")
        if isinstance(value, (date, datetime)):
            return value.isoformat()
        return datetime.fromisoformat(str(value)).isoformat()
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise DatasetEngineError(
            f"Cannot cast value {value!r} to {target.value}"
        ) from exc


def _mapped_record(
    source_records: Mapping[str, Mapping[str, Any]],
    mappings: Sequence[ColumnMappingSpec],
) -> Dict[str, Any]:
    output: Dict[str, Any] = {}
    for mapping in mappings:
        source = source_records.get(mapping.source_alias)
        if source is None:
            raise SchemaDriftError(f"Unknown input alias: {mapping.source_alias}")
        if mapping.source_column not in source:
            raise SchemaDriftError(
                f"Column {mapping.source_alias}.{mapping.source_column} is missing"
            )
        if mapping.target_column in output:
            raise DatasetEngineError(
                f"Duplicate output column mapping: {mapping.target_column}"
            )
        output[mapping.target_column] = _cast(
            source[mapping.source_column], mapping.cast
        )
    return output


def _default_join_record(
    left_alias: str,
    left: Mapping[str, Any],
    right_alias: str,
    right: Mapping[str, Any] | None,
) -> Dict[str, Any]:
    result = dict(left)
    if right is None:
        return result
    for name, value in right.items():
        target = name if name not in result else f"{right_alias}.{name}"
        result[target] = value
    return result


def _matches_filter(record: Mapping[str, Any], rule: DatasetFilterSpec) -> bool:
    value = record.get(rule.column)
    if rule.operator == FilterOperator.IS_EMPTY:
        return value is None or value == ""
    if rule.operator == FilterOperator.IS_NOT_EMPTY:
        return value is not None and value != ""
    if rule.operator == FilterOperator.EQ:
        return value == rule.value
    if rule.operator == FilterOperator.NE:
        return value != rule.value
    if rule.operator == FilterOperator.CONTAINS:
        return value is not None and str(rule.value) in str(value)
    if value is None:
        return False
    try:
        if rule.operator == FilterOperator.GT:
            return value > rule.value
        if rule.operator == FilterOperator.GTE:
            return value >= rule.value
        if rule.operator == FilterOperator.LT:
            return value < rule.value
        if rule.operator == FilterOperator.LTE:
            return value <= rule.value
    except TypeError as exc:
        raise DatasetEngineError(
            f"Filter type mismatch for column {rule.column}"
        ) from exc
    raise DatasetEngineError(f"Unsupported filter operator: {rule.operator}")


def _filtered(
    records: Iterable[Dict[str, Any]], filters: Sequence[DatasetFilterSpec]
) -> Iterator[Dict[str, Any]]:
    for record in records:
        if all(_matches_filter(record, rule) for rule in filters):
            yield record


def join_records(
    inputs: Sequence[tuple[str, Sequence[Dict[str, Any]]]],
    definition_data: Mapping[str, Any],
    *,
    max_records: int = MAX_DATASET_RECORDS,
    max_bytes: int = MAX_DATASET_BYTES,
) -> tuple[List[Dict[str, Any]], int]:
    if len(inputs) != 2:
        raise DatasetEngineError("JOIN requires exactly two inputs")
    definition = JoinDefinition.model_validate(definition_data)
    (left_alias, left_records), (right_alias, right_records) = inputs

    right_index: Dict[tuple[Any, ...], List[Dict[str, Any]]] = defaultdict(list)
    for record in right_records:
        key = tuple(record.get(name) for name in definition.right_keys)
        if any(value is None for value in key):
            continue
        right_index[key].append(record)

    def generated() -> Iterator[Dict[str, Any]]:
        for left in left_records:
            key = tuple(left.get(name) for name in definition.left_keys)
            matches = [] if any(value is None for value in key) else right_index.get(key, [])
            if matches:
                for right in matches:
                    if definition.mappings:
                        yield _mapped_record(
                            {left_alias: left, right_alias: right}, definition.mappings
                        )
                    else:
                        yield _default_join_record(left_alias, left, right_alias, right)
            elif definition.join_type == "LEFT":
                if definition.mappings:
                    right_nulls = {
                        mapping.source_column: None
                        for mapping in definition.mappings
                        if mapping.source_alias == right_alias
                    }
                    yield _mapped_record(
                        {left_alias: left, right_alias: right_nulls}, definition.mappings
                    )
                else:
                    yield _default_join_record(left_alias, left, right_alias, None)

    return bounded_records(
        _filtered(generated(), definition.filters),
        max_records=max_records,
        max_bytes=max_bytes,
    )


def union_records(
    inputs: Sequence[tuple[str, Sequence[Dict[str, Any]]]],
    definition_data: Mapping[str, Any],
    *,
    max_records: int = MAX_DATASET_RECORDS,
    max_bytes: int = MAX_DATASET_BYTES,
) -> tuple[List[Dict[str, Any]], int]:
    if not 2 <= len(inputs) <= 10:
        raise DatasetEngineError("UNION requires 2 to 10 inputs")
    definition = UnionDefinition.model_validate(definition_data)

    def generated() -> Iterator[Dict[str, Any]]:
        expected_columns: List[str] | None = None
        expected_types: Dict[str, str] | None = None
        mappings_by_alias: Dict[str, List[ColumnMappingSpec]] = defaultdict(list)
        for mapping in definition.mappings:
            mappings_by_alias[mapping.source_alias].append(mapping)

        for alias, records in inputs:
            mapped_records = [
                _mapped_record({alias: record}, mappings_by_alias[alias])
                if definition.mappings
                else dict(record)
                for record in records
            ]
            current_schema = infer_schema(mapped_records)
            current_columns = [item["name"] for item in current_schema["columns"]]
            current_types = schema_columns(current_schema)
            if expected_columns is None:
                expected_columns = current_columns
                expected_types = current_types
            elif current_columns != expected_columns or current_types != expected_types:
                raise SchemaDriftError(
                    f"UNION input {alias} schema does not match the first input"
                )
            yield from mapped_records

    return bounded_records(
        _filtered(generated(), definition.filters),
        max_records=max_records,
        max_bytes=max_bytes,
    )
