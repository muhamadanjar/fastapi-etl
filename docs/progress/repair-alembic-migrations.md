Related Plan: [Repair Alembic Migrations Plan](../plans/repair-alembic-migrations.md)

# Repair Alembic Migrations Progress

## Status

Complete. Compatibility hardening now supports databases initialized by the legacy Alembic chain, current SQLModel metadata, or a mixture of both layouts.

## Checklist

- [x] Inspect the Alembic revision graph and confirm a single head.
- [x] Reproduce the current chain against an empty PostgreSQL database.
- [x] Add idempotent schema bootstrap to the initial revision.
- [x] Replace the overlong `0006` revision identifier.
- [x] Make downgrade/re-upgrade safe for PostgreSQL enum types.
- [x] Repair schema-aware autogenerate/check behavior and sequential revision assignment.
- [x] Add `0007_align_model_columns` for missing model columns, preserved metadata rename, and index alignment.
- [x] Align SQLModel storage types and foreign-key actions with the existing database contract.
- [x] Add automated migration-chain regression tests.
- [x] Verify upgrade/check/downgrade/re-upgrade on disposable PostgreSQL.
- [x] Verify `metadata` JSONB survives both the `0007` upgrade and downgrade.
- [x] Write final migration documentation.
- [x] Refresh knowledge graphs.
- [x] Make revisions `0006` and `0007` tolerant of compatible pre-existing columns and indexes.
- [x] Test `0007` from legacy-only, model-only, and mixed metadata column states.
- [x] Repeat PostgreSQL upgrade/check/downgrade verification after compatibility hardening.

## Verification

- `pytest -q tests/test_migration_chain.py`: 4 passed.
- `python -m compileall -q app migrations tests`: passed.
- Fresh PostgreSQL 16 `alembic upgrade head`: passed.
- `alembic check` before and after round-trip: no new upgrade operations detected.
- `alembic downgrade base` followed by `alembic upgrade head`: passed.
- Current revision: `0007_align_model_columns (head)`.
- Existing JSONB preservation check returned `legacy` before and after downgrade.
- Legacy-only metadata scenario: upgraded with no drift and retained `{"legacy": true}`.
- Model-only metadata scenario: upgraded with no drift and retained `{"model": true}`.
- Mixed metadata scenario: merged both documents, retained both keys, and preferred `source_metadata` for the duplicate `shared` key.
- Pre-existing artifact columns/index scenario: revisions `0006` and `0007` upgraded successfully with no drift.
- `graphify update .`: completed with 5,458 nodes and 11,742 edges; HTML uses the aggregated community view because the graph exceeds 5,000 nodes.
- codebase-memory fast index: completed with 3,753 nodes and 19,670 edges.
