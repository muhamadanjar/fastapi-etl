# Replace jobs with recipes and runs

The MVP models reusable data-combination configuration as a Data Recipe and records each execution as a Transformation Run. ETL jobs, schedules, dependency graphs, and orchestration rules are excluded because they mix configuration with runtime concerns and are not required for explicit ingestion and dataset composition; background workers remain an implementation detail.

## Consequences

A Data Recipe never carries runtime status or scheduling configuration. It accepts Raw or Derived Datasets; Data Source inputs default to their latest snapshots while allowing an older snapshot to be selected. Every Transformation Run records its exact input snapshot IDs and produces one immutable Derived Dataset. Recipe chains are explicit, reject cycles, and never trigger upstream runs automatically.

Recipes are editable and monotonically versioned. A Transformation Run freezes the complete recipe definition and version used, so later edits affect only future runs.
