# Limit the MVP product surface

The ETL MVP exposes only Overview, Sources, Datasets, Recipes, and Runs. Jobs, Entities, Dependencies, Data Quality, Rejected Records, standalone Metrics, Alerts, Notifications, and System Configuration are removed because they do not directly support collecting immutable source snapshots and composing them into new datasets.

## Consequences

Operational counts and failures appear in Overview or Runs rather than separate subsystems. Reintroducing an excluded area requires a demonstrated product use case rather than preserving the existing implementation by default.
