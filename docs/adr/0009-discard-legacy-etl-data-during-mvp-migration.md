# Discard legacy ETL data during the MVP migration

Legacy ETL job, dependency, quality, entity, execution, and development data will not be translated into the new Dataset/Recipe/Run model because the existing semantics are ambiguous and the environment data is disposable. New SQLModel models are defined first, Alembic generates the schema migration, and legacy tables are dropped only after the replacement schema exists; Upload API and User Management integration configuration remains in scope.
