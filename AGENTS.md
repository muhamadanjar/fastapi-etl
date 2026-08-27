# Database Schema & Migrations

- For any database schema change—creating a table, adding a column, altering a column, or changing constraints—define or update the SQLModel model first.
- Generate and apply migrations through Alembic; do not hand-create Alembic revision/version files.
- Manually editing or creating a migration revision is allowed only when unavoidable, such as repairing migration ordering, revision naming, or another migration-metadata issue that Alembic cannot resolve safely.
