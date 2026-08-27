# ETL Online Permission Authorization Plan

Related Progress: [ETL Online Permission Authorization Progress](../progress/online-permission-authorization.md)

ETL delegates every protected request to User Management `/auth/authorize` with no positive authorization cache. Read requests require `etl.read`, mutation requests require `etl.manage`, and execution paths require `etl.jobs.execute`. User Management failures map to `503`; denied permissions map to `403`.
