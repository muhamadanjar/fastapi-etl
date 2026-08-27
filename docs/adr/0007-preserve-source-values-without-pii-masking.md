# Preserve source values without PII masking

The ETL MVP preserves and exposes source values as received rather than detecting or masking PII automatically. Access to metadata, record previews, and downloads is authorized through separate User Management permissions; ETL never writes record contents or credentials to application logs and never includes credentials in responses or error details.
