--CREATE SCHEMA
CREATE SCHEMA IF NOT EXISTS STAGING AUTHORIZATION airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA STAGING TO airflow;