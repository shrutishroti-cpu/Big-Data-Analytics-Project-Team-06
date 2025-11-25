# AWS Glue ETL Job with Secrets Manager Integration

## Overview
This document explains how AWS Secrets Manager was configured and used in the Glue ETL job to securely fetch RDS credentials and write processed data from S3 to RDS.

---

## Why Use Secrets Manager?
- Avoid hardcoding sensitive credentials in ETL scripts.
- Enable secure storage, encryption, and rotation of secrets.
- Centralized management of database credentials.

---

## Steps Implemented in Glue ETL Job

### 1. Import Required Libraries
```python
import boto3
import json
```
- `boto3`: AWS SDK for Python to interact with Secrets Manager.
- `json`: Parse the secret string returned by Secrets Manager.

---

### 2. Define Function to Fetch Secret
```python
def get_rds_credentials(secret_name, region_name="us-east-1"):
    client = boto3.client('secretsmanager', region_name=region_name)
    try:
        secret_value = client.get_secret_value(SecretId=secret_name)
        if 'SecretString' in secret_value:
            secret = json.loads(secret_value['SecretString'])
        else:
            secret = json.loads(secret_value['SecretBinary'].decode('utf-8'))
        return secret
    except Exception as e:
        print(f"Error fetching RDS credentials: {e}")
        raise e
```
- Creates a Secrets Manager client.
- Fetches the secret using `get_secret_value`.
- Parses the secret into a Python dictionary.

---

### 3. Retrieve Secret
```python
secret_name = "s3-to-rds-job-secret"
secret = get_rds_credentials(secret_name)
```
- `secret_name`: The name of the secret stored in Secrets Manager.
- `secret`: Dictionary containing keys like `rds_url`, `username`, `password`.

---

### 4. Extract Credentials
```python
rds_url = secret["rds_url"]
rds_user = secret["username"]
rds_password = secret["password"]
driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
```
- These values are used in the JDBC connection for writing data to RDS.

---

### 5. Use Credentials in Glue Write Operation
```python
transaction_df.write     .format("jdbc")     .option("url", rds_url)     .option("dbtable", "transaction_table")     .option("user", rds_user)     .option("password", rds_password)     .option("driver", driver)     .mode("overwrite")     .save()
```
- Securely connects to RDS without exposing credentials in the script.

---

## Architecture Diagram Description
Glue Job → Secrets Manager → RDS
- Glue fetches credentials securely from Secrets Manager before writing to RDS.

**Visual Representation:**
```
[Glue ETL Job] ---> [AWS Secrets Manager] ---> [Amazon RDS]
```

---

## Key Benefits
- Enhanced security for sensitive credentials.
- Compliance with best practices for cloud security.
- Simplified credential management for multiple environments.
