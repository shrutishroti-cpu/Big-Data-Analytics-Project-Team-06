# Firehose Configuration with Kinesis Data Stream and Lambda

## Overview
This explains how we configured Amazon Kinesis Firehose to deliver streaming clickstream data from a Kinesis Data Stream to an S3 bucket, using a Lambda function to generate synthetic clickstream events.

---

## 1. Created Kinesis Data Stream
- Navigated to **AWS Management Console** → **Kinesis** → **Data Streams**.
- Clicked **Create Data Stream**.
- Enter:
  - **Name:** `data-clicks-06`
  - **Shard count:** 1 (for demo purposes)
- Clicked **Create Data Stream**.

---

## 2. Created Lambda Function for Clickstream Data
-namely--> handler.py

- Attach IAM Role with:
  - `AmazonKinesisFullAccess`
  - `AWSLambdaBasicExecutionRole`
- Deploy the function.

---

## 3. Created Firehose Delivery Stream
- **Amazon Kinesis** → **Firehose** → **Create Delivery Stream**.
- Choose:
  - **Source:** Kinesis Data Stream
  - **Stream name:** `clickstream-data-stream`
- Destination:
  - **Amazon S3**
  - **Bucket:** `project-raw-bucket/clickstream/YYYY-MM-DD/`
- Enable **Dynamic Partitioning**:
  - Partition key: `date=!{timestamp:yyyy-MM-dd}`
- Buffer settings:
  - Size: 5 MB
  - Interval: 60 seconds
- Click **Create Delivery Stream**.

---

## 4. Test the Setup
- Invoked Lambda manually or set a CloudWatch Event trigger.
- Checked Kinesis Data Stream for incoming records.
- Verified Firehose writes JSON files to S3 in date-partitioned folders.

---

## Line Diagram
Lambda → Kinesis Data Stream → Firehose → S3 (partitioned by date)
