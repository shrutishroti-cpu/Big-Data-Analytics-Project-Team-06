-- ============================================
-- Create External Table: Clickstreams
-- Stores user clickstream events in Parquet format
-- Partitioned by processed date for efficient querying
-- ============================================
CREATE EXTERNAL TABLE IF NOT EXISTS projectdb.clickstreams (
    event_id STRING,          -- Unique identifier for the event
    user_id STRING,           -- ID of the user performing the action
    page STRING,              -- Page where the event occurred
    action STRING,            -- Action performed (e.g., click, scroll)
    ts BINARY                 -- Timestamp in binary format
)
PARTITIONED BY (
    processed_time_year STRING,   -- Year of processing
    processed_time_month STRING,  -- Month of processing
    processed_time_day STRING     -- Day of processing
)
STORED AS PARQUET
LOCATION 's3://project-processed-06/clickstream/'  -- S3 path for clickstream data
TBLPROPERTIES ('parquet.compress'='SNAPPY');       -- Compression for optimized storage


-- ============================================
-- Create External Table: Users
-- Stores user profile and signup details
-- ============================================
CREATE EXTERNAL TABLE IF NOT EXISTS projectdb.users (
    user_id STRING,              -- Unique user identifier
    signup_date DATE,            -- Date of signup
    country STRING,              -- Country of the user
    age INT,                     -- Age of the user
    signup_channel STRING        -- Channel used for signup (e.g., web, app)
)
PARTITIONED BY (
    processed_time_year STRING,
    processed_time_month STRING,
    processed_time_day STRING
)
STORED AS PARQUET
LOCATION 's3://project-processed-06/user_sample/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');


-- ============================================
-- Create External Table: Transactions
-- Stores purchase transaction details
-- ============================================
CREATE EXTERNAL TABLE IF NOT EXISTS projectdb.transactions (
    transaction_id STRING,       -- Unique transaction identifier
    user_id STRING,              -- User who made the transaction
    item_id STRING,              -- Purchased item ID
    item_category STRING,        -- Category of the item
    price DOUBLE,                -- Price per item
    quantity INT,                -- Quantity purchased
    total DOUBLE,                -- Total amount (price * quantity)
    currency STRING,             -- Currency used
    payment_type STRING,         -- Payment method (e.g., credit card)
    created_at TIMESTAMP         -- Transaction timestamp
)
PARTITIONED BY (
    processed_time_year STRING,
    processed_time_month STRING,
    processed_time_day STRING
)
STORED AS PARQUET
LOCATION 's3://project-processed-06/transaction_sample/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');