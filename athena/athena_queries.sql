-- ============================================
-- Sample Query: Fetch Clickstream Data for a Specific Date
-- ============================================
SELECT *
FROM projectdb.clickstreams
WHERE processed_time_year = '2025'
  AND processed_time_month = '11'
  AND processed_time_day = '19';

SELECT *
FROM projectdb.transactions
WHERE processed_time_year = '2025'
  AND processed_time_month = '11'
  AND processed_time_day = '19';
s

SELECT *
FROM projectdb.users
WHERE processed_time_year = '2025'
  AND processed_time_month = '11'
  AND processed_time_day = '19';


