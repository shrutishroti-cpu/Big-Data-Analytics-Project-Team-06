--CLICKSTREAM DATA EXAMPLE

SELECT event_id, user_id, page, action, ts

FROM clickstreams

ORDER BY ts DESC;

--Peak Hour User Activity

SELECT DATEPART(HOUR, ts) AS activity_hour,

COUNT(*) AS event_count

FROM clickstreams

GROUP BY DATEPART(HOUR, ts)

ORDER BY event_count DES

--Join Users and Transactions for Country-wise Revenue

SELECT u.country, SUM(t.total) AS revenue

FROM transactions t

JOIN users u ON t.user_id = u.user_id

GROUP BY u.country

ORDER BY revenue DESC;