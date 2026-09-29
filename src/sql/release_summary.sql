CREATE OR REPLACE TABLE IDENTIFIER(:status_table) AS

SELECT
    max(certified_at) AS last_certified_at,
    count(*) AS certified_releases,
    max_by(periods, certified_at) AS last_periods

FROM IDENTIFIER(:release_table);