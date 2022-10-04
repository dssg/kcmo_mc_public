/*
SQL query to create labels
Checks if a person has had a new case within 1 year from disposition
*/

WITH cohort AS (
SELECT
    person_id,
    cohort_date
FROM
    {schema_name}.{cohort_table_name}
),
new_cases AS(
SELECT
   cohort.person_id as person_id,
   cohort_date,
   count(v.case_num) AS num_cases
FROM
    cohort
LEFT JOIN clean.violations v
    ON cohort.person_id = v.person_id
    AND cohort_date::date < v.viol_dttm::date AND v.viol_dttm::date <= cohort.cohort_date::date + INTERVAL '{label_window}'
    GROUP BY cohort.person_id, cohort_date
 )
 SELECT
    person_id,
    cohort_date,
    CASE WHEN num_cases > 0 THEN 1 ELSE 0 END AS label
FROM new_cases
ORDER BY cohort_date, person_id ASC
     





