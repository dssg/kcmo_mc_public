with cohort as (
    SELECT
        person_id,
        cohort_date::date
    FROM {schema_name}.{cohort_table_name} 
),
days_since_disp as(
    SELECT
        c.person_id as person_id,
        cohort_date,
        cohort_date::date - max(disp_date::date) as days_since_last_disp
    FROM cohort c
    LEFT JOIN clean.dispositions t1 
    ON c.person_id = t1.person_id AND (cohort_date > disp_date)
    GROUP BY 1, 2
   ),
   days_since_viol as(
    SELECT
        c.person_id as person_id,
        cohort_date,
        cohort_date::date - max(viol_dttm::date) as days_since_last_viol 
    FROM cohort c
    LEFT JOIN clean.violations t2 
    on c.person_id = t2.person_id AND (cohort_date > viol_dttm)
    group by 1, 2
) 
	SELECT 
        person_id, 
	    cohort_date, 
	    coalesce(days_since_last_disp, max (days_since_last_disp) over (order by cohort_date range between unbounded preceding and current row) + 1) as days_since_last_disp, 
	    coalesce(days_since_last_viol, max (days_since_last_viol) over (order by cohort_date range between unbounded preceding and current row) + 1) as days_since_last_viol,
	    cohort_date - '2012-01-01'::date as days_since_2012
	FROM days_since_disp JOIN days_since_viol
	USING (person_id, cohort_date)
    ORDER BY 2,1