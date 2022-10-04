with cohort as (
    select 
        person_id,
        cohort_date::date
    from {schema_name}.{cohort_table_name} 
)
SELECT
        c.person_id as person_id,
        cohort_date,
        {feature_cols}
FROM cohort c
LEFT JOIN {from_arg} t 
on c.person_id = t.person_id
group by 1, 2
ORDER BY 2, 1