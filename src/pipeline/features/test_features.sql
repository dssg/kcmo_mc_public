with cohort as (
    select 
        person_id,
        cohort_date
    from {schema_name}.{cohort_table_name}
),
ages as (
    select
        DATE_PART('year', cohort_date::date) - DATE_PART('year', dob::date) as age,
        cohort.person_id as person_id,
        cohort_date
    from cohort
    left join clean.dispositions dp
    on cohort.person_id = dp.person_id
    left join clean.id_with_cases
    on cohort.person_id = clean.id_with_cases.person_id
)
select person_id, cohort_date, age from ages
group by person_id, cohort_date, age
order by cohort_date, person_id ASC