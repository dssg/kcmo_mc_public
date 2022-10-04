with cohort as (
select
	cohort.person_id,
	cohort_date::date
from 
    {schema_name}.{cohort_table_name} 
    ),
    dems as (
select
	cohort.person_id,
	cohort_date,
	coalesce (
		age_at_viol,
		last_value (age_at_viol) over (
    		partition by cohort.person_id, cohort_date
			order by cohort_date asc
    	range between unbounded preceding and current row
    	),
		last_value (age_at_disp) over (
    		partition by cohort.person_id, cohort_date
			order by cohort_date asc
    		range between unbounded preceding and current row
    	),
		AVG(age_at_viol) over (
			order by cohort_date asc
    		range between unbounded preceding and current row
    	)
    ) as age_at_viol,
	case
		when age_at_viol is null then 1
		else 0
	end as age_at_viol_imp,
	coalesce (
    	age_at_disp,
		AVG(age_at_disp) over(
			order by cohort_date
    		range between unbounded preceding and current row
    	)
    ) as age_at_disp,
	case
		when age_at_disp is null then 1
		else 0
	end as age_at_disp_imp,
	case
		when race = 'A' then 1
		else 0
	end as race_a,
	case
		when race = 'B' then 1
		else 0
	end as race_b,
	case
		when race = 'I' then 1
		else 0
	end as race_i,
	case
		when race = 'U' then 1
		else 0
	end as race_u,
	case
		when race = 'W' then 1
		else 0
	end as race_w,
	case
		when race is null then 1
		else 0
	end as race_missing,
	case
		when sex = 'F' then 1
		else 0
	end as sex_f,
	case
		when sex = 'M' then 1
		else 0
	end as sex_m,
	case
		when sex = 'X' then 1
		else 0
	end as sex_x,
	case
		when sex is null then 1
		else 0
	end as sex_missing
from
	cohort
left join
	clean.dispositions t on
	cohort.person_id = t.person_id
	and cohort_date = disp_date::date
left join 
	clean.demographics
		using (case_num)
order by
	person_id,
	cohort_date
), by_disp as(
select
	person_id ,
	cohort_date,
	round(avg(age_at_viol)) as avg_age_at_viol,
	round(avg(age_at_viol_imp)) as age_at_viol_imp,
	round(avg(age_at_disp)) as age_at_disp,
	round(avg(age_at_disp_imp)) as age_at_disp_imp,
	round(avg(race_a)) as race_a,
	round(avg(race_b)) as race_b,
	round(avg(race_i)) as race_i,
	round(avg(race_u)) as race_u,
	round(avg(race_w)) as race_w,
	round(avg(race_missing)) as race_missing,
	round(avg(sex_f)) as sex_f,
	round(avg(sex_m)) as sex_m,
	round(avg(sex_x)) as sex_x,
	round(avg(sex_missing)) as sex_missing
from
	dems
group by
	person_id,
	cohort_date
)
select * FROM by_disp
ORDER BY cohort_date, person_id