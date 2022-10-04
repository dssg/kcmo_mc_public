--chrg_desc count
select "CHRG_DESC" from tmp_raw.probations p 
limit 10


select "STATUTE_ORD"  from tmp_raw.probations p 
limit 10


select "RVSD_CHRG_DESC"  from tmp_raw.probations p 
limit 10

select "RVSD_STATUTE"  from tmp_raw.probations p 
limit 10

select p."RVSD_STATUTE",count(p.""RVSD_STATUTE"")  
from tmp_raw.probations p 
limit 10

--looking for the charg description counts, by length of time
select distinct (p."RVSD_CHRG_DESC"),count(p."RVSD_CHRG_DESC"), 
avg(age(p."RETURN_DATE"::date, p."START_DTTM"::date)) as "avg_time"
from tmp_raw.probations p 
group by 1
order by 2 


select p."CASE_NUM", p."RETURN_DATE"::date, p."START_DTTM"::date
from tmp_raw.probations p 


select p."RVSD_STATUTE",count(p."RVSD_STATUTE") as "count of RVSD_STATUTE"
from tmp_raw.probations p 
group by p."RVSD_STATUTE"
limit 10


select p."CASE_NUM",count(p."CASE_NUM")  as "count of cases"
from tmp_raw.probations p 
group by p."CASE_NUM"
limit 10


select p."ZIPCODE",count(p."ZIPCODE") "count of ZIPCODE"
from tmp_raw.probations p 
group by p."ZIPCODE"
limit 10


select p."FINAL_ACTION" ,count(p."FINAL_ACTION")
from tmp_raw.probations p 
group by p."FINAL_ACTION" 
limit 10