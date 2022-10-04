set role "kcmo-mc-role";

--TO-DO
--avg. probation years by race

select 
	count(distinct case_num) 
from raw_court.dssgname; --114820 case

select 
	count(distinct case_num), 
	count(distinct last_name)
from raw_court.dssgname; --16090 last names

 -- dist of race
select 
	race, 
	count(distinct case_num) 
from raw_court.dssgname 
group by race 
order by count(*); 

-- dist of sex
select 
	sex, 
	count(*) 
from raw_court.dssgname 
group by sex; 

--suffix?
select 
	gen_sfx,
	count(*)
from raw_court.dssgname 
group by gen_sfx

select 
	prob_agcy, 
	count(distinct case_num) 
from raw_court.dssgc_ordv6222022 do2 
group by prob_agcy  
order by count(*) desc; -- should join this table with codes_fixed to make sense of agency names

-- what are counts of probation remarks? these appear to be more descriptive, free form comments
select 
	count(*), 
	prob_rmrk 
from raw_court.dssgc_ordv6222022 
group by prob_rmrk 
order by count(*) desc;


-- Case numbers per person - max is 39
select 
	last_name, 
	first_name,
    count(distinct case_num) as num_cases
from raw_court.dssgname 
group by last_name, first_name, dob
order by num_cases desc;

--all cases for the individual with 39 cases
select * 
from raw_court.dssgname
where first_name = 'Z'
and last_name = 'ZZZ';


select * d 
from raw_court.dssgname 
where last_name = 'ZZZ';

select * 
from raw_court.dssgname 
where last_name ilike 'XXX' and first_name ilike '%XXX%';

--how many people in total?  57832
select count (*) from(
	select first_name , last_name, dob
	from raw_court.dssgname
	group by last_name, first_name, dob
)t;

-- distribution of charges associated with distinct cases
select 
	chrg_desc, 
	count(distinct case_num)
from raw_court.dssgcharge6222022  
group by chrg_desc 
order by count(distinct case_num) desc; 

------
-- navigating/getting information on schema/tables
select * from pg_catalog.pg_tables;

select tablename 
from pg_catalog.pg_tables 
where schemaname = 'raw_ccl';

select * 
from information_schema.columns 
where column_name ilike '%Assessment%';
-------

-- counts of type of attorneys
select 
	atty_type,
	count(*) 
from raw_court.code_atty_fixed 
group by atty_type; -- should join with codes_fixed, and see the outcomes of each atty


-- count dist. of each type of probation supervision fee and what the code stands for
select 
	prob_supv_fee_code,
	code_desc,
	count(distinct case_num)
from raw_court.dssgchrg_prob6222022  p
left join raw_court.codes_fixed c 
on p.prob_supv_fee_code = c.code
group by prob_supv_fee_code, code, code_desc
order by count (*) desc;

-- 156 probation codes and what the codes stand for and how many of each
select  
	prob_code, 
	code_desc, 
	count(*) 
from raw_court.dssgc_ordv6222022  
left join raw_court.codes_fixed 
on dssgc_ordv6222022.prob_code = codes_fixed.code
group by prob_code,code_desc 
order by count(*) desc;  -- should fix the duplicates issue by also joining on code_type

select code_type, count(*) from raw_court.codes_fixed group by code_type;


select 
	rvsd_chrg_desc, 
	count(*) 
from raw_court.dssgcharge
group by rvsd_chrg_desc 
order by count(*) desc; -- 111 unique charges, most common non-null charge is improper regsitration cert title

select count(*) from raw_court.dssgcharge; --114820 rows

select 
	plea, 
	count(*) 
from raw_court.dssgcharge
group by plea 
order by count(*) desc; 

select 
	disp_code, 
	disp_rsn, 
	count(*) 
from raw_court.dssgcharge
group by disp_code, disp_rsn 
order by count(*) desc; 

select 
	chrg_seq_num, 
	disp_seq_num,
	count(*) 
from raw_court.dssgcharge
group by chrg_seq_num, disp_seq_num 
order by count(*) desc; 

select rvsd_chrg_severity, "RVSD_SEND_TO_DOR"  ,count(*) from raw_court.dssgcharge
group by rvsd_chrg_severity,"RVSD_SEND_TO_DOR"   order by count(*) desc; 

with individuals as (select first_name, last_name,  dob 
from raw_court.dssgname  
group by last_name, first_name, dob)
select count(*) from individuals;


--create table clean.individual_ids as (select first_name, last_name,  dob from raw_court.dssgname 
--group by last_name, first_name, dob)
--select count (num_people) from individuals;
--create schema clean;
--drop table clean.individual_ids -- to delete
--create table clean.individual_ids (
--first_name varchar, last_name  varchar, dob varchar, dssg_id  serial)
--insert into clean.individual_ids (
--select first_name as first_name, last_name as last_name,  dob as dob
--from raw_court.dssgname 
--group by last_name, first_name, dob)

with individuals as (select first_name, last_name,  dob
from raw_court.dssgname 
group by last_name, first_name, dob)
select * from individuals

-- for histogram, # of unique cases per indv.
select 
	count(distinct case_num) as num_cases
from raw_court.dssgname 
group by last_name, first_name, dob
order by num_cases desc;

-- number of cases that fall under a particular sentence execution code
select 
	sent_exec_code, 
	count(*) as num_cases
from raw_court.dssgcharge 
group by chrg_seq_num, disp_seq_num, sent_exec_code
order by num_cases desc;

-- number of cases that fall under a particular sentence execution code
select 
	code_desc, 
	sent_exec_code, 
	count(*) as num_cases
from raw_court.dssgcharge 
left join raw_court.codes_fixed cf 
on dssgcharge.sent_exec_code =  cf.code
group by code_desc, sent_exec_code
order by num_cases desc;


select  
	prob_code, 
	code_desc, 
	count(*) 
from raw_court.dssgc_ordv6222022  
left join raw_court.codes_fixed 
on dssgc_ordv6222022.prob_code = codes_fixed.code
group by prob_code, code_desc order by count(*) desc; 

select count (*) from(
	select 
		first_name, 
		last_name, 
		dob
	from raw_court.dssgname 
	group by last_name, first_name, dob
)t;  ---57832 indiv.

-- check if same number of distinct cases in various files, 114820
select count(distinct case_num), count(*) from raw_court.dssgproject6622;
select count(distinct case_num), count(*) from raw_court.dssgcharge6222022 ;
select count(distinct case_num), count(*) from raw_court.dssgcharge;
select count(distinct case_num), count(*) from raw_court.dssgchrg_prob6222022;
select count(distinct case_num), count(*) from raw_court.dssgname;
select count(distinct case_num), count(*) from raw_court.assoc_case6222022_fixed;
select count(distinct case_num), count(*) from raw_court.dssgchrg_recommend6222022;

-- count probation terms - SIS/SES combinations, with description of codes --- with multiple rows due to different types for 
--each acronym
with cases as (
	select * 
	from raw_court.dssgc_ordv6222022 do2
	inner join raw_court.dssgcharge d 
	on do2.case_num=d.case_num), 
sis_ses as (
	select 
		prob_code, 
		sent_exec_code,  
		count(*) as numcases 
	from cases group by prob_code, sent_exec_code)
select 
	prob_code, 
	sent_exec_code, 
	numcases, 
	code_desc from sis_ses 
inner join raw_court.codes_fixed cf 
on sis_ses.prob_code = cf.code
order by numcases desc;



-- count probation terms - SIS/SES combinations, with description of codes --- INNER JOIN ON FULL codes_fixed
with cases as (
	select * 
	from raw_court.dssgc_ordv6222022 do2
inner join raw_court.dssgcharge d 
on do2.case_num=d.case_num
), 
sis_ses as (
	select 
		prob_code,
		sent_exec_code,  
		count(*) as numcases 
	from cases group by prob_code,sent_exec_code
)
select 
	prob_code, 
	sent_exec_code, 
	numcases, 
	code_desc 
from sis_ses 
inner join raw_court.codes_fixed cf 
on sis_ses.prob_code = cf.code 
where code_type = 'PROBCNDS' order by numcases desc;

-- count probation terms - SIS/SES combinations, with description of codes --- final
with cases as (
	select * 
	from raw_court.dssgc_ordv6222022 do2
inner join raw_court.dssgcharge d 
on do2.case_num = d.case_num
), 
sis_ses as (
	select 
		prob_code,
		sent_exec_code,  
		count(*) as numcases 
	from cases 
	group by prob_code, sent_exec_code
),
codes as (
	select 
		code_type, 
		code, 
		code_desc 
	from raw_court.codes_fixed 
	group by code_type, code, code_desc
)
select 
	prob_code, 
	sent_exec_code, 
	numcases, 
	code_desc 
from sis_ses 
inner join codes 
on sis_ses.prob_code = codes.code 
where code_type = 'PROBCNDS' 
order by numcases desc;

--counts of different sent_exec types
with c as (
	select * 
	from raw_court.dssgcharge do3 
inner join raw_court.codes_fixed cf 
on  cf.code=do3.sent_exec_code
)
select 
	sent_exec_code, 
 	count(*), 
 	code_desc
 from c 
group by sent_exec_code, code_desc 
order by count(*) desc;

--counts of different
with c as (
	select * 
	from raw_court.dssgcharge6222022 do3 
inner join raw_court.codes_fixed cf 
on  cf.code=do3.statute_ord
)
select 
	statute_ord, 
 	count(*), 
 	code_desc
 from c 
group by statute_ord, code_desc 
order by count(*) desc;

--Counts of cases by SIS/SES and prob codes
with cases as (
	select * 
	from raw_court.dssgc_ordv6222022 do2
	inner join raw_court.dssgcharge d 
	on do2.case_num = d.case_num
)
select 
	prob_code,
	sent_exec_code,
	count(*) from cases
group by prob_code, sent_exec_code 
order by COUNT(*) desc;


--Disposition codes distribution
with cases as (
	select * 
	from raw_court.dssgc_ordv6222022 do2
inner join raw_court.dssgcharge d 
on do2.case_num = d.case_num
)	
select 
	disp_code, 
	count (*) 
from cases 
group by disp_code 
order by COUNT(*) desc;




--Disposition codes distribution  - duplicate rows for each code, need to specify code_type
--with cases as (select * from raw_court.dssgchrg_prob6222022 dp 
--inner join raw_court.codes_fixed cf on dp.disp_code = cf.code)
--select disp_code, code_desc, count (distinct case_num ) from cases group by disp_code, code_desc order by COUNT(*) desc

--select COUNT (*) from raw_court.dssgchrg_prob6222022

-- How many dispositions each year?
with cases as (
	select * 
	from raw_court.dssgc_ordv6222022 do2
inner join raw_court.dssgcharge d 
on do2.case_num = d.case_num
)
select 
	count(*), 
	extract(year from disp_date::timestamp::date) as disp_year 
from cases 
group by disp_year 
order by disp_year desc;


--Disposition codes distribution 2 ROW
with cases as (
	select * from raw_court.dssgchrg_prob6222022 dp 
	inner join raw_court.codes_fixed cf 
	on dp.disp_code = cf.code
	where code_type = 'OSCAIMCD'
)
select 
	disp_code, 
	code_desc, 
	count (distinct case_num) 
from cases 
group by disp_code, code_desc 
order by COUNT(*) desc

--Disposition codes distribution using code_type = 'PLEA', 2 rows
with cases as (
	select * from raw_court.dssgcharge dc
	inner join raw_court.codes_fixed cf 
	on dc.plea = cf.code
	where code_type = 'PLEA'
)
select 
	plea, 
	code_desc, 
	count (distinct case_num) 
from cases 
group by plea, code_desc 
order by COUNT(*) desc;

-- what do the various codes with type OSCAIMCD mean
select 
	code, 
	code_desc 
from raw_court.codes_fixed
where code_type = 'OSCAIMCD'
group by code, code_desc;

--select code_type, count(*) from raw_court.codes_fixed cf group by code_type order by count (distinct code_type) desc


-- all the code type and descriptions in codes_fixed that match code = 'G'
select
	code_type, 
	code, 
	code_desc 
from raw_court.codes_fixed 
where code = 'G' 
group by code, code_desc, code_type;

--checking which code type the code 'satop' appears with
select 
	count(*), 
	code_type, 
	code, 
	code_desc 
from raw_court.codes_fixed 
where code = 'SATOP' 
group by code, code_desc, code_type;

--checking which code type the code 'DNDMV' appears with, and its description
select 
	count(*), 
	code_type, 
	code, 
	code_desc 
from raw_court.codes_fixed 
where code = 'DNDMV' 
group by code , code_desc, code_type;

--checking the descriptions of various fee codes
select 
	code_desc, 
	code
from raw_court.codes_fixed cf 
where code_type = 'ASDSRCRO';

--list of probation agencies and codes
select 
	code_desc, 
	code  
from raw_court.codes_fixed cf 
where code_type = 'PROBAGCY';

--distribution of sex
with demog as (
	select 
		last_name, 
		first_name, 
		dob,  
		sex 
	from raw_court.dssgname 
	group by last_name, first_name, dob, sex
)
select 
	sex, 
	count(*) 
from demog 
group by sex;

-- how many zipcodes fall in KCMO (71440) vs. not (18036) -- some people may have moved, hence does not add up to 57,832
with zipcase as (
	select 
		last_name, 
		first_name, 
		dob, 
		rcd.case_num, 
		zipcode 
	from raw_court.dssgname rcd
	inner join raw_court.dssgaddress6222022 rcda 
	on rcda.case_num=rcd.case_num
), 
demog as (
	select 
		last_name, 
		first_name, 
		dob, 
		zipcode 
	from zipcase 
	group by last_name, first_name, dob, zipcode
),
kc as (
	select 
		case when substring(zipcode, 1, 2) = '64' then 1 else 0 end as zip2 
	from demog
)
select 
	zip2, 
	count(*) 
from kc 
group by zip2;

with zipcase as (select last_name, first_name, dob, rcd.case_num, zipcode from raw_court.dssgname rcd
inner join raw_court.dssgaddress6222022 rcda on rcda.case_num=rcd.case_num), 
demog as (select last_name , first_name , dob from zipcase group by last_name , first_name , dob)
select count(*) from demog  --adds up to 57,832

--zipcodes first 5 digits grouped by race - neighborhood racial percentages can be compared with this to see if disproportionate prob
with zipprob as (select last_name, first_name, dob, race, rcd.case_num, substring(zipcode, 1, 5) as zip 
from raw_court.dssgname rcd inner join raw_court.dssgaddress6222022 rcda on rcda.case_num=rcd.case_num)
select zip, count (*), race from zipprob group by zip,race order by count(*) desc;

with zipprob as (select last_name, first_name, dob, race, rcd.case_num, substring(zipcode, 1, 5) as zip 
from raw_court.dssgname rcd inner join raw_court.dssgaddress6222022 rcda on rcda.case_num=rcd.case_num)
select zip, count (*) from zipprob group by zip order by count(*) desc limit 15;

--zipcodes first 5 digits grouped by sex
with zipprob as (select last_name, first_name, dob,  sex, rcd.case_num, substring(zipcode, 1, 5) as zip 
from raw_court.dssgname rcd inner join raw_court.dssgaddress6222022 rcda on rcda.case_num=rcd.case_num)
select zip, count (*), sex from zipprob group by zip,sex order by count(*) desc;

-- how many zipcodes are associated with each indv? mostly 1-2, but goes all the way up to 11-12.
with zipcase as (select last_name, first_name, dob, rcd.case_num, zipcode from raw_court.dssgname rcd
inner join raw_court.dssgaddress6222022 rcda on rcda.case_num=rcd.case_num), 
demog as (select last_name , first_name , dob, zipcode from zipcase group by last_name , first_name , dob, zipcode)
select last_name, first_name, dob, count(distinct substring(zipcode, 1, 5)) as k from demog group by last_name, first_name, dob order by k desc;

-- (not done) race/zip code/charge etc. counts 
--with zipcase as (select last_name, first_name, dob, rcd.case_num, zipcode from raw_court.dssgname rcd
--inner join raw_court.dssgaddress6222022 rcda on rcda.case_num=rcd.case_num), 
--demog as (select last_name , first_name , dob, zipcode from zipcase group by last_name , first_name , dob, zipcode),
--chrg as (select * from demog inner join raw_court rc on demog.case_num = rc.case_num)
--select * from chrg

-- How many cases each year? how many indvs each year?
with cases as (select * from raw_court.dssgc_ordv6222022 do2
inner join raw_court.dssgcharge d on do2.case_num=d.case_num),
names as (select * from cases inner join raw_court.dssgname n using (case_num))
select * from names;

-- distinct case numbers with prob start years by year
select count(distinct case_num), extract(year from start_dttm::timestamp::date) as start_year 
from raw_court.dssgc_ordv6222022 do2  
group by start_year order by start_year desc;

-- adding up the numbers from the table above, 115642 
with c as (select count(distinct case_num), extract(year from start_dttm::timestamp::date) as start_year
from raw_court.dssgc_ordv6222022 do2  
group by start_year order by start_year desc)
select sum(count) from c;

-- number of probation terms starting in each year (adds up to 173930),
-- number of probation terms starting in each year associated with distinct case numbers (adds up to 115642)
select count(*) as count_all, count(distinct case_num) as count_distinct_cases, 
extract(year from start_dttm::timestamp::date) as start_year
from raw_court.dssgc_ordv6222022 group by start_year; 

-- number of probation terms with a start date in each year, grouped by defendants.. adds up to ~79k
with ordv_name as (select * from raw_court.dssgc_ordv6222022 do2  
inner join raw_court.dssgname n on do2.case_num = n.case_num),
ordv_name2 as (select last_name, first_name, dob, start_dttm from ordv_name 
group by last_name, first_name, dob, start_dttm)
select count(*), extract(year from start_dttm::timestamp::date) as start_year
from ordv_name2 group by start_year order by start_year;

--counts of probation terms per year
select extract(year from start_dttm::timestamp::date) as start_year, prob_code, count(*) as c
from raw_court.dssgc_ordv6222022 do2 group by  start_year, prob_code order by start_year, prob_code

-- which probation terms were most common in each year?
select extract(year from start_dttm::timestamp::date) as start_year, prob_code, count(*) as c
from raw_court.dssgc_ordv6222022 do2 group by  start_year, prob_code order by c desc;
--most common is PROB for most years, followed by:
-- DNDMV stands for DO NOT DRIVE A MOTOR VEHICLE UNLESS LICENSED AND INSURED
-- DNOOFF Do Not Obtain Any Similar Offenses
-- CMSR20	Community Service-20 hours
-- DIP4HR	Driver Improvement-4 hour

--how many probation codes/terms in total, same as select count(*) from raw_court.dssgc_ordv6222022
with probc as (select extract(year from start_dttm::timestamp::date) as start_year, prob_code, count(*) as c
from raw_court.dssgc_ordv6222022 do2 group by  start_year, prob_code order by start_year, prob_code)
select sum(c) from probc; --173930

select * from raw_court.dssgcaseaudt2021
where case_num ilike '200%'
order by case_num;


select * from raw_court.assoc_data_fixed adf 
where case_num = '200040566-6' order by dkt_dttm::timestamp;

select 
	statute_ord, chrg_desc,
	count (distinct case_num) as counts
from raw_court.dssgcharge6222022 d 
group by statute_ord, chrg_desc
order by counts desc; 

--counts of statute_ord per year
select 
	extract(year from dor_conv_sent_dttm::timestamp::date) as start_year, 
	statute_ord, 
	count(*) as c
from raw_court.dssgcharge6222022 d 
group by start_year, statute_ord 
order by start_year, c desc;

select 
	extract(year from enter_dttm::timestamp::date) as start_year, 
	statute_ord, 
	count(*) as c
from raw_court.dssgcharge6222022 d 
group by start_year, statute_ord 
order by start_year, c desc;


select 
	statute_ord, chrg_desc,
	count (distinct case_num) as counts
from raw_court.dssgcharge6222022 d 
where chrg_desc ilike '%LARCENY%'
group by statute_ord, chrg_desc
order by counts desc; 

select * from raw_court.dssgcharge6222022 d 
where statute_ord  ilike '%62-89%'; 

-- How many cases from before 2012, 5822 cases
select count(*) from raw_court.dssgcharge6222022 d 
where case_num ilike 'N%' or case_num ilike 'L%' or case_num ilike '83%'


select 
	statute_ord, chrg_desc,
	count (distinct case_num) as counts
from raw_court.dssgcharge6222022 d 
where chrg_desc ilike '%theft%'
group by statute_ord, chrg_desc
order by counts desc; -- combine 50-120B, (b) etc

select 
	statute_ord, chrg_desc,
	count (distinct case_num) as counts
from raw_court.dssgcharge6222022 d 
where chrg_desc ilike '%licen%'
group by statute_ord, chrg_desc
order by counts desc; 

select 
	statute_ord, chrg_desc,
	count (distinct case_num) as counts
from raw_court.dssgcharge6222022 d 
group by statute_ord, chrg_desc
order by counts desc; 

select 
	statute_ord, chrg_desc,
	count (distinct case_num) as counts
from raw_court.dssgcharge6222022 d 
group by statute_ord, chrg_desc; 

select dkt_dttm from raw_court.assoc_data6222022_fixed adf 




select info_type, code_type, code, code_desc , array_agg(distinct audit_userid)
from raw_court.dssgcaseaudt2019 d
left join raw_court.codes_fixed cf on d.info_type = cf.code and cf.code_type = 'AUDINFTP'
group by code_type, code, code_desc , info_type
order by info_type; 


select count (*) 
from raw_court.dssgc_ordv6222022 do2 


select street_name_1, 
count(*) 
from raw_court.dssgaddressnoproba d 
group by street_name_1 
order by count(*) desc;


select 
	column_name, 
	table_schema, 
	table_name  
from information_schema.columns 
where column_name ilike '%cont%' and  table_schema = 'raw_court';

select 
	distinct assoc_type, 
	code, code_type, 
	code_desc 
from raw_court.assoc_data6222022_fixed adf 
left join raw_court.codes_fixed cf 
on adf.assoc_type = cf.code 
order by assoc_type; 

select 	
	extract(year from disp_date::timestamp::date) as start_year, 
	sent_exec_code, 
	count(*)
from raw_court.dssgcharge
group by start_year,sent_exec_code order by start_year 


with sent as (select 
	 sent_exec_code, 
	 extract(year from disp_date::timestamp::date) as start_year
	--code_desc,
from raw_court.dssgcharge d 
inner join raw_court.codes_fixed cf 
on d.sent_exec_code = cf.code)
select 
	sent_exec_code, 
	start_year, 
	count(*) from sent 
where start_year > '2011' 
group by start_year, sent_exec_code 
order by start_year, sent_exec_code;


select 
	final_action, 
	count(*) 
from raw_court.dssgc_ordv6222022 do2 
group by final_action;


with ordv as(
select do2.case_num, prob_code, final_action 
from raw_court.dssgc_ordv6222022 do2
left join raw_court.dssgcharge d on d.case_num = do2.case_num)
select count(*) from ordv

select count(*) from raw_court.dssgcharge

select count(*) from raw_court.assoc_data_fixed adf 

select count(*) from raw_court.dssgname d  

select count (*) 
from raw_court.dssgcharge6222022 d  


select count(distinct statute_ord), count(distinct chrg_desc) from raw_court.dssgcharge6222022 d 



-- clean statute ord
--update raw_court.dssgcharge6222022
--set statute_ord = '50-10' 
--where chrg_desc = 'POSSESSION OF MARIJUANA';
--
--update raw_court.dssgcharge6222022
--set statute_ord = '50-201' 
--where chrg_desc = 'POSS NARCOTIC PARAPHENAL';
--
--update raw_court.dssgcharge6222022
--set statute_ord = '50-72' 
--where chrg_desc = 'PROSTzITUTION PATRONIZE';
--
--update raw_court.dssgcharge6222022
--set statute_ord = '70-304' 
--where chrg_desc = 'OPER MV WITH BAC OF 08';

--update raw_court.dssgcharge6222022
--set statute_ord = '70-302' 
--where chrg_desc = 'OPER MTR VEH UNDR INFLNC';

--update raw_court.dssgcharge6222022
--set statute_ord = '50-44' 
--where chrg_desc = 'OBSTRUCTING AN OFFICER';
--
--update raw_court.dssgcharge6222022
--set statute_ord = '48-30' 
--where chrg_desc = 'RANK WEEDS NOXIOUS PLANT';

--update raw_court.dssgcharge6222022
--set statute_ord = '50-111.5(A)' 
--where chrg_desc = 'RECEIVE STOLEN PROPERTY';
--
--update raw_court.dssgcharge6222022
--set statute_ord = '50-121' 
--where chrg_desc = 'MAL DESTRUCTION OF PROP';

update raw_court.dssgcharge6222022
set chrg_desc = 'MAL DESTRUCTION OF PROP'
where chrg_desc = 'MALIC DEST OF PROPERTY';

--update raw_court.dssgcharge6222022
--set statute_ord = '14-31(C)' 
--where chrg_desc = 'PUBLIC NUISANCE';

--update raw_court.dssgcharge6222022
--set statute_ord = '50-270(A)' 
--where chrg_desc = 'NO INSURANCE';

--update raw_court.dssgcharge6222022
--set statute_ord = '50-72' 
--where chrg_desc = 'SOLICIT IMMORAL PURPOSE';
--
--update raw_court.dssgcharge6222022
--set statute_ord = '50-169' 
--where chrg_desc = 'SIMPLE ASLT NOT FIGHT';
--
--update raw_court.dssgcharge6222022
--set statute_ord = '50-44' 
--where chrg_desc = 'RESISTING OFFICER';
--
--update raw_court.dssgcharge6222022
--set statute_ord = '50-261' 
--where chrg_desc = 'POSSESSION OF WEAPON';
--
--update raw_court.dssgcharge6222022
--set statute_ord = '50-261' 
--where chrg_desc = 'CARRYING WEAPONS';
--
--update raw_court.dssgcharge6222022
--set statute_ord = '48-25' 
--where chrg_desc = 'TRASH PUB PRIVATE PROP';
--
--
--update raw_court.dssgcharge6222022
--set statute_ord = '70-273' 
--where chrg_desc = 'TOW VEH AND ACC SCENES';
--
--update raw_court.dssgcharge6222022
--set statute_ord = '70-400' 
--where chrg_desc = 'WRG SIDE OF DIV HWY-STR';
--
--update raw_court.dssgcharge6222022
--set statute_ord = '48-27' 
--where chrg_desc = 'WRECK DAM DEM DIS VEHICL';

--update raw_court.dssgcharge6222022
--set statute_ord = '70-955' 
--where chrg_desc = 'VIOL FLASHING SIGNALS';
--
--update raw_court.dssgcharge6222022
--set statute_ord = '14-16A' 
--where chrg_desc = 'VIOL OF CITY ANIMAL ORD';
--
--update raw_court.dssgcharge6222022
--set statute_ord = '50-106' 
--where chrg_desc = 'LARCENY-UNDER $50';
--
--update raw_court.dssgcharge6222022
--set statute_ord = '50-47' 
--where chrg_desc = 'VIOLATION OF ORD OF PROT';

update raw_court.dssgcharge6222022
set chrg_desc  = 'VEH ON LEFT FAILD T YLD'
where chrg_desc = 'VEH TURN LEFT-FAIL T YLD';

--update raw_court.dssgcharge6222022
--set statute_ord = '70-331(A)' 
--where chrg_desc = 'VEH ON LEFT FAILD T YLD';

--
--update raw_court.dssgcharge6222022
--set statute_ord = '50-231' 
--where chrg_desc = 'CHILD ENDANGERMENT';
--
--update raw_court.dssgcharge6222022
--set statute_ord = '14-29' 
--where chrg_desc = 'DANGEROUS DOGS';
--
--
--update raw_court.dssgcharge6222022
--set statute_ord = '50-261' 
--where chrg_desc = 'UNLAWFUL USE OF WEAPON';

--update raw_court.dssgcharge6222022
--set statute_ord = '50-102' 
--where chrg_desc = 'TRESPASS-UNLAWFUL ENTRY';
--
--update raw_court.dssgcharge6222022
--set statute_ord = '50-128(B)' 
--where chrg_desc = 'TRESPAS VACANT BLDG PROP';
--
--update raw_court.dssgcharge6222022
--set statute_ord = '50-128(B)' 
--where chrg_desc = 'TRESPASS VACANT BLDG';
--
--update raw_court.dssgcharge6222022
--set statute_ord = '50-106' 
--where chrg_desc = 'LARCENY ATTEMPT';
--
--update raw_court.dssgcharge6222022
--set statute_ord = '50-106' 
--where chrg_desc = 'LARCENY-$50 TO $199';
--
--update raw_court.dssgcharge6222022
--set statute_ord = '50-106' 
--where chrg_desc = 'LARCENY-$200 AND OVER';

--update raw_court.dssgcharge6222022
--set chrg_desc = 'TRESSPASS AT VACANT PROP' 
--where chrg_desc = 'TRESPASS AT VACANT PROP';
--
--update raw_court.dssgcharge6222022
--set chrg_desc = 'TRESPAS VACANT BLDG PROP' 
--where chrg_desc = 'TRESPASS VACANT BLDG';

--update raw_court.dssgcharge6222022
--set statute_ord  = '50-128(C)' 
--where chrg_desc = 'TRESSPASS AT VACANT PROP';
--
--update raw_court.dssgcharge6222022
--set statute_ord  = '50-102' 
--where chrg_desc = 'TRESPASS GENERALLY';
--
--update raw_court.dssgcharge6222022
--set statute_ord  = '50-102' 
--where chrg_desc = 'DART TRESPASS';

--update raw_court.dssgcharge6222022
--set chrg_desc = 'TRESPASS GENERALLY'
--where chrg_desc = 'DART TRESPASS';

select 
	statute_ord, chrg_desc,
	count (distinct case_num) as counts
from raw_court.dssgcharge6222022 d 
group by statute_ord, chrg_desc
order by chrg_desc  desc;

select 
	statute_ord, chrg_desc,
	count (distinct case_num) as counts
from raw_court.dssgcharge6222022 d 
group by statute_ord, chrg_desc
order by counts  desc; 

select 
	statute_ord, chrg_desc,
	count (distinct case_num) as counts
from raw_court.dssgcharge6222022 d 
where chrg_desc ilike '%tres%'
group by statute_ord, chrg_desc
order by chrg_desc  desc; 

select 
	statute_ord,
	count (distinct case_num) as counts
from raw_court.dssgcharge6222022 
group by statute_ord 
order by counts desc;



with individuals as (select first_name, last_name,  dob 
from raw_court.dssgname  
group by last_name, first_name, dob)
select count(*) from individuals;

-- individuals and how many cases they have associated with them
with individuals as (
	select 
		first_name, 
		last_name,  
		dob, 
		n.case_num  
from raw_court.dssgname n
left join raw_court.dssgc_ordv6222022 do2 
on n.case_num = do2.case_num)
select 
	last_name, first_name, dob, count(distinct case_num) as cases
from individuals
group by last_name, first_name, dob
order by cases desc;

-- individuals and their earliest case dates
with individuals as (
	select 
	first_name, last_name, dob, n.case_num, start_dttm 
from raw_court.dssgname n
inner join raw_court.dssgc_ordv6222022 do2 
on n.case_num = do2.case_num)
select 
	last_name, first_name, dob, min(cast(start_dttm as date))
from individuals
group by last_name, first_name, dob


select * from clean.dispositions d; 

with chrg as(
SELECT
	d.case_num::varchar,
	--chrg_seq_num::int,
	--disp_seq_num::int,
	disp_date::timestamp,
	--plea_date::text,
	d.plea::varchar,
	--disp_code::text,
	--disp_rsn::text,
	disp_crtrm::varchar,
	disp_dkt_type::varchar,
	sent_exec_code::varchar,
	--sent_exec_date::timestamp,
	--confidential_sw::varchar,
	jail_days::int,
	community_serv::int,
	--mandatory_insurance::varchar,
	--drvr_imprvmnt_pgm_sw::varchar,
	--accident_addtnl_pts_sw::varchar,
	--chng_of_venue_ori::varchar,
	--rvsd_chrg_code::int,
	--rvsd_chrg_desc::varchar,
	--rvsd_chrg_severity::varchar,
	--rvsd_send_to_dor::varchar,
	--rvsd_statute::varchar,
	judg_id::varchar,
	--prob_term_date::timestamp,
	prob_days::int,
	--enter_dttm::timestamp,
	--enter_userid::timestamp,
	--change_dttm::timestamp,
	--change_userid::varchar,
	--enter_tran_id::varchar,
	prob_months::int,
	prob_years::int,
	--sent_type::varchar,
	sent_jail_days::int,
	sent_community_serv::int,
	--sent_flat_sw::varchar,
	--sent_case_num::varchar,
	prob_supv_fee_code::varchar,
	--prob_supv_fee_cycles::int,
	--confidential_after_case_closed_sw::varchar
	--mshp_disp_sent_dttm::timestamp,
	--mshp_disp_rcvd_dttm::timestamp,
	statute_ord::varchar,
	case 
		when chrg_desc = 'TRESPASS AT VACANT PROP' then 'TRESSPASS AT VACANT PROP'
		when chrg_desc = 'TRESPASS VACANT BLDG' then 'TRESPAS VACANT BLDG PROP'
	    when chrg_desc = 'DART TRESPASS' then 'TRESPASS GENERALLY'
		when chrg_desc = 'VEH TURN LEFT-FAIL T YLD' then 'VEH ON LEFT FAILD T YLD'
	else chrg_desc 
	end as chrg_desc,
	--chrg_desc::varchar,
	dssg_id::int
	FROM raw_court.dssgcharge d
	left join raw_court.dssgcharge6222022 d2 
	on d.case_num = d2.case_num 
	left join clean.individual_ids ii 
	on d.case_num = ii.case_num	
),
statute as (
	SELECT
	case_num::varchar,
	--chrg_seq_num::int,
	--disp_seq_num::int,
	disp_date::timestamp,
	--plea_date::text,
	plea::varchar,
	--disp_code::text,
	--disp_rsn::text,
	disp_crtrm::varchar,
	disp_dkt_type::varchar,
	sent_exec_code::varchar,
	--sent_exec_date::timestamp,
	--confidential_sw::varchar,
	jail_days::int,
	community_serv::int,
	--mandatory_insurance::varchar,
	--drvr_imprvmnt_pgm_sw::varchar,
	--accident_addtnl_pts_sw::varchar,
	--chng_of_venue_ori::varchar,
	--rvsd_chrg_code::int,
	--rvsd_chrg_desc::varchar,
	--rvsd_chrg_severity::varchar,
	--rvsd_send_to_dor::varchar,
	--rvsd_statute::varchar,
	judg_id::varchar,
	--prob_term_date::timestamp,
	prob_days::int,
	--enter_dttm::timestamp,
	--enter_userid::timestamp,
	--change_dttm::timestamp,
	--change_userid::varchar,
	--enter_tran_id::varchar,
	prob_months::int,
	prob_years::int,
	--sent_type::varchar,
	sent_jail_days::int,
	sent_community_serv::int,
	--sent_flat_sw::varchar,
	--sent_case_num::varchar,
	prob_supv_fee_code::varchar,
	--prob_supv_fee_cycles::int,
	--confidential_after_case_closed_sw::varchar
	--mshp_disp_sent_dttm::timestamp,
	--mshp_disp_rcvd_dttm::timestamp,
	--statute_ord::varchar,
	chrg_desc::varchar,
	case 
		when chrg_desc = 'POSSESSION OF MARIJUANA' then '50-10'
		when chrg_desc = 'POSS NARCOTIC PARAPHENAL' then '50-201' 
		when chrg_desc = 'TRESSPASS AT VACANT PROP' then  '50-128(C)' 
		when chrg_desc = 'PROSTITUTION PATRONIZE' then '50-72'
		when chrg_desc = 'OPER MV WITH BAC OF 08' then '70-304'
		when chrg_desc = 'OPER MTR VEH UNDR INFLNC' then '70-302'
		when chrg_desc = 'OBSTRUCTING AN OFFICER' then '50-44'
		when chrg_desc = 'RANK WEEDS NOXIOUS PLANT' then '48-30'
		when chrg_desc = 'RECEIVE STOLEN PROPERTY' then '50-111.5(A)'
		when chrg_desc = 'MAL DESTRUCTION OF PROP' then '50-121'
		when chrg_desc = 'PUBLIC NUISANCE' then '14-31(C)' 
		when chrg_desc = 'NO INSURANCE' then '50-270(A)'
		when chrg_desc = 'SOLICIT IMMORAL PURPOSE' then '50-72'
		when chrg_desc = 'SIMPLE ASLT NOT FIGHT' then '50-169'
		when chrg_desc = 'RESISTING OFFICER' then '50-44'
		when chrg_desc = 'POSSESSION OF WEAPON' then '50-261'
		when chrg_desc = 'CARRYING WEAPONS' then '50-261' 
		when chrg_desc = 'TRASH PUB PRIVATE PROP' then '48-25'
		when chrg_desc = 'TOW VEH AND ACC SCENES' then '70-273' 
		when chrg_desc = 'WRG SIDE OF DIV HWY-STR' then '70-400'
		when chrg_desc = 'WRECK DAM DEM DIS VEHICL' then '48-27' 		
		when chrg_desc = 'VIOL FLASHING SIGNALS' then '70-955' 
		when chrg_desc = 'VIOL OF CITY ANIMAL ORD' then '14-16A' 
		when chrg_desc = 'LARCENY-UNDER $50' then '50-106' 
		when chrg_desc = 'VIOLATION OF ORD OF PROT' then '50-47'
		when chrg_desc = 'VEH ON LEFT FAILD T YLD' then '70-331(A)'
		when chrg_desc = 'CHILD ENDANGERMENT' then '50-231' 
		when chrg_desc = 'DANGEROUS DOGS' then '14-29' 
		when chrg_desc = 'UNLAWFUL USE OF WEAPON' then '50-261' 
		when chrg_desc = 'TRESPASS-UNLAWFUL ENTRY' then '50-102' 
		when chrg_desc = 'TRESPAS VACANT BLDG PROP' then '50-128(B)'
		when chrg_desc = 'TRESPASS VACANT BLDG' then '50-128(B)' 
		when chrg_desc = 'LARCENY ATTEMPT' then '50-106' 
		when chrg_desc = 'LARCENY-$50 TO $199' then '50-106'
		when chrg_desc = 'LARCENY-$200 AND OVER' then '50-106'
		when chrg_desc = 'TRESPASS GENERALLY' then '50-102' 
		else statute_ord
	end as statute_ord,
	dssg_id::int
from chrg 
)
select 
	statute_ord, chrg_desc,
	count (distinct case_num) as counts
from statute
group by statute_ord, chrg_desc
order by counts  desc; 




-- create variable that stores an array of case numbers for each individual
select 
    first_name, 
    last_name, 
    dob,
    array_agg(case_num) as cases
from raw_court.dssgname 
group by 
 first_name, last_name, dob
 
 

 --create new table with auto increment column as unique ids
DROP TABLE IF EXISTS clean.individual_ids;

create table clean.individual_ids (
    dssg_id serial,
    first_name varchar, 
    last_name  varchar, 
    dob varchar,  
    cases varchar[],
    total_num_cases int
   );

--add columns into the new table
insert into clean.individual_ids 
with ids as (
select 
	nextval('clean.individual_ids_dssg_id_seq'),
    first_name, 
    last_name, 
    dob,
    array_agg(case_num) as cases
from raw_court.dssgname 
group by 
 first_name, last_name, dob
)
select *, array_length(cases,1) from ids;

select  count(distinct dssg_id ) from clean.id_with_cases; 




drop table if exists clean.ordvpost2012;

create table clean.ordvpost2012 as (
    with cases_wzip as (
    with cases as (
    select 
        person_id,
        disp_date,
        statute_ord,
        coalesce (prob_months, 0) as p_months,
        coalesce(prob_years, 0) as p_years,
        case_num,
        sent_exec_code 
    from clean.dispositions
   -- where sent_exec_code = 'SES' or sent_exec_code = 'SIS'
    where disp_date > '2011-12-31'
    ),
    wdescr as (
    select 
        person_id,
        cases.case_num,
        disp_date,
        p_months,
        p_years,
        cf.code,
        cf.code_desc,
        p_months + 12 * p_years as prob_total_months,
        start_dttm::date,
        final_action_dttm::date,
        final_action as outcome,
        statute_ord,
        sent_exec_code 
    from cases
    left join raw_court.dssgc_ordv08102022 ov--raw_court.dssgc_ordv6222022 ov
        on ov.case_num = cases.case_num
    left join raw_court.codes_fixed cf
    on ov.prob_code = cf.code
    where code_type = 'PROBCNDS'
    ),
    w_rank as (
    select 
        wdescr.*,
        row_number() over(
            partition by person_id, case_num, code
            order by final_action_dttm desc,
            case when final_action_dttm is null then 0 else 1 end,
            final_action_dttm desc) as rnk
    from wdescr
    )
    select 
        * 
    from w_rank 
    where rnk = 1
    )
    select 
        wz.*,
        substring(rcda.zipcode, 1, 5) as zipcode, 
        d.race,
        d.sex
    from cases_wzip wz
    left join raw_court.dssgaddress6222022 rcda 
        on rcda.case_num = wz.case_num
    left join raw_court.dssgname d
        on d.case_num = wz.case_num
)



select * from pg_stat_activity where usename = 'meltemo'


select pg_terminate_backend(30801)


drop table if exists clean.ordvpost2012_nonviol;

create table clean.ordvpost2012_nonviol as (
    with cases_wzip as (
    with cases as (
    select 
        person_id,
        disp_date,
        statute_ord,
        coalesce (prob_months, 0) as p_months,
        coalesce(prob_years, 0) as p_years,
        case_num,
        sent_exec_code 
    from clean.dispositions
   -- where sent_exec_code = 'SES' or sent_exec_code = 'SIS'
    where disp_date > '2011-12-31'
    and statute_ord not in (
        '50-157(B)(2)',	--'ASSAULT PUBLIC OPERATOR'
        '50-169',	--'DOMESTIC ASSAULT'
        '50-170(A)',--'ASSAULT SCHOOL'
        '50-169',	--'ASSAULT'
        '50-168',	--'ATTEMPT ASSAULT'
        '50-231(A)(1)',	--'ENDANGR WELFARE OF CHILD'
        '50-242',	--'ABUSE OF A CHILD'
        '50-263',	--'DISCHARGE OF FIREARM'
        '50-232',	--'SEX ABUSE OF CHILD'
        '50-157(C)',--'HURL MISSILE AT PUB TRAN'
        '70-336',	--'FAIL YIELD-EMERGENCY VEH'
        '70-857B4',	--'VIOL CHILD SAFE BELT LAW'
        '70-857B3',	--'FAIL SECURE CHILD UNDR 8'
        '70-857B2',	--'FAIL SECURE CHILD U 40LB'
        '70-857B1',	--'CHILD RESTRAINED UNDER 4'
        '70-856C',	--'SEAT BELTS CHILD 4-16YO'
        '50-261(A)(4)',	--'CARRY WEAPON CHURCH'
        '50-261(A)(5)',	--'CARRY WEAPON SCHOOL'
        '50-9',		--'STALKING'
        '50-244(2)',	--'ALLOW BULLY MINOR'
        '50-244(1)',	--'BULLY CYBER BULLY MINOR'
        '50-193(A)',	--'LEAVING EXPLOSIVE DEVICE'
        '14-37',	--'DISPOSAL OF DEAD ANIMALS'
        '48-34',	--'RAT INFEST RAT HARBORAGE'
        '48-50',	--'CHRONIC NUISANCE'
        '62-89A',	--'ILLEGAL DUMPING'
        '56-544(A)',	--'DANGEROUS BUILD TICKET 1'
        '56-544(A)',	--'DANGEROUS BUILD TICKET 2'
        '56-544(B)',	--'DANGEROUS BUILDINGS'
        '50-109(D)',	--'LANDLORD RETALIATE'
        '50-109(E)',	--'LANDLORD NOT ALLOW TERM'
        '14-31(B)',	--'ANIMAL PUT PERSON FEAR'
        '14-32',	--'CONFINEMENT-DOG IN HEAT'
        '14-37(B)',	--'DISPOSAL OF DEAD ANIMALS'
        '14-47',	--'GUARD DOG'
        '14-60(D)',	--'PIT BULL BREEDING'
        '14-60(B)',	--'SPAYING AND NEUTERING'
        '14-38',	--'ANIMAL MARKETS'
        '14-41',	--'CONFINEMENT ANIMAL BITE'
        '14-46',	--'ANIMAL SHOWS'
        '14-60(G)',	--'PIT BULL SALE-TRANSFER'
        '14-17',	--'INJ TRAP POISON ANIMAL'
        '14-15(A)',	--'SM AN OR FOWL PEN'
        '14-13(A)',	--'VIET POTBELLIED PIGS'
        '14-9A',	--'DANGEROUS ANIMALS'
        '14-9B',	--'PROHIBITED ANIMALS'
        '14-30',	--'EXCESSIVE ANIMAL NOISE'
        '14-15(D)',	--'SM AN OR FOWL AREA'
        '50-174(C)(2)',	--'NUISANCE PARTY'
        '14-15(C)',	--'SM AN OR FOWL WASTE'
        '14-15(E)',	--'SM AN OR FOWL ENCLOSURE'
        '14-16(B)',	--'ABUSE OF ANIMAL'
        '14-15(F)',	--'SM AN OR FOWL NUMBER ROS'
        '14-61B',	--'POLICE SERVICE ANIMAL'
        '14-29',	--'DANGEROUS DOGS'
        '14.16.1',	--'ANIMAL FIGHT'
        '14-16A',	--'ADEQUATE ANIMAL CARE'
        '14-48',	--'ANIMAL LEFT IN VEHICLE'
        '14-16A',	--'ADEQUATE ANIMAL CARE'
        '14-11',	--'ABANDONMENT'
        '14-28A'	--'LIMITATION CATS DOGS'
        )
    ),
    wdescr as (
    select 
        person_id,
        cases.case_num,
        disp_date,
        p_months,
        p_years,
        cf.code,
        cf.code_desc,
        p_months + 12 * p_years as prob_total_months,
        start_dttm::date,
        final_action_dttm::date,
        final_action as outcome,
        statute_ord,
        sent_exec_code 
    from cases
    left join raw_court.dssgc_ordv08102022 ov
        on ov.case_num = cases.case_num
    left join raw_court.codes_fixed cf
    on ov.prob_code = cf.code
    where code_type = 'PROBCNDS'
    ),
    w_rank as (
    select 
        wdescr.*,
        row_number() over(
            partition by person_id, case_num, code
            order by final_action_dttm desc,
            case when final_action_dttm is null then 0 else 1 end,
            final_action_dttm desc) as rnk
    from wdescr
    )
    select 
        * 
    from w_rank 
    where rnk = 1
    )
    select 
        wz.*,
        substring(rcda.zipcode, 1, 5) as zipcode, 
        d.race,
        d.sex
    from cases_wzip wz
    left join raw_court.dssgaddress6222022 rcda 
        on rcda.case_num = wz.case_num
    left join raw_court.dssgname d
        on d.case_num = wz.case_num
)



set role "kcmo-mc-role";


-- drop table clean.individual_ids -- to delete table 


DROP TABLE IF EXISTS clean.individual_ids;

-- create new table with auto increment column as unique ids,
-- a column containing the array of cases associated with that
-- individual, and a column for the total number of cases
create table clean.individual_ids (
    dssg_id  serial,
    first_name varchar, 
    last_name  varchar, 
    dob varchar, 
    cases varchar[],
    total_num_cases int
    );

-- add columns into the new table
insert into clean.individual_ids 
with ids as (
select 
    nextval('clean.individual_ids_dssg_id_seq'),
    first_name, 
    last_name, 
    dob,
    array_agg(case_num) as cases
from raw_court.dssgname 
group by 
 first_name, last_name, dob
)
select *, array_length(cases, 1) from ids;


-- after running the insert query multiple times accidentally, deleted data
-- inside the table but kept the format
-- truncate table clean.individual_ids 

-- creating the second table
DROP TABLE IF EXISTS clean.id_with_cases;

create table clean.id_with_cases as (
select 
    first_name, 
    last_name, 
    dob,
	unnest(cases) as case_num,
	dssg_id 
from clean.individual_ids
);

select count(distinct person_id) from clean.dispositions d

select count(distinct dssg_id) from clean.id_with_cases

select * from clean.individual_ids

select * from clean.dispositions d 
where disp_date is not null order by disp_date





with cases as (
select 
    person_id,
    disp_date,
    coalesce (prob_months, 0) as p_months,
	coalesce(prob_years, 0) as p_years, 
   -- sum(prob_months + 12 * prob_years) as total_prob_months,
    case_num
from clean.dispositions
where sent_exec_code = 'SIS'
and disp_date > '2011-12-31'
),
probs as (
select 
    person_id,
    cases.case_num,
    disp_date,
    p_months, 
    p_years, 
    code_desc, start_dttm,
    final_action_dttm,
    final_action, 
    prob_code
from cases
left join raw_court.dssgc_ordv6222022 ov
on ov.case_num = cases.case_num
left join raw_court.codes_fixed cf 
on ov.prob_code = cf.code
where code_type = 'PROBCNDS'
--and final_action_dttm is not null
)--,
--wch as (
select
	sum(p_months + 12 * p_years), person_id as total_prob_months from probs group by person_id 



select max(disp_date) from clean.dispositions d  

select  sum(prob_months + 12 * prob_years) as total_prob_months from clean.dispositions d group by person_id


select * from clean.dispositions  d  where case_num = 'G00139596-B'

select * from raw_court.dssgc_ordv6222022 do2 where case_num = 'G00139596-B'--'150185145-7'



select * from raw_court.codes_fixed cf  where case_num = 'G00139596-B'

with cases as (
select 
    person_id,
    disp_date,
    coalesce (prob_months, 0) as p_months,
	coalesce(prob_years, 0) as p_years,
    case_num
from clean.dispositions
where sent_exec_code = 'SIS'
and disp_date > '2011-12-31'
and case_num = 'G00139596-B'
)
select 
    person_id,
    cases.case_num,
    disp_date,
    p_months,
    p_years,
    cf.code,
    -- array_agg(distinct code_desc) as code_desc_arr,
    p_months + 12*p_years as prob_total_months,
    start_dttm::date,
    final_action_dttm::date,
    final_action as outcome
    -- array_agg(distinct prob_code) as prob_code_arr
from cases
left join raw_court.dssgc_ordv6222022 ov
     on ov.case_num = cases.case_num
left join raw_court.codes_fixed cf
   on ov.prob_code = cf.code
where code_type = 'PROBCNDS'
--group by cases.person_id,
--    cases.case_num,
--    cases.disp_date, 
--    cases.p_months,
--    cases.p_years,
--    ov.start_dttm,
--    ov.final_action_dttm,
--    ov.final_action

select * from raw_court.codes_fixed cf  where code_type = 'PROBCNDS' and code = 'PROB'

select code_type, code, count(*) as n_appearances from raw_court.codes_fixed cf  where code_type = 'PROBCNDS' group by 1, 2 order by 3 desc


--DROP TABLE IF EXISTS clean.codes_fixed;
--create table clean.codes_fixed as 
--(select distinct code_type, code, code_desc from raw_court.codes_fixed);

select * from raw_court.dssgc_ordv6222022  d where case_num = 'G00139596-B' 

select * from clean.dispositions d2 where case_num = 'G00139596-B'

--DROP TABLE IF EXISTS clean.sescases_post2012;

--create table clean.sescases_post2012 as 
--(
with cases as (
select 
    person_id,
    disp_date,
    statute_ord,
    coalesce (prob_months, 0) as p_months,
	coalesce(prob_years, 0) as p_years,
    case_num
from clean.dispositions
where sent_exec_code = 'SES'
and disp_date > '2011-12-31'
--and case_num = 'G00139596-B'
),
wdescr as (
select 
    person_id,
    cases.case_num,
    disp_date,
    p_months,
    p_years,
    cf.code,
    cf.code_desc,
    p_months + 12 * p_years as prob_total_months,
    start_dttm::date,
    final_action_dttm::date,
    final_action as outcome,
    statute_ord 
from cases
left join raw_court.dssgc_ordv6222022 ov
     on ov.case_num = cases.case_num
left join raw_court.codes_fixed cf
   on ov.prob_code = cf.code
where code_type = 'PROBCNDS'
),
w_rank as (
select 
	wdescr.*,
	row_number() over(
    	partition by person_id, case_num, code
        order by final_action_dttm desc,
        case when final_action_dttm is null then 0 else 1 end,
        final_action_dttm desc) as rnk
from wdescr
)
select 
* from w_rank where rnk = 1
--);

select * from pg_stat_activity where usename = 'meltemo' --where state = 'idle';

select count(*) from clean.siscases_post2012;

select pg_terminate_backend(16098)


select person_id, disp_date, count(*) from clean.dispositions d group by person_id, disp_date
order by count desc



select case_num, disp_date, chrg_desc, statute_ord  from clean.dispositions d  where person_id = '403847'
group by case_num, disp_date, chrg_desc, statute_ord 


select count (distinct case_num) from clean.dispositions d  where person_id = '403847'

select count(*) from pipeline.cohort c left join clean.dispositions d
on c.person_id = d.person_id
    and c.cohort_date = d.disp_date

 
with ct as (select person_id, count(*) from clean.dispositions group by person_id)
select count(*) from ct


--explain 
with data as(
select 
    c.*, 
    d.statute_ord 
from pipeline.cohort c
left join clean.dispositions d
on c.person_id = d.person_id
)
select 
    statute_ord,
    count(*)
from data
group by statute_ord
order by count desc;


explain select 1;

--reassign owned by meltemo to "kcmo-mc-role";

select * from clean.siscases_post2012 sp where case_num = 'G00128800-5'

select count(distinct person_id) from clean.siscases_post2012 d 

select count(distinct case_num) from clean.siscases_post2012 d 


select count(distinct case_num) from clean.siscases_post2012 d  where code = 'PROB'


select 
	case_num,
	bool_or(code = 'PROB') 
from clean.siscases_post2012 d 
group by case_num

select true 

select * from raw_court.dssgc_ordv6222022 sp where case_num = '111091287-7'


select 
sp.*, nm.race, nm.sex
from clean.siscases_post2012 sp 
left join raw_court.dssgname nm
on nm.case_num = sp.case_num 

substring(zipcode, 1, 5) as zip 


--create table clean.sis_ses_2012 as (
with cases_wzip as (
with cases as (
select 
    person_id,
    disp_date,
    statute_ord,
    coalesce (prob_months, 0) as p_months,
	coalesce(prob_years, 0) as p_years,
    case_num,
    sent_exec_code 
from clean.dispositions
where sent_exec_code = 'SES' or sent_exec_code = 'SIS'
and disp_date > '2011-12-31'
--and case_num = 'G00139596-B'
),
wdescr as (
select 
    person_id,
    cases.case_num,
    disp_date,
    p_months,
    p_years,
    cf.code,
    cf.code_desc,
    p_months + 12 * p_years as prob_total_months,
    start_dttm::date,
    final_action_dttm::date,
    final_action as outcome,
    statute_ord,
    sent_exec_code 
from cases
left join raw_court.dssgc_ordv6222022 ov
     on ov.case_num = cases.case_num
left join raw_court.codes_fixed cf
   on ov.prob_code = cf.code
where code_type = 'PROBCNDS'
),
w_rank as (
select 
	wdescr.*,
	row_number() over(
    	partition by person_id, case_num, code
        order by final_action_dttm desc,
        case when final_action_dttm is null then 0 else 1 end,
        final_action_dttm desc) as rnk
from wdescr
)
select 
* from w_rank where rnk = 1
)
select 
	wz.*,
	substring(rcda.zipcode, 1, 5) as zipcode, 
	d.race,
	d.sex
from cases_wzip wz
left join raw_court.dssgaddress6222022 rcda 
on rcda.case_num=wz.case_num
left join raw_court.dssgname d
on d.case_num=wz.case_num
--)

select * from clean.sis_ses_2012 


select 
count(distinct case_num),
sex, sent_exec_code
from clean.sis_ses_2012 
group by sex, sent_exec_code

 

select 
count(distinct case_num),
race, sent_exec_code
from clean.sis_ses_2012 
group by race, sent_exec_code


SELECT * FROM raw_court.dssgc_ordv6222022 do2 WHERE case_num = '111091287-7'


select count(*), sum(label) from pipeline.labels  where cohort_date between '2021-03-15'::date and '2021-04-15'::date 


select * from pipeline.cohort c 

select 
statute_ord, chrg_desc, count(*)
from clean.dispositions d 
group by statute_ord, chrg_desc
order by count desc


with c as (select 
statute_ord, chrg_desc, count(*) 
from clean.dispositions d 
group by statute_ord, chrg_desc 
order by count desc)
select sum(count) from c


with non_viol as (
    select
        case_num, sent_exec_code, disp_date, person_id
    from clean.dispositions d
    where
        sent_exec_code = 'SIS'
        and
        (disp_date > '2016-07-07')
        and
        (disp_date <= '2016-10-05')
    and statute_ord not in (
        '50-157(B)(2)', --'ASSAULT PUBLIC OPERATOR'
        '50-169',   --'DOMESTIC ASSAULT'
        '50-170(A)',--'ASSAULT SCHOOL'
        '50-169',   --'ASSAULT'
        '50-168',   --'ATTEMPT ASSAULT'
        '50-231(A)(1)', --'ENDANGR WELFARE OF CHILD'
        '50-242',   --'ABUSE OF A CHILD'
        '50-263',   --'DISCHARGE OF FIREARM'
        '50-232',   --'SEX ABUSE OF CHILD'
        '50-157(C)',--'HURL MISSILE AT PUB TRAN'
        '70-336',   --'FAIL YIELD-EMERGENCY VEH'
        '70-857B4', --'VIOL CHILD SAFE BELT LAW'
        '70-857B3', --'FAIL SECURE CHILD UNDR 8'
        '70-857B2', --'FAIL SECURE CHILD U 40LB'
        '70-857B1', --'CHILD RESTRAINED UNDER 4'
        '70-856C',  --'SEAT BELTS CHILD 4-16YO'
        '50-261(A)(4)', --'CARRY WEAPON CHURCH'
        '50-261(A)(5)', --'CARRY WEAPON SCHOOL'
        '50-9',     --'STALKING'
        '50-244(2)',    --'ALLOW BULLY MINOR'
        '50-244(1)',    --'BULLY CYBER BULLY MINOR'
        '50-193(A)',    --'LEAVING EXPLOSIVE DEVICE'
        '14-37',    --'DISPOSAL OF DEAD ANIMALS'
        '48-34',    --'RAT INFEST RAT HARBORAGE'
        '48-50',    --'CHRONIC NUISANCE'
        '62-89A',   --'ILLEGAL DUMPING'
        '56-544(A)',    --'DANGEROUS BUILD TICKET 1'
        '56-544(A)',    --'DANGEROUS BUILD TICKET 2'
        '56-544(B)',    --'DANGEROUS BUILDINGS'
        '50-109(D)',    --'LANDLORD RETALIATE'
        '50-109(E)',    --'LANDLORD NOT ALLOW TERM'
        '14-31(B)', --'ANIMAL PUT PERSON FEAR'
        '14-32',    --'CONFINEMENT-DOG IN HEAT'
        '14-37(B)', --'DISPOSAL OF DEAD ANIMALS'
        '14-47',    --'GUARD DOG'
        '14-60(D)', --'PIT BULL BREEDING'
        '14-60(B)', --'SPAYING AND NEUTERING'
        '14-38',    --'ANIMAL MARKETS'
        '14-41',    --'CONFINEMENT ANIMAL BITE'
        '14-46',    --'ANIMAL SHOWS'
        '14-60(G)', --'PIT BULL SALE-TRANSFER'
        '14-17',    --'INJ TRAP POISON ANIMAL'
        '14-15(A)', --'SM AN OR FOWL PEN'
        '14-13(A)', --'VIET POTBELLIED PIGS'
        '14-9A',    --'DANGEROUS ANIMALS'
        '14-9B',    --'PROHIBITED ANIMALS'
        '14-30',    --'EXCESSIVE ANIMAL NOISE'
        '14-15(D)', --'SM AN OR FOWL AREA'
        '50-174(C)(2)', --'NUISANCE PARTY'
        '14-15(C)', --'SM AN OR FOWL WASTE'
        '14-15(E)', --'SM AN OR FOWL ENCLOSURE'
        '14-16(B)', --'ABUSE OF ANIMAL'
        '14-15(F)', --'SM AN OR FOWL NUMBER ROS'
        '14-61B',   --'POLICE SERVICE ANIMAL'
        '14-29',    --'DANGEROUS DOGS'
        '14.16.1',  --'ANIMAL FIGHT'
        '14-16A',   --'ADEQUATE ANIMAL CARE'
        '14-48',    --'ANIMAL LEFT IN VEHICLE'
        '14-16A',   --'ADEQUATE ANIMAL CARE'
        '14-11',    --'ABANDONMENT'
        '14-28A'    --'LIMITATION CATS DOGS'
        )
)
select person_id,
       disp_date as cohort_date
    from non_viol
    group by person_id, cohort_date
    order by cohort_date, person_id ASC

with dat as(
select 
   person_id, disp_date
from  clean.dispositions d
left join raw_court.dssgc_ordv6222022 do2 
on d.case_num = do2.case_num 
group by person_id, disp_date
)
select 
    --statute_ord, 
    count(*)
from dat
--group by statute_ord
--order by count desc;



select value from modeling.model_metrics where metric_name = 'AUC'



select distinct
	msm.model_set_id, 
	md.model_id, 
	md.train_end_date, 
	metric_name, 
	metric_param, 
	value
from modeling.model_set_metadata msm 
left join modeling.model_metadata md
	on msm.model_set_id = md.model_set_id
left join modeling.model_metrics mm 
	on mm.model_id = md.model_id 
where mm.metric_name = 'precision'
	and metric_param = 10

select model_id, count(*) 
from modeling.model_metrics
where metric_name = 'precision'
and metric_param = 10
group by model_id order by count(*) DESC

select model_set_id, count(*)
from modeling.model_set_metadata msm 
group by model_set_id 


select * from modeling.model_set_metadata msm 
where model_set_id = '40b5fec1fef010ca842f948b64e9ad0d'

select 
	count(distinct person_id) 
from clean.prob_terms pt 
where (start_dttm::date < '2020-01-01' 
	and start_dttm::date > '2018-12-31')
or where ( _dttm::date > '2018-12-31')

select * from modeling.model_metadata mm 
where model_set_id = '0415a887972d32bc13c36786985a2287'


select * from modeling.model_metadata  mm 
where model_set_id = 'bcc6cef0259bff478da4bd3b0badcebe'



select * from clean.individual_ids except select * from clean_test.individual_ids;

select * from clean.violations except select * from clean_test.violations;

select * from clean.id_with_cases except select * from clean_test.id_with_cases;