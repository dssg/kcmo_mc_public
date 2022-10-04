
-- before creating schema, set the team role so that the schema owner is that role
set role "kcmo-mc-role";

-- first create new schema
create schema if not exists clean;

-- drop table clean.individual_ids -- to delete table

DROP TABLE IF EXISTS clean.individual_ids;

-- create new table with auto increment column as unique ids,
-- a column containing the array of cases associated with that
-- individual, and a column for the total number of cases
create table clean.individual_ids (
    person_id  serial,
    first_name varchar,
    last_name  varchar,
    dob date,
    cases varchar[],
    total_num_cases int
    );

-- add columns into the new table
insert into clean.individual_ids
with person_info as (
    SELECT
    first_name::varchar,
    last_name::varchar,
    dob::date,
    case_num::varchar
    FROM raw_court.dssgname
    UNION SELECT
    first_name::varchar,
    last_name::varchar,
    dob::date,
    case_num::varchar
    FROM raw_court.dssgnamenoprob_a
    UNION SELECT
    first_name::varchar,
    last_name::varchar,
    dob::date,
    case_num::varchar
    FROM raw_court.dssgnamenoprob_b
),
ids as (
select
    nextval('clean.individual_ids_person_id_seq'),
    first_name,
    last_name,
    dob::date,
    array_agg(case_num) as cases
from person_info
group by
 first_name, last_name, dob
)
select *, array_length(cases, 1) from ids;

create index on clean.individual_ids(person_id);


-- after running the insert query multiple times accidentally, deleted data
-- inside the table but kept the format
-- truncate table clean.individual_ids

-- creating the second table
DROP TABLE IF EXISTS clean.id_with_cases;

create table clean.id_with_cases as (
select
    unnest(cases) as case_num,
	person_id
from clean.individual_ids
);

create index on clean.id_with_cases (person_id);

-- CLEAN VIOLATIONS TABLE

DROP TABLE IF EXISTS clean.violations;

-- Create a clean violations table
CREATE TABLE clean.violations AS (
WITH caserec AS(
   SELECT
   person_id::int,
   case_num::varchar,
   rec_id::varchar,
   viol_dttm::timestamp,
   orig_dkt_crtrm::varchar,
   orig_dkt_dttm::timestamp,
   traffic_viol_ind::varchar,
   tkt_type::varchar,
   school_zone_flag::varchar,
   domestic_violence_flag::varchar
   FROM
   raw_court.caserec_fixed
   LEFT JOIN clean.id_with_cases USING (case_num)
   UNION SELECT
   person_id::int,
   case_num::varchar,
   rec_id::varchar,
   viol_dttm::timestamp,
   orig_dkt_crtrm::varchar,
   orig_dkt_dttm::timestamp,
   traffic_viol_ind::varchar,
   tkt_type::varchar,
   school_zone_flag::varchar,
   domestic_violence_flag::varchar
   FROM
   raw_court.caserec_b_fixed
   LEFT JOIN clean.id_with_cases USING (case_num)
   )
   select * from caserec
);

create index on clean.violations(person_id);
create index on clean.violations(case_num);
create index on clean.violations(viol_dttm);


-- CLEAN DISPOSITIONS TABLE
-- this table includes only probation dispositions
DROP TABLE IF EXISTS clean.dispositions;

CREATE TABLE clean.dispositions as (
    WITH charge_dispositions AS (
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
            'raw_court.dssgchargedisp_fixed' AS disposition_source_table
        FROM raw_court.dssgchargedisp_fixed
        UNION
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
            'raw_court.dssgchargedispnoprob_fixed' AS disposition_source_table
        FROM raw_court.dssgchargedispnoprob_fixed
    ),
    charges AS (
        SELECT
            case_num::varchar,
            statute_ord::varchar,
            CASE
                WHEN chrg_desc = 'TRESPASS AT VACANT PROP' THEN 'TRESSPASS AT VACANT PROP'
                WHEN chrg_desc = 'TRESPASS VACANT BLDG' THEN 'TRESPAS VACANT BLDG PROP'
                WHEN chrg_desc = 'DART TRESPASS' THEN 'TRESPASS GENERALLY'
                WHEN chrg_desc = 'VEH TURN LEFT-FAIL T YLD' THEN 'VEH ON LEFT FAILD T YLD'
                ELSE chrg_desc
            END AS chrg_desc
            --chrg_desc::varchar
        FROM raw_court.dssgcharge8052022_part_1_fixed
        UNION
        SELECT
            case_num::varchar,
            statute_ord::varchar,
            CASE
                WHEN chrg_desc = 'TRESPASS AT VACANT PROP' THEN 'TRESSPASS AT VACANT PROP'
                WHEN chrg_desc = 'TRESPASS VACANT BLDG' THEN 'TRESPAS VACANT BLDG PROP'
                WHEN chrg_desc = 'DART TRESPASS' THEN 'TRESPASS GENERALLY'
                WHEN chrg_desc = 'VEH TURN LEFT-FAIL T YLD' THEN 'VEH ON LEFT FAILD T YLD'
                ELSE chrg_desc
            END AS chrg_desc
            --chrg_desc::varchar
        FROM raw_court.dssgcharge8052022_part_2_fixed
        UNION
        SELECT
            case_num::varchar,
            statute_ord::varchar,
            CASE
                WHEN chrg_desc = 'TRESPASS AT VACANT PROP' THEN 'TRESSPASS AT VACANT PROP'
                WHEN chrg_desc = 'TRESPASS VACANT BLDG' THEN 'TRESPAS VACANT BLDG PROP'
                WHEN chrg_desc = 'DART TRESPASS' THEN 'TRESPASS GENERALLY'
                WHEN chrg_desc = 'VEH TURN LEFT-FAIL T YLD' THEN 'VEH ON LEFT FAILD T YLD'
                ELSE chrg_desc
            END AS chrg_desc
            --chrg_desc::varchar
        FROM raw_court.dssgcharge6222022
    )
    SELECT
        charge_dispositions.case_num::varchar,
        charge_dispositions.disp_date::timestamp,
        charge_dispositions.plea::varchar,
        charge_dispositions.disp_crtrm::varchar,
        charge_dispositions.disp_dkt_type::varchar,
        charge_dispositions.sent_exec_code::varchar,
        charge_dispositions.jail_days::int,
        charge_dispositions.community_serv::int,
        charge_dispositions.judg_id::varchar,
        charge_dispositions.prob_days::int,
        charge_dispositions.prob_months::int,
        charge_dispositions.prob_years::int,
        charge_dispositions.sent_jail_days::int,
        charge_dispositions.sent_community_serv::int,
        charge_dispositions.prob_supv_fee_code::varchar,
        charges.chrg_desc::varchar,
        CASE
            WHEN charges.chrg_desc = 'POSSESSION OF MARIJUANA' THEN '50-10'
            WHEN charges.chrg_desc = 'POSS NARCOTIC PARAPHENAL' THEN '50-201'
            WHEN charges.chrg_desc = 'TRESSPASS AT VACANT PROP' THEN  '50-128(C)'
            WHEN charges.chrg_desc = 'PROSTITUTION PATRONIZE' THEN '50-72'
            WHEN charges.chrg_desc = 'OPER MV WITH BAC OF 08' THEN '70-304'
            WHEN charges.chrg_desc = 'OPER MTR VEH UNDR INFLNC' THEN '70-302'
            WHEN charges.chrg_desc = 'OBSTRUCTING AN OFFICER' THEN '50-44'
            WHEN charges.chrg_desc = 'RANK WEEDS NOXIOUS PLANT' THEN '48-30'
            WHEN charges.chrg_desc = 'RECEIVE STOLEN PROPERTY' THEN '50-111.5(A)'
            WHEN charges.chrg_desc = 'MAL DESTRUCTION OF PROP' THEN '50-121'
            WHEN charges.chrg_desc = 'PUBLIC NUISANCE' THEN '14-31(C)'
            WHEN charges.chrg_desc = 'NO INSURANCE' THEN '50-270(A)'
            WHEN charges.chrg_desc = 'SOLICIT IMMORAL PURPOSE' THEN '50-72'
            WHEN charges.chrg_desc = 'SIMPLE ASLT NOT FIGHT' THEN '50-169'
            WHEN charges.chrg_desc = 'RESISTING OFFICER' THEN '50-44'
            WHEN charges.chrg_desc = 'POSSESSION OF WEAPON' THEN '50-261'
            WHEN charges.chrg_desc = 'CARRYING WEAPONS' THEN '50-261'
            WHEN charges.chrg_desc = 'TRASH PUB PRIVATE PROP' THEN '48-25'
            WHEN charges.chrg_desc = 'TOW VEH AND ACC SCENES' THEN '70-273'
            WHEN charges.chrg_desc = 'WRG SIDE OF DIV HWY-STR' THEN '70-400'
            WHEN charges.chrg_desc = 'WRECK DAM DEM DIS VEHICL' THEN '48-27'
            WHEN charges.chrg_desc = 'VIOL FLASHING SIGNALS' THEN '70-955'
            WHEN charges.chrg_desc = 'VIOL OF CITY ANIMAL ORD' THEN '14-16A'
            WHEN charges.chrg_desc = 'LARCENY-UNDER $50' THEN '50-106'
            WHEN charges.chrg_desc = 'VIOLATION OF ORD OF PROT' THEN '50-47'
            WHEN charges.chrg_desc = 'VEH ON LEFT FAILD T YLD' THEN '70-331(A)'
            WHEN charges.chrg_desc = 'CHILD ENDANGERMENT' THEN '50-231'
            WHEN charges.chrg_desc = 'DANGEROUS DOGS' THEN '14-29'
            WHEN charges.chrg_desc = 'UNLAWFUL USE OF WEAPON' THEN '50-261'
            WHEN charges.chrg_desc = 'TRESPASS-UNLAWFUL ENTRY' THEN '50-102'
            WHEN charges.chrg_desc = 'TRESPAS VACANT BLDG PROP' THEN '50-128(B)'
            WHEN charges.chrg_desc = 'TRESPASS VACANT BLDG' THEN '50-128(B)'
            WHEN charges.chrg_desc = 'LARCENY ATTEMPT' THEN '50-106'
            WHEN charges.chrg_desc = 'LARCENY-$50 TO $199' THEN '50-106'
            WHEN charges.chrg_desc = 'LARCENY-$200 AND OVER' THEN '50-106'
            WHEN charges.chrg_desc = 'TRESPASS GENERALLY' THEN '50-102'
            ELSE charges.statute_ord
        END AS statute_ord,
        id_with_cases.person_id::int
    FROM charge_dispositions
    LEFT JOIN charges USING (case_num)
    LEFT JOIN clean.id_with_cases USING (case_num)
);

create index on clean.dispositions(case_num);
create index on clean.dispositions(person_id);
create index on clean.dispositions(disp_date);

-- DEMOGRAPHICS TABLE
DROP TABLE IF EXISTS clean.demographics;

CREATE TABLE clean.demographics as (
with demographics as(
SELECT
	case_num::varchar,
	dob::date,
	age::int,
	race::varchar,
	sex::varchar
from raw_court.dssgname
union SELECT
case_num::varchar,
	dob::date,
	age::int,
	race::varchar,
	sex::varchar
from raw_court.dssgnamenoprob_a
union select
case_num::varchar,
	dob::date,
	age::int,
	race::varchar,
	sex::varchar
from raw_court.dssgnamenoprob_b
)
select id.person_id,
	dm.case_num,
	dm.dob,
	dm.age,
	race,
	sex,
	date_part('year', AGE(d.disp_date::date, dm.dob::date)) as age_at_disp,
	date_part('year', AGE(v.viol_dttm::date, dm.dob::date)) as age_at_viol
FROM demographics dm
LEFT JOIN
clean.id_with_cases id USING (case_num)
LEFT JOIN
clean.dispositions d USING (case_num)
left join
clean.violations v USING (case_num)
ORDER BY person_id
);

create index on clean.demographics(case_num);
create index on clean.demographics(person_id);

DROP TABLE IF EXISTS clean.codes_fixed;
create table clean.codes_fixed as (
    select distinct
        code_type::varchar,
        code::varchar,
        code_desc::varchar
    from raw_court.codes_fixed
);

DROP TABLE IF EXISTS clean.all_cases;
CREATE TABLE clean.all_cases AS (
    WITH dssgname AS (
        SELECT
            case_num,
            count(*) AS num_records
        FROM raw_court.dssgname
        GROUP BY case_num
    ),
    dssgnamenoprob_a AS (
        SELECT
            case_num,
            count(*) AS num_records
        FROM raw_court.dssgnamenoprob_a
        GROUP BY case_num
    ),
    dssgnamenoprob_b AS (
        SELECT
            case_num,
            count(*) AS num_records
        FROM raw_court.dssgnamenoprob_b
        GROUP BY case_num
    ),
    caserec_fixed AS (
        SELECT
            case_num,
            count(*) AS num_records
        FROM raw_court.caserec_fixed
        GROUP BY case_num
    ),
    caserec_b_fixed AS (
        SELECT
            case_num,
            count(*) AS num_records
        FROM raw_court.caserec_b_fixed
        GROUP BY case_num
    ),
    dssgchargedisp_fixed AS (
        SELECT
            case_num,
            count(*) AS num_records
        FROM raw_court.dssgchargedisp_fixed
        GROUP BY case_num
    ),
    dssgchargedispnoprob_fixed AS (
        SELECT
            case_num,
            count(*) AS num_records
        FROM raw_court.dssgchargedispnoprob_fixed
        GROUP BY case_num
    ),
    dssgcharge8052022_part_1_fixed AS (
        SELECT
            case_num,
            count(*) AS num_records
        FROM raw_court.dssgcharge8052022_part_1_fixed
        GROUP BY case_num
    ),
    dssgcharge8052022_part_2_fixed AS (
        SELECT
            case_num,
            count(*) AS num_records
        FROM raw_court.dssgcharge8052022_part_2_fixed
        GROUP BY case_num
    ),
    dssgcharge6222022 AS (
        SELECT
            case_num,
            count(*) AS num_records
        FROM raw_court.dssgcharge6222022
        GROUP BY case_num
    ),
    all_counts AS (
        SELECT
            case_num,
            COALESCE(dssgname.num_records, 0) AS num_records_in_dssgname,
            COALESCE(dssgnamenoprob_a.num_records, 0) AS num_records_in_dssgnamenoprob_a,
            COALESCE(dssgnamenoprob_b.num_records, 0) AS num_records_in_dssgnamenoprob_b,
            COALESCE(caserec_fixed.num_records, 0) AS num_records_in_caserec_fixed,
            COALESCE(caserec_b_fixed.num_records, 0) AS num_records_in_caserec_b_fixed,
            COALESCE(dssgchargedisp_fixed.num_records, 0) AS num_records_in_dssgchargedisp_fixed,
            COALESCE(dssgchargedispnoprob_fixed.num_records, 0) AS num_records_in_dssgchargedispnoprob_fixed,
            COALESCE(dssgcharge8052022_part_1_fixed.num_records, 0) AS num_records_in_dssgcharge8052022_part_1_fixed,
            COALESCE(dssgcharge8052022_part_2_fixed.num_records, 0) AS num_records_in_dssgcharge8052022_part_2_fixed,
            COALESCE(dssgcharge6222022.num_records, 0) AS num_records_in_dssgcharge6222022
        FROM dssgname
        FULL JOIN dssgnamenoprob_a USING (case_num)
        FULL JOIN dssgnamenoprob_b USING (case_num)
        FULL JOIN caserec_fixed USING (case_num)
        FULL JOIN caserec_b_fixed USING (case_num)
        FULL JOIN dssgchargedisp_fixed USING (case_num)
        FULL JOIN dssgchargedispnoprob_fixed USING (case_num)
        FULL JOIN dssgcharge8052022_part_1_fixed USING (case_num)
        FULL JOIN dssgcharge8052022_part_2_fixed USING (case_num)
        FULL JOIN dssgcharge6222022 USING (case_num)
    )
    SELECT
        case_num,
        num_records_in_dssgname,
        num_records_in_dssgnamenoprob_a,
        num_records_in_dssgnamenoprob_b,
        num_records_in_caserec_fixed,
        num_records_in_caserec_b_fixed,
        num_records_in_dssgchargedisp_fixed,
        num_records_in_dssgchargedispnoprob_fixed,
        num_records_in_dssgcharge8052022_part_1_fixed,
        num_records_in_dssgcharge8052022_part_2_fixed,
        num_records_in_dssgcharge6222022,
        (
            num_records_in_dssgname > 0
            OR num_records_in_dssgnamenoprob_a > 0
            OR num_records_in_dssgnamenoprob_b > 0
        ) AS in_a_names_table,
        (
            num_records_in_caserec_fixed > 0
            OR num_records_in_caserec_b_fixed > 0
        ) AS in_a_records_table,
        (
            num_records_in_dssgchargedisp_fixed > 0
            OR num_records_in_dssgchargedispnoprob_fixed > 0
        ) AS in_a_dispositions_table,
        (
            num_records_in_dssgcharge8052022_part_1_fixed > 0
            OR num_records_in_dssgcharge8052022_part_2_fixed > 0
            OR num_records_in_dssgcharge6222022 > 0
        ) AS in_a_charges_table,
        (
            num_records_in_dssgname > 0
            OR num_records_in_dssgchargedisp_fixed > 0
            OR num_records_in_dssgcharge6222022 > 0
        ) AS in_a_probations_file,
        (
            num_records_in_dssgnamenoprob_a > 0
            OR num_records_in_dssgnamenoprob_b > 0
            OR num_records_in_dssgchargedispnoprob_fixed > 0
            OR num_records_in_dssgcharge8052022_part_1_fixed > 0
            OR num_records_in_dssgcharge8052022_part_2_fixed > 0
        ) AS in_a_no_probations_file
    FROM all_counts
);
CREATE INDEX ON clean.all_cases (case_num);
