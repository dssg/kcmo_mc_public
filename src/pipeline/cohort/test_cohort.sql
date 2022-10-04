
-- Selects Person ID and Disposition Date pairs where 
-- no case in the disposition date is in the excluded list
-- and the dispositions are between the specified start and end dates

with person_disp_pairs_exclude as (
	select 
		person_id,
		date(disp_date) as disp_date, 
		ARRAY_AGG(case_num) AS cases,
		BOOL_OR(statute_ord in (
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
		)) as exclude
	from clean.dispositions 
	where 
		sent_exec_code = 'SIS'
		and
		(DATE(disp_date) > '{start_date}')
		and 
		(DATE(disp_date) <= '{end_date}')
GROUP BY person_id, DATE(disp_date))
select person_id, 
	   DATE(disp_date) as cohort_date
	from person_disp_pairs_exclude 
	where exclude is false 
	order by cohort_date, person_id ASC

-- COUNTS of (person_id, cohort_date) pairs
-- without using exclude flag: 52,936
-- when we only drop people who had all excluded charges: 47,010
-- when we exclude people who had dispositions with any excluded charge: 45,334

-- COUNTS of num_cases 
-- without exclude flag: 108,725
-- with exclude flag: 66,259
