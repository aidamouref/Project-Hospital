use healthcare;

select * from admission;


select gender, medical_cond, count(medical_cond) as al from patient p join bloodtype bt on p.blood_id=bt.blood_id
join medicalcond mc on p.medcond_id=mc.medcond_id
group by gender, medical_cond
order by al desc;



-- 1. which one is the most common condition?
select count(p.medcond_id) as totalcount, medical_cond from medicalcond mc join patient p on p.medcond_id=mc.medcond_id
group by p.medcond_id having totalcount=(select max(totalcount) from (select count(p.medcond_id) as totalcount, medical_cond from medicalcond mc join patient p on p.medcond_id=mc.medcond_id
group by p.medcond_id)sub1);

select medical_cond from medicalcond;

-- 2.select mean age of patients with each medical condition;
select round(avg(age),2) as meanage, medical_cond from patient p join medicalcond mc on p.medcond_id=mc.medcond_id
group by medical_cond
order by meanage desc;

select round(avg(age),2) as meanage, admission_type from patient p join admission a on p.pat_id=a.pat_id
join admission_type aty on a.admtype_id=aty.admtype_id
group by admission_type
order by meanage desc;

-- 3. is the inpatient stay length related to test results?
select test_result, datediff(disc_date,adm_date) as lengthstay from admission ad join test_results tr on ad.testres_id=tr.testres_id
group by test_result, lengthstay
order by lengthstay desc;

select *, date_format(convert(adm_date,date), '%M-%d-%Y') as new from admission;


-- which are the 5 hospitals that have more patients with 0+ blood_type?
select count(blood_type), hospital from patient p join medicalcond mc on p.medcond_id=mc.medcond_id 
join admission ad on p.pat_id=ad.pat_id join hospital h on ad.hospital_id=h.hospital_id
join bloodtype bt on p.blood_id=bt.blood_id
group by hospital, blood_type
having blood_type='o-' -- this is the universal blood type so good for transfusions
order by count(blood_type) desc
limit 5;


-- Name of the patients that spent between 1000 and 2000 dollars per stay
select name, billing_amount, adm_date, disc_date from admission a join patient p on a.pat_id=p.pat_id
where billing_amount between 1000 and 2000
order by billing_amount desc;


select name, billing_amount, adm_date, disc_date,
CASE
    WHEN billing_amount >20000 THEN 'medium bill'
    WHEN billing_amount >40000 THEN 'high bill'
    ELSE 'low bill'
END as newvar
from admission a join patient p on a.pat_id=p.pat_id;


-- window function
-- select the maximum amount spent per provider
select hospital_id,max(billing_amount) over (partition by hospital_id) as hosp_amount
from admission
where billing_amount < 2000
order by hosp_amount desc;

-- list all the patients who stayed in a room number 463
select name from patient where pat_id in (select pat_id from admission where room_number=463);


