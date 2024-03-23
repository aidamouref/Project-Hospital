use healthcare;

-- Condition prevalence per gender
select gender as Gender, medical_cond as Medical_condition, count(medical_cond) as N from patient p join bloodtype bt on p.blood_id=bt.blood_id
join medicalcond mc on p.medcond_id=mc.medcond_id
group by gender, medical_cond
order by Medical_condition desc;


-- Most prevalent condition
select medical_cond as Medical_condition, count(p.medcond_id) as N  from medicalcond mc join patient p on p.medcond_id=mc.medcond_id
group by p.medcond_id having N=(select max(N) from (select count(p.medcond_id) as N, medical_cond from medicalcond mc 
join patient p on p.medcond_id=mc.medcond_id
group by p.medcond_id)sub1);

-- Mean age of patients by medical condition;
select medical_cond as Medical_condition, round(avg(age),2) as MeanAge from patient p join medicalcond mc on p.medcond_id=mc.medcond_id
group by Medical_condition
order by MeanAge desc;

-- Mean age of patients by admission type
select admission_type as AdmissionType, round(avg(age),2) as MeanAge  from patient p join admission a on p.pat_id=a.pat_id
join admission_type aty on a.admtype_id=aty.admtype_id
group by AdmissionType
order by MeanAge desc;

-- Is the inpatient stay length related to test results?
select test_result as TestResult, datediff(disc_date,adm_date) as LengthStay from admission ad join test_results tr on ad.testres_id=tr.testres_id
group by TestResult, LengthStay
order by LengthStay desc
limit 6;

-- Which is the month with more admissions?
select date_format(convert(adm_date,date), '%M') as Mes, count(*) as N  from admission
group by Mes
order by N desc;

-- Which are the 5 hospitals that have more patients with 0- blood_type?
select hospital as Hospital, count(blood_type) as N_BloodType  from patient p join medicalcond mc on p.medcond_id=mc.medcond_id 
join admission ad on p.pat_id=ad.pat_id join hospital h on ad.hospital_id=h.hospital_id
join bloodtype bt on p.blood_id=bt.blood_id
group by Hospital, blood_type
having blood_type='o-' -- this is the universal blood type so good for transfusions
order by N_BloodType desc
limit 5;

-- Name of the patients that spent between 1000 and 2000 dollars per stay

select p.pat_id, name as Patient, billing_amount as $ from admission a join patient p on a.pat_id=p.pat_id
where billing_amount between 0 and 800
order by $ desc;

-- Clasify patients who paid a cheap/medium/expensive bill
alter table admission
add column pat_nation VARCHAR(50) default null after hospital_id;

UPDATE admission 
SET 
    pat_nation = 'American'
WHERE
    adm_id > 0;

select * from admission; 


select billing_amount as $,
CASE
    WHEN billing_amount >20000 THEN 'Medium bill'
    WHEN billing_amount >40000 THEN 'High bill'
    ELSE 'Low bill'
END as Bill_Type
from admission a join patient p on a.pat_id=p.pat_id
limit 5;

-- window function
-- select the maximum amount spent per hospital
select hospital_id as Hospital, max(billing_amount) over (partition by hospital_id) as MaxBill$
from admission
where billing_amount < 2000
order by MaxBill$
limit 5;

-- list 3 patients who stayed in a room number 463
select name as Patient from patient where pat_id in (select pat_id from admission where room_number=463)
limit 3;
