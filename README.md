# PROJECT II: IMPLEMENTING A NEW HOSPITAL IN THE USA HEALTHCARE SYSTEM 

![hospital.jpg](Images/hospital.jpg)

## INTRO

As a CEO of different hospitals spread across the States I am planning to design and implement a new hospital specialised in some of the most prevalent and resource-demanding health conditions:

* Diabetes
* Cancer
* Hyperthesion
* Asthma
* Arthritis
* Obesity

As a result, a market revision is needed in order to find the best conditions (geographical, population, disease prevalence, etc.) to place the hospital.

## OBJECTIVES

* Check the prevalence of each condition across gender and age and which one is the most common disease
* Study the different admission types. Is there any relation to the age of patients?
* Study the effects of test results. Are they related with the length of stay?
* Month of admission
* Study other hospitals. How many patients do they have of 0- blood type?
* Health expenditure in figures. Who are the patients who spent less amount of money (i.e. 1000$-2000$) in hospital stays? 
* Classify the patients where they had a cheap/medium/expensive bill after being discharged from hospital.
* Which patients stayed in the V.I.P. room (i.e. room 463)

## METHODOLOGY

* Database with 10,000 patient records from different USA hospitals (Kaggle)
* No missing data, no duplicates considered
* Data on admissions, bloodtype, medical conditions, test results, age, hospitals and doctors
* Brief EDA: unique, nunique, valuecount, summarize, rename variables
* Homogeneity of information: random.randint
* Variables: prim/sec keys, unique
* States creation: State list from Google, random number --> merge by index
* Id creation for connexions: from Python to SQL, from SQL to Python
* Connection to SQL

#### Connexion Schema

![Diagram.jpg](Images/Diagram.jpg)




## RESULTS



```python
import pymysql
import sqlalchemy as alch
from getpass import getpass
import pandas as pd
import random
```

    C:\Users\Aida\AppData\Local\Temp\ipykernel_456\3285062778.py:4: DeprecationWarning: 
    Pyarrow will become a required dependency of pandas in the next major release of pandas (pandas 3.0),
    (to allow more performant data types, such as the Arrow string type, and better interoperability with other libraries)
    but was not found to be installed on your system.
    If this would cause problems for you,
    please provide us feedback at https://github.com/pandas-dev/pandas/issues/54466
            
      import pandas as pd
    


```python
password= getpass("Por favor, introduce tu contraseña:") 
```


```python
dbName="healthcare"
```


```python
# Connect to MySQL
conn = pymysql.connect(
    host='localhost',
    user='root',
    password=password,
    database='healthcare'
)
```


```python
connectionData=f"mysql+pymysql://root:{password}@localhost/healthcare"
engine = alch.create_engine(connectionData)
```


### LET'S START QUERYING!!

#### 1. Check the prevalence of each condition across gender and age and which one is the most common disease


```python
query=pd.read_sql_query('''
select gender as Gender, medical_cond as Medical_condition, count(medical_cond) as N from patient p join bloodtype bt on p.blood_id=bt.blood_id
join medicalcond mc on p.medcond_id=mc.medcond_id
group by gender, medical_cond
order by Medical_condition desc;''', engine
)
query


```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Gender</th>
      <th>Medical_condition</th>
      <th>N</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Male</td>
      <td>Obesity</td>
      <td>790</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Female</td>
      <td>Obesity</td>
      <td>838</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Male</td>
      <td>Hypertension</td>
      <td>852</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Female</td>
      <td>Hypertension</td>
      <td>836</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Female</td>
      <td>Diabetes</td>
      <td>825</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Male</td>
      <td>Diabetes</td>
      <td>798</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Male</td>
      <td>Cancer</td>
      <td>816</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Female</td>
      <td>Cancer</td>
      <td>887</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Male</td>
      <td>Asthma</td>
      <td>834</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Female</td>
      <td>Asthma</td>
      <td>874</td>
    </tr>
    <tr>
      <th>10</th>
      <td>Male</td>
      <td>Arthritis</td>
      <td>835</td>
    </tr>
    <tr>
      <th>11</th>
      <td>Female</td>
      <td>Arthritis</td>
      <td>815</td>
    </tr>
  </tbody>
</table>
</div>



##### Most prevalent condition (subquery)


```python
query=pd.read_sql_query('''
select medical_cond as Medical_condition, count(p.medcond_id) as N  from medicalcond mc join patient p on p.medcond_id=mc.medcond_id
group by p.medcond_id having N=(select max(N) from (select count(p.medcond_id) as N, medical_cond from medicalcond mc 
join patient p on p.medcond_id=mc.medcond_id
group by p.medcond_id)sub1)''', engine
)
query
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Medical_condition</th>
      <th>N</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Asthma</td>
      <td>1708</td>
    </tr>
  </tbody>
</table>
</div>



#### 2. Study the effect of age. Is there any relation to the admission type or to the health condition?


```python
query=pd.read_sql_query('''
select admission_type as AdmissionType, round(avg(age),2) as MeanAge  from patient p join admission a on p.pat_id=a.pat_id
join admission_type aty on a.admtype_id=aty.admtype_id
group by AdmissionType
order by MeanAge desc''', engine
)
query
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>AdmissionType</th>
      <th>MeanAge</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Urgent</td>
      <td>51.96</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Elective</td>
      <td>51.44</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Emergency</td>
      <td>51.35</td>
    </tr>
  </tbody>
</table>
</div>




```python
query=pd.read_sql_query('''
select medical_cond as Medical_condition, round(avg(age),2) as MeanAge from patient p join medicalcond mc on p.medcond_id=mc.medcond_id
group by Medical_condition
order by MeanAge desc''', engine
)
query
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Medical_condition</th>
      <th>MeanAge</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Diabetes</td>
      <td>51.80</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Obesity</td>
      <td>51.63</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Cancer</td>
      <td>51.58</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Arthritis</td>
      <td>51.53</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Asthma</td>
      <td>51.45</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Hypertension</td>
      <td>50.74</td>
    </tr>
  </tbody>
</table>
</div>



#### 3. Study the effects of test results. Are they related with the length of stay?


```python
query=pd.read_sql_query('''
select test_result as TestResult, datediff(disc_date,adm_date) as LengthStay from admission ad join test_results tr on ad.testres_id=tr.testres_id
group by TestResult, LengthStay
order by LengthStay desc
limit 6''', engine
)
query
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>TestResult</th>
      <th>LengthStay</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Inconclusive</td>
      <td>30</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Normal</td>
      <td>30</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Abnormal</td>
      <td>30</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Abnormal</td>
      <td>29</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Normal</td>
      <td>29</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Inconclusive</td>
      <td>29</td>
    </tr>
  </tbody>
</table>
</div>



#### 4. Which is the month with more admissions?


```python
query=pd.read_sql_query('''
select date_format(convert(adm_date,date), '%%M') as Mes, count(*) as N  from admission
group by Mes
order by N desc''', engine
)
query

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Mes</th>
      <th>N</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>July</td>
      <td>1005</td>
    </tr>
    <tr>
      <th>1</th>
      <td>April</td>
      <td>985</td>
    </tr>
    <tr>
      <th>2</th>
      <td>October</td>
      <td>980</td>
    </tr>
    <tr>
      <th>3</th>
      <td>August</td>
      <td>968</td>
    </tr>
    <tr>
      <th>4</th>
      <td>May</td>
      <td>960</td>
    </tr>
    <tr>
      <th>5</th>
      <td>June</td>
      <td>959</td>
    </tr>
    <tr>
      <th>6</th>
      <td>January</td>
      <td>948</td>
    </tr>
    <tr>
      <th>7</th>
      <td>December</td>
      <td>935</td>
    </tr>
    <tr>
      <th>8</th>
      <td>March</td>
      <td>926</td>
    </tr>
    <tr>
      <th>9</th>
      <td>November</td>
      <td>918</td>
    </tr>
    <tr>
      <th>10</th>
      <td>September</td>
      <td>877</td>
    </tr>
    <tr>
      <th>11</th>
      <td>February</td>
      <td>874</td>
    </tr>
  </tbody>
</table>
</div>



#### 5 Study other hospitals. How many patients do they have 0- blood type?


```python
query=pd.read_sql_query('''
select hospital as Hospital, count(blood_type) as N_BloodType  from patient p join medicalcond mc on p.medcond_id=mc.medcond_id 
join admission ad on p.pat_id=ad.pat_id join hospital h on ad.hospital_id=h.hospital_id
join bloodtype bt on p.blood_id=bt.blood_id
group by Hospital, blood_type
having blood_type='o-' 
order by N_BloodType desc
limit 5''', engine
)
query

# 0- is the universal blood type so good for transfusions
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Hospital</th>
      <th>N_BloodType</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Martinez PLC</td>
      <td>9</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Williams LLC</td>
      <td>6</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Smith-Williams</td>
      <td>6</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Burton, Harris and Phillips</td>
      <td>6</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Booth LLC</td>
      <td>6</td>
    </tr>
  </tbody>
</table>
</div>



#### 6. Who are the patients who spent less amount of money (i.e. 0$-800$) in hospital stays? 


```python
query=pd.read_sql_query('''
select p.pat_id, name as Patient, billing_amount as $ from admission a join patient p on a.pat_id=p.pat_id
where billing_amount between 0 and 800
order by $ desc''', engine
)
query
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>pat_id</th>
      <th>Patient</th>
      <th>$</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>9279</td>
      <td>Mrs. Sandra Wood</td>
      <td>800.0</td>
    </tr>
  </tbody>
</table>
</div>



#### 7.  Classify the patients where they had a cheap/medium/expensive bill after being discharged from hospital.


```python
query=pd.read_sql_query('''
select billing_amount as $,
CASE
    WHEN billing_amount >20000 THEN 'Medium bill'
    WHEN billing_amount >40000 THEN 'High bill'
    ELSE 'Low bill'
END as Bill_Type
from admission a join patient p on a.pat_id=p.pat_id
limit 5''', engine
)
query
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>$</th>
      <th>Bill_Type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>26702.0</td>
      <td>Medium bill</td>
    </tr>
    <tr>
      <th>1</th>
      <td>16492.0</td>
      <td>Low bill</td>
    </tr>
    <tr>
      <th>2</th>
      <td>16492.0</td>
      <td>Low bill</td>
    </tr>
    <tr>
      <th>3</th>
      <td>22986.0</td>
      <td>Medium bill</td>
    </tr>
    <tr>
      <th>4</th>
      <td>22986.0</td>
      <td>Medium bill</td>
    </tr>
  </tbody>
</table>
</div>



#### 8. Select the maximum bill per hospital (window function). 


```python
query=pd.read_sql_query('''
select hospital_id as Hospital, max(billing_amount) over (partition by hospital_id) as MaxBill$
from admission
where billing_amount > 2000
order by Hospital
limit 10''', engine
)
query
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Hospital</th>
      <th>MaxBill$</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>35608.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>35608.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>4934.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>7060.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>4063.0</td>
    </tr>
    <tr>
      <th>5</th>
      <td>5</td>
      <td>39238.0</td>
    </tr>
    <tr>
      <th>6</th>
      <td>6</td>
      <td>38813.0</td>
    </tr>
    <tr>
      <th>7</th>
      <td>6</td>
      <td>38813.0</td>
    </tr>
    <tr>
      <th>8</th>
      <td>7</td>
      <td>19144.0</td>
    </tr>
    <tr>
      <th>9</th>
      <td>8</td>
      <td>30432.0</td>
    </tr>
  </tbody>
</table>
</div>



#### 9. List three patients who stayed in the V.I.P. room (Room 463). How many patients stayed in that room? -- subquery


```python
query=pd.read_sql_query('''
select name as Patient from patient where pat_id in (select pat_id from admission where room_number=463)
limit 3''', engine
)
query
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Patient</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>David Copeland</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Lori Smith</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Brian Cox</td>
    </tr>
  </tbody>
</table>
</div>




```python
query=pd.read_sql_query('''
select count(*) from patient where pat_id in (select pat_id from admission where room_number=463)
''', engine
)
query
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>count(*)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>18</td>
    </tr>
  </tbody>
</table>
</div>



#### 10. Add a column with the patient's nationality



```sql

alter table admission
add column pat_nation VARCHAR(50) default null after hospital_id;

UPDATE admission 
SET 
    pat_nation = 'American'
WHERE
    adm_id > 0;
```

All patients are from an American Country


```python
# Commit changes
conn.commit()

# Close connection
conn.close()
```

## STRENGTHS AND WEEAKNESSESS 

##### Database clean and easy to work with (S)
##### Nice N for robust conclusions (S)
##### Mix of real and fake data (S)

##### Time constraints (W)
##### Very homogeneus data at some point -- solution: randomised (W)
##### Some desired variables were missing --> incorporated manually (W)

## CONCLUSIONS

##### By looking a the data, efforts should be directed to asthmatic patients with a mean age of 50, although all the disease are similarly prevalente if we don't take into account other variables (e.g. gender). More staff should be hired during July (tourism and parties?). There are no age differences of patients regarding health disease or test result.
