from ast import Break
import dbm
from tkinter.tix import COLUMN
import pandas as pd 
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from faker import Faker # https://faker.readthedocs.io/en/master/
import uuid
import random

load_dotenv('credentials.env')

GCP_MYSQL_HOSTNAME = os.getenv('GCP_MYSQL_HOSTNAME')
GCP_MYSQL_USER = os.getenv('GCP_MYSQL_USER')
GCP_MYSQL_PASSWORD = os.getenv('GCP_MYSQL_PASSWORD')
GCP_MYSQL_DATABASE = os.getenv('GCP_MYSQL_DATABASE')

##### connecting to the database #####

connection_string = f'mysql+pymysql://{GCP_MYSQL_USER}:{GCP_MYSQL_PASSWORD}@{GCP_MYSQL_HOSTNAME}/{GCP_MYSQL_DATABASE}'
db = create_engine(connection_string)

tables_names = db.table_names()
print(tables_names) ## confirming connection worked in addition to printing the current tables within the connected database 

##### creating fake data ##### 

fake = Faker()

## creating a dictionary within a list of fake patient information 

fake_patients = [
    {
        'mrn': str(uuid.uuid4())[:5],  #keep just the first 5 characters of the uuid
        'first_name':fake.first_name(), 
        'last_name':fake.last_name(),
        'zip_code':fake.zipcode(),
        'dob':(fake.date_between(start_date='-90y', end_date='-20y')).strftime("%Y-%m-%d"),
        'gender': fake.random_element(elements=('M', 'F')),
        'contact_mobile':fake.phone_number(),
        'contact_email':fake.email()
    } for x in range(20)
]

df_fake_patients = pd.DataFrame(fake_patients) ## turn the list into a dataframe
df_fake_patients = df_fake_patients.drop_duplicates(subset=['mrn']) 

##### loading in some real data ##### 

ndc_codes = pd.read_csv('https://raw.githubusercontent.com/hantswilliams/FDA_NDC_CODES/main/NDC_2022_product.csv')
list(ndc_codes.columns)
ndc_codes_1k = ndc_codes.sample(n=1000, random_state=1)
# drop duplicates from ndc_codes_1k
ndc_codes_1k = ndc_codes_1k.drop_duplicates(subset=['PRODUCTNDC'], keep='first')

cpt_codes = pd.read_csv('https://gist.githubusercontent.com/lieldulev/439793dc3c5a6613b661c33d71fdd185/raw/25c3abcc5c24e640a0a5da1ee04198a824bf58fa/cpt4.csv')
list(cpt_codes.columns)
cpt_codes.rename(columns={'com.medigy.persist.reference.type.clincial.CPT.code':'CPT code'}, inplace=True) ## renaming the column name
cpt_codes_1k = cpt_codes.sample(n=1000, random_state=1)
cpt_codes_1k = cpt_codes_1k.drop_duplicates(subset=['CPT code'], keep='first')

icd10_code = pd.read_csv('https://raw.githubusercontent.com/Bobrovskiy/ICD-10-CSV/master/2020/diagnosis.csv')
list(icd10_code.columns)
icd10_code_short = icd10_code[['CodeWithSeparator', 'ShortDescription']]
icd10_code_short_1k = icd10_code_short.sample(n=1000, random_state=1)
icd10_code_short_1k = icd10_code_short_1k.drop_duplicates(subset=['CodeWithSeparator'], keep='first')

loinc_code = pd.read_csv('LOINC CODE.csv')
list(loinc_code.columns)
loinc_code.drop_duplicates(subset=['LOINC Code'], keep='first')

##### Inserting the values into the tables #####

insertQuery = """INSERT INTO patients (
    mrn, 
    first_name, 
    last_name, 
    zip_code, 
    dob, 
    gender, 
    contact_mobile, 
    contact_email
    ) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

for index, row in df_fake_patients.iterrows():
    db.execute(insertQuery, (row['mrn'], row['first_name'], row['last_name'], row['zip_code'], row['dob'], row['gender'], row['contact_mobile'], row['contact_email']))
    print("inserted row: ", index)

insertQuery = "INSERT INTO medications (med_ndc, med_human_name) VALUES (%s, %s)"

startingRow = 0
for index, row in ndc_codes_1k.iterrows(): 
    startingRow += 1 
    db.execute(insertQuery, (row['PRODUCTNDC'], row['NONPROPRIETARYNAME']))
    print("Row completed: ", index)
    ## stop once we have 20 rows
    if startingRow == 20:
        break

insertQuery = "INSERT INTO treatments_procedure (treatment_cpt_code, cpt_code_description) VALUES (%s, %s)" 

startingRow = 0 
for index, row in cpt_codes_1k.iterrows():
    startingRow += 1 
    db.execute(insertQuery, (row['CPT code'], row['label']))
    print("inserted row #: ", index)
    if startingRow == 20: 
        break 

insertQuery = "INSERT INTO conditions (icd10_code, icd10_description) VALUES (%s, %s)"

startingRow = 0
for index, row in icd10_code_short_1k.iterrows():
    startingRow += 1
    db.execute(insertQuery, (row['CodeWithSeparator'], row['ShortDescription']))
    print("inserted row db: ", index)
    if startingRow == 20:
        break

insertQuery = "INSERT INTO social_determinant (loinc_code, loinc_code_desciprtion) VALUES (%s, %s)"

startingRow = 0 
for index, row in loinc_code.iterrows():
    startingRow += 1 
    db.execute(insertQuery, (row['LOINC Code'], row['Description ']))
    print("inserted row #: ", index)
    if startingRow == 20:
        break 

##### inserting values for tables with foreign key ##### 

df_treatment = pd.read_sql_query("SELECT treatment_cpt_code FROM treatments_procedure", db)
df_patients = pd.read_sql_query("SELECT mrn FROM patients", db)

# create a dataframe that is stacked and give each patient a random number of conditions between 1 and 5
df_patient_treatment = pd.DataFrame(columns=['mrn', 'treatment_cpt_code'])
# for each patient in df_patient_conditions, take a random number of conditions between 1 and 10 from df_conditions and palce it in df_patient_conditions
for index, row in df_patients.iterrows():
    # get a random number of conditions between 1 and 5
    # numtreatment = random.randint(1, 5)
    # get a random sample of conditions from df_conditions
    df_treatment_sample = df_treatment.sample(n=random.randint(1, 5))
    # add the mrn to the df_conditions_sample
    df_treatment_sample['mrn'] = row['mrn']
    # append the df_conditions_sample to df_patient_conditions
    df_patient_treatment = df_patient_treatment.append(df_treatment_sample)

df_patient_treatment.rename(columns={'treatment_cpt_code':'cpt_code'},inplace=True) ## changing the column name to match the new name

insertQuery = "INSERT INTO patient_treatment (mrn, cpt_code) VALUES (%s, %s)"

for index, row in df_patient_treatment.iterrows():
    db.execute(insertQuery, (row['mrn'], row['cpt_code']))
    print("inserted row #: ", index)

df_pm = pd.read_sql_query("SELECT med_ndc FROM medications", db)
df_patients2 = pd.read_sql_query("SELECT mrn FROM patients", db)

df_patient_medication = pd.DataFrame(columns=['mrn', 'med_ndc'])

for index, row in df_patients2.iterrows():
    df_medication_sample = df_pm.sample(n=random.randint(1, 5))
    df_medication_sample['mrn'] = row['mrn']
    df_patient_medication = df_patient_medication.append(df_medication_sample)

df_patient_medication.rename(columns={'med_ndc':'ndc_code'},inplace=True)
    
insertQuery = "INSERT INTO patient_medications (mrn, ndc_code) VALUES (%s, %s)"

startingRow = 0
for index, row in df_patient_medication.iterrows():
    startingRow += 1
    db.execute(insertQuery, (row['mrn'], row['ndc_code']))
    print("inserted row #: ", index)
    if startingRow == 30:
        break

##### confirming the data insertion functioned properly ##### 

df_patients = pd.read_sql_query("SELECT * FROM patients", db) 
df_medications = pd.read_sql_query("SELECT * FROM medications", db)
df_treatment_procedure = pd.read_sql_query("SELECT * FROM treatments_procedure")
df_conditions = pd.read_sql_query("SELECT * FROM conditions", db)
df_social_determinant = pd.read_sql_query("SELECT * FROM social_determinant", db)
df_pt = pd.read_sql_query("SELECT * FROM patient_treatment", db)
df_patientmed = pd.read_sql_query("SELECT * FROM patient_medications", db)

##### Performing query task listed for Assignment ##### 

query1 = pd.read_sql_query("show databases", db)
query2 = pd.read_sql_query("show tables", db)
query3 = pd.read_sql_query("SELECT * FROM patient_portal.medications", db) 
query4 = pd.read_sql_query("SELECT * FROM patient_portal.treatments_procedure", db)
query5 = pd.read_sql_query("SELECT * FROM patient_portal.conditions", db)

### Comment it out later ### 

df_conditions_fake = pd.read_sql_query("SELECT icd10_code FROM conditions", db)
df_patients_fake = pd.read_sql_query("SELECT mrn FROM patients", db)
df_social_fake = pd.read_sql_query("SELECT loinc_code FROM social_determinant", db)

df_patient_ccurrent_info = pd.DataFrame(columns=['mrn', 'icd10_code', 'loinc_code'])

for index, row in df_patients_fake.iterrows():
    randomCount = random.randint(1,5)   
    df_conditions_sample = df_conditions_fake.sample(randomCount)
    df_conditions_sample['mrn'] = row['mrn']
    df_social_sample = df_social_fake.sample(randomCount)
    df_patient_ccurrent_info = df_patient_ccurrent_info.append(df_conditions_sample)
    df_patient_ccurrent_info = df_patient_ccurrent_info.append(df_social_sample)

