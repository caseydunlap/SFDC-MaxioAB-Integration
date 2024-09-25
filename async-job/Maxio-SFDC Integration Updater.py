import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key
import requests
import json
import base64
from sqlalchemy import create_engine
import boto3
import pandas as pd

#Load secrets from secrets manager
def get_secrets(secret_names, region_name="us-east-1"):
    secrets = {}
    
    client = boto3.client(
        service_name='secretsmanager',
    )
    
    for secret_name in secret_names:
        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name)
        except Exception as e:
                raise e
        else:
            if 'SecretString' in get_secret_value_response:
                secrets[secret_name] = get_secret_value_response['SecretString']
            else:
                secrets[secret_name] = base64.b64decode(get_secret_value_response['SecretBinary'])

    return secrets
    
#Extract secret values from fetched secrets
def extract_secret_value(data):
    if isinstance(data, str):
        return json.loads(data)
    return data

secrets = ['maxio_core_api_key','snowflake_bizops_user','snowflake_account','snowflake_key_pass','snowflake_bizops_wh','snowflake_fivetran_db','snowflake_bizops_role','maxio_base_url']

fetch_secrets = get_secrets(secrets)

extracted_secrets = {key: extract_secret_value(value) for key, value in fetch_secrets.items()}

maxio_core_api_key = extracted_secrets['maxio_core_api_key']['maxio_core_api_key']
maxio_base_url = extracted_secrets['maxio_base_url']['maxio_base_url']
snowflake_user = extracted_secrets['snowflake_bizops_user']['snowflake_bizops_user']
snowflake_account = extracted_secrets['snowflake_account']['snowflake_account']
snowflake_key_pass = extracted_secrets['snowflake_key_pass']['snowflake_key_pass']
snowflake_bizops_wh = extracted_secrets['snowflake_bizops_wh']['snowflake_bizops_wh']
snowflake_schema = 'MAXIO_SAASOPTICS'
snowflake_fivetran_db = extracted_secrets['snowflake_fivetran_db']['snowflake_fivetran_db']
snowflake_role = extracted_secrets['snowflake_bizops_role']['snowflake_bizops_role']

password = snowflake_key_pass.encode()

#AWS S3 Configuration params
s3_bucket = 'aws-glue-assets-bianalytics'
s3_key = 'BIZ_OPS_ETL_USER.p8'

#Function to download file from S3
def download_from_s3(bucket, key):
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return response['Body'].read()
    except Exception as e:
        print(f"Error downloading from S3: {e}")
        return None

#Download the private key file from S3
key_data = download_from_s3(s3_bucket, s3_key)

#Load the private key as PEM
private_key = load_pem_private_key(key_data, password=password)

#Extract the private key bytes in PKCS8 format
private_key_bytes = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption())
    
ctx = snowflake.connector.connect(
    user=snowflake_user,
    account=snowflake_account,
    private_key=private_key_bytes,
    role=snowflake_role,
    warehouse=snowflake_bizops_wh)
    
staging_table_name = 'INTEGRATION_STAGING_TEMP_CONTRACT'

#Clear the intermediate staging table
cs = ctx.cursor()
script = f"""
delete from "{snowflake_fivetran_db}"."{snowflake_schema}"."{staging_table_name}"
"""
payload = cs.execute(script)

table_name = 'INTEGRATION_STAGING'

#Fetch rows that still require data enrichment
cs = ctx.cursor()
script = f"""
select
*
from "{snowflake_fivetran_db}"."{snowflake_schema}"."{table_name}"
where (lower(ab_contract_assoc_complete) = 'false')
"""
payload = cs.execute(script)
contract_assocs_needed = pd.DataFrame.from_records(iter(payload), columns=[x[0] for x in payload.description])

#Extract customer ids from snowflake results
customer_ids = ", ".join(contract_assocs_needed['AB_REFERENCE'].apply(lambda x: f"'{x}'"))

#Get details about customers who need enrichment from snowflake
cs = ctx.cursor()
script = f"""
select 
o.number as sf_act_id,
c.id as contract_id,
o.id as customer_id,
c.text_field_2
from "{snowflake_fivetran_db}"."{snowflake_schema}"."CONTRACT" c
inner join "{snowflake_fivetran_db}"."{snowflake_schema}"."CUSTOMER" o on o.id = c.customer_id
inner join "{snowflake_fivetran_db}"."{snowflake_schema}"."REGISTER" r on c.register_id = r.id
where lower(r.name) like '%cashe% and sf_act_id in ({customer_ids})
"""
payload = cs.execute(script)
customer_update = pd.DataFrame.from_records(iter(payload), columns=[x[0] for x in payload.description])

#Do some data cleanup, begin preparing import dataframe
customers_missing_value = customer_update.rename(columns={'SF_ACT_ID':'AB_REFERENCE'})
customers_missing_value['AB_CONTRACT_ASSOC_COMPLETE'] = True
customers_missing_value_final = customers_missing_value.drop(columns=['CONTRACT_ID','CUSTOMER_ID','TEXT_FIELD_2'])

merged_customer_list = pd.merge(customers_missing_value_final,contract_assocs_needed,left_on='AB_REFERENCE',right_on='AB_REFERENCE')
merged_customer_list_final = pd.merge(merged_customer_list,customer_update,left_on='AB_REFERENCE',right_on='SF_ACT_ID')

#Function to extract necessary data from dataframe
def extract_ids_from_dataframe(df):
    return [
        {
            'id': row['CONTRACT_ID'],
            'text_field2': row['AB_SUBSCRIPTION']
        }
        for _, row in df.iterrows()
    ]

result = extract_ids_from_dataframe(merged_customer_list_final)

#Send a patch request to core contracts endpoint to write AB subscription ids to Core
base_url = f'{maxio_base_url}/api/v1.0/contracts/'

for item in result:
    contract_id = item['id']
    text_field2 = item['text_field2']
    
    url = f'{base_url}{contract_id}/'

    headers = {
        'Authorization': f'Token {maxio_core_api_key}',
        'Content-Type': 'application/json'
    }
    
    payload = {
        "text_field2": text_field2
    }
    
    response = requests.patch(url, headers=headers, json=payload)
    
#Construct the SQLAlchemy connection string
connection_string = f"snowflake://{snowflake_user}@{snowflake_account}/{snowflake_fivetran_db}/{snowflake_schema}?warehouse={snowflake_bizops_wh}&role={snowflake_role}&authenticator=externalbrowser"

#Instantiate SQLAlchemy engine with the private key
engine = create_engine(
        connection_string,
        connect_args={
            "private_key": private_key_bytes
        }
    )

#Write the updated customers to snowflake int staging table
customers_missing_value_final.to_sql(staging_table_name, engine, if_exists='append', index=False, method='multi', chunksize=1000)

#Merge the updated rows with the final staging table
merge_sql = f"""
MERGE INTO {table_name} AS target
USING "{snowflake_fivetran_db}"."{snowflake_schema}"."{staging_table_name}" AS staging
ON target.AB_REFERENCE = staging.AB_REFERENCE
WHEN MATCHED THEN
    UPDATE SET
target.AB_CONTRACT_ASSOC_COMPLETE=staging.AB_CONTRACT_ASSOC_COMPLETE;
COMMIT;"""
engine.execute(merge_sql)




