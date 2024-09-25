import requests
import pandas as pd
import boto3
import base64
import numpy as np
import logging
import random
import json
import time
from advancedbilling.advanced_billing_client import AdvancedBillingClient
from advancedbilling.http.auth.basic_auth import BasicAuthCredentials
from advancedbilling.models.pricing_scheme import PricingScheme
from advancedbilling.models.price import Price
from advancedbilling.models.create_subscription_request import CreateSubscription,CreateSubscriptionRequest
from advancedbilling.models.create_customer_request import CreateCustomer,CreateCustomerRequest
from advancedbilling.models.create_component_price_point_request import CreateComponentPricePointRequest
from advancedbilling.models.create_component_price_point import CreateComponentPricePoint
from advancedbilling.models.create_subscription_component import CreateSubscriptionComponent
import io
from io import BytesIO
from sqlalchemy import create_engine
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key

#Set up cloudwatch logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    print("Recieved event:", json.dumps(event,indent=2))
    logger.info("Lambda function started")

    try:
        #If the event is a string, parse it as JSON
        if isinstance(event, str):
            event = json.loads(event)

        #Extract the body from the event
        event_data = event["body"]

        #Parse the body content
        body = json.loads(event_data)

        #Extract Opportunity_Id from the body
        opportunity_id = body["Opportunity_Id"]

        #Load secrets from secrets manager
        def get_secrets(secret_names, region_name="us-east-1"):
            secrets = {}
            
            client = boto3.client(
                service_name='secretsmanager',
                region_name=region_name
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

        secrets = ['sfdc_prod_client_id','sfdc_prod_client_secret','maxio_prod_ab_api_key','snowflake_bizops_user','snowflake_account','snowflake_key_pass','snowflake_bizops_wh','snowflake_fivetran_db','snowflake_bizops_role',
                   'sfdc_hostname','maxio_ab_domain']

        fetch_secrets = get_secrets(secrets)

        extracted_secrets = {key: extract_secret_value(value) for key, value in fetch_secrets.items()}

        sfdc_prod_client_id = extracted_secrets['sfdc_prod_client_id']['sfdc_prod_client_id']
        sfdc_prod_secret_id = extracted_secrets['sfdc_prod_client_secret']['sfdc_prod_client_secret']
        maxio_prod_api_key = extracted_secrets['maxio_prod_ab_api_key']['maxio_prod_ab_api_key']
        snowflake_user = extracted_secrets['snowflake_bizops_user']['snowflake_bizops_user']
        snowflake_account = extracted_secrets['snowflake_account']['snowflake_account']
        snowflake_key_pass = extracted_secrets['snowflake_key_pass']['snowflake_key_pass']
        snowflake_bizops_wh = extracted_secrets['snowflake_bizops_wh']['snowflake_bizops_wh']
        snowflake_schema = 'MAXIO_SAASOPTICS'
        snowflake_fivetran_db = extracted_secrets['snowflake_fivetran_db']['snowflake_fivetran_db']
        snowflake_role = extracted_secrets['snowflake_bizops_role']['snowflake_bizops_role']
        sfdc_hostname = extracted_secrets['sfdc_hostname']['sfdc_hostname']
        maxio_ab_domain = extracted_secrets['maxio_ab_domain']['maxio_ab_domain']

        password = snowflake_key_pass.encode()

        #AWS S3 Configuration
        s3_bucket = 'cashemaxiohandler-env'
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

        #Call download from S3 function, download the private key file
        key_data = download_from_s3(s3_bucket, s3_key)

        #Loead the private key as PEM
        private_key = load_pem_private_key(key_data, password=password)

        #Extract the private key bytes in PKCS8 format
        private_key_bytes = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption())

        #Construct the SQLAlchemy connection string
        connection_string = f"snowflake://{snowflake_user}@{snowflake_account}/{snowflake_fivetran_db}/{snowflake_schema}?warehouse={snowflake_bizops_wh}&role={snowflake_role}&authenticator=externalbrowser"

        #If the event is a string, parse it as JSON
        if isinstance(event, str):
            event = json.loads(event)

        #Extract the body from the event
        event_data = event["body"]

        #Parse the body content
        body = json.loads(event_data)

        #Extract Opportunity_Id from the body
        opportunity_id = body["Opportunity_Id"]

        client_id = sfdc_prod_client_id
        client_secret = sfdc_prod_secret_id

        #Fetch the instance url and access token from Salesforce API
        hostname = sfdc_hostname
        token_url = f'{hostname}/services/oauth2/token'

        payload = {
            'grant_type': 'client_credentials',
            'client_id': client_id,
            'client_secret': client_secret
        }

        response = requests.post(token_url, data=payload)

        token_response = response.json()
        access_token = token_response.get('access_token')
        instance_url = token_response.get('instance_url')

        #Build the Salesforce query url
        query_url = f"{instance_url}/services/data/v61.0/query/"

        #Headers for each Salesforce query
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }

        #Validate the bu
        opp_query = f"""
        SELECT fields(all)
        from opportunity
        where Id = '{opportunity_id}'
        limit 200
        """
        opp_params = {'q': opp_query}
        opp_response = requests.get(query_url, headers=headers, params=opp_params)
        opp_records = opp_response.json()['records']
        primary_quote = opp_response.json()['records'][0].get('SBQQ__PrimaryQuote__c')
        primary_contact = opp_response.json()['records'][0].get('ContactId')
        opp_df = pd.DataFrame(opp_records)

        opp_line_item_query = f"""
        SELECT fields(all)
        FROM OpportunityLineItem
        WHERE OpportunityId = '{opportunity_id}'
        LIMIT 200
        """
        opp_li_params = {'q': opp_line_item_query}
        opp_li_response = requests.get(query_url, headers=headers, params=opp_li_params)

        #Process the response to extract the records
        opp_li_records = opp_li_response.json().get('records', [])
        product_ids = [record.get('Product2Id') for record in opp_li_records]
        product_ids_string = ','.join(f"'{id}'" for id in product_ids)

        product_name_query = f"""
        SELECT fields(ALL)
        from Product2
        where Id in ({product_ids_string})
        limit 200
        """
        product_params = {'q': product_name_query}
        product_response = requests.get(query_url, headers=headers, params=product_params)
        products = product_response.json().get('records', [])
        product_skus = [i.get('ProductCode') for i in products]

        #Check all of the skus on the opportunity, if cashe continue, if not exit
        if any("CASH" in i for i in product_skus):
            pass
        else:
            return {"statusCode": 200, "body": json.dumps({"message": "Found no cashe products on opportunity, exiting function."})}

        #Query the opportunity table to fetch the primary quote
        opp_query = f"""
            SELECT fields(all)
            from opportunity
            where Id = '{opportunity_id}'
            limit 200
        """

        opp_params = {'q': opp_query}
        opp_response = requests.get(query_url, headers=headers, params=opp_params)
        opp_records = opp_response.json()['records']
        primary_quote = opp_response.json()['records'][0].get('SBQQ__PrimaryQuote__c')
        primary_contact = opp_response.json()['records'][0].get('ContactId')
        opp_df = pd.DataFrame(opp_records)

        #Query the quote table to fetch the primary contact
        quote_query = f"""
        SELECT fields(ALL)
        from SBQQ__Quote__c
        where Id = '{primary_quote}'
        limit 200
        """

        quote_params = {'q': quote_query}
        quote_response = requests.get(query_url, headers=headers, params=quote_params)
        primary_contact = quote_response.json()['records'][0].get('SBQQ__PrimaryContact__c')

        #Query the contact table
        contact_name_query = f"""
            SELECT fields(ALL)
            from Contact
            where Id = '{primary_contact}'
            limit 200
        """

        contact_params = {'q': contact_name_query}
        contact_response = requests.get(query_url, headers=headers, params=contact_params)

        time.sleep(30)

        #Query the order table
        order_query = f"""
            SELECT fields(all)
            from order
            where SBQQ__Quote__c = '{primary_quote}'
            limit 200
        """

        order_params = {'q': order_query}
        order_response = requests.get(query_url, headers=headers, params=order_params)
        order_id = order_response.json()['records'][0].get('Id')

        #Query the order item table
        orderitem_query = f"""
            SELECT fields(ALL)
            from OrderItem
            where orderid = '{order_id}'
            limit 200
        """

        orderitem_params = {'q': orderitem_query}
        orderitem_response = requests.get(query_url, headers=headers, params=orderitem_params)
        item_records = orderitem_response.json().get('records', [])
        order_item_ids = ','.join([f"'{record.get('Id')}'" for record in item_records if record.get('Id')])
        order_item_df = pd.DataFrame(item_records)

            #Query the consumption table
        consumption_schedule_query = f"""
            SELECT fields(ALL)
            from SBQQ__OrderItemConsumptionSchedule__c
            where SBQQ__OrderItem__c in ({order_item_ids})
            limit 200
        """

        conssched_params = {'q': consumption_schedule_query}
        conssched_response = requests.get(query_url, headers=headers, params=conssched_params)
        cons_records = conssched_response.json().get('records', [])
        cons_schedule_ids = ','.join([f"'{record.get('Id')}'" for record in cons_records if record.get('Id')])
        cons_item_df = pd.DataFrame(cons_records)

        #Query the consumption rate table
        consumption_rate_query = f"""
            SELECT fields(all)
            from SBQQ__OrderItemConsumptionRate__c
            where SBQQ__OrderItemConsumptionSchedule__c in ({cons_schedule_ids})
            limit 200
        """
        consrate_params = {'q': consumption_rate_query}
        consrate_response = requests.get(query_url, headers=headers, params=consrate_params)
        cons_rates = consrate_response.json().get('records', [])
        cons_rate_df = pd.DataFrame(cons_rates)

        #Do some data transformation for final price consumption dataframe
        item_cols_to_keep = ['Id','Product2Id']
        temp_orderitem_df = order_item_df[item_cols_to_keep]
        fin_orderitem_df = temp_orderitem_df.rename(columns={'Id':'OrderLineId','Product2Id':'ProductID'})

        cons_cols_to_keep = ['Id','SBQQ__OrderItem__c']
        temp_cons_item_df = cons_item_df[cons_cols_to_keep]
        fin_cons_item_df = temp_cons_item_df.rename(columns={'Id':'ConsScheduleId','SBQQ__OrderItem__c':'OrderLineId'})

        cons_rate_cols_to_keep = ['SBQQ__OrderItemConsumptionSchedule__c','Name','SBQQ__LowerBound__c','SBQQ__Price__c']
        temp_cons_rate_item_df = cons_rate_df[cons_rate_cols_to_keep]
        fin_cons_rate_item_df = temp_cons_rate_item_df.rename(columns={'SBQQ__OrderItemConsumptionSchedule__c':'ConsScheduleId','SBQQ__LowerBound__c':'LowerBound','SBQQ__Price__c':'PricePer'})

        first_merged_df = pd.merge(fin_orderitem_df,fin_cons_item_df,left_on='OrderLineId',right_on='OrderLineId',how='left')
        second_merged_df = pd.merge(first_merged_df,fin_cons_rate_item_df,left_on='ConsScheduleId',right_on='ConsScheduleId',how='left')

        second_merged_df['AccountID'] = opp_df['AccountId'].iloc[0]

        account_name_query = f"""
            SELECT fields(ALL)
            from Account
            where Id = '{second_merged_df['AccountID'][0]}'
            limit 200
        """

        accname_params = {'q': account_name_query}
        accname_response = requests.get(query_url, headers=headers, params=accname_params)
        accname = accname_response.json()['records'][0].get('Name')

        second_merged_df['AccountName'] = accname

        product_ids_string = ','.join(f"'{id}'" for id in second_merged_df['ProductID'])

        product_name_query = f"""
            SELECT fields(ALL)
            from Product2
            where Id in ({product_ids_string})
            limit 200
        """

        product_params = {'q': product_name_query}
        product_response = requests.get(query_url, headers=headers, params=product_params)
        products = product_response.json().get('records', [])
        product_name_df = pd.DataFrame(products)
        product_cols_to_keep = ['Id','Name']
        temp_product_df = product_name_df[product_cols_to_keep]
        final_product_df = temp_product_df.rename(columns={'Id':'ProductID','Name':'ProductName'})

        #Build out the dataframe which will serve as the source for our component price tier
        final_salesforce_df = pd.merge(second_merged_df,final_product_df,left_on='ProductID',right_on='ProductID',how='left')

        #Remove products that do not have a consumption schedule
        final_salesforce_df.dropna(inplace=True)

        #Initialize an empty DataFrame with multiple columns
        customer_create_df = pd.DataFrame(columns=['Reference','AccName','Email','Address1','City','State','Zip','Country','Phone'])

        #Get the values from the response with error handling (assigning None if not found)
        id_value = accname_response.json()['records'][0].get('Id', None)
        name_value = accname_response.json()['records'][0].get('Name', None)
        email_value = accname_response.json()['records'][0].get('ia_crm__Email_ID__c', None)
        if email_value is None:
            email_value = contact_response.json()[0].get('Email', 'noemailprovided@testco.com') if isinstance(contact_response.json(), list) else contact_response.json().get('records', [{}])[0].get('Email', 'noemailprovided@testco.com')
        address_value = accname_response.json()['records'][0].get('BillingStreet', None)
        city_value = accname_response.json()['records'][0].get('BillingCity', None)
        state_value = accname_response.json()['records'][0].get('BillingState', None)
        zip_value = accname_response.json()['records'][0].get('BillingPostalCode', None)
        country_value = accname_response.json()['records'][0].get('BillingCountry', None)
        phone_value = accname_response.json()['records'][0].get('Phone', None)

        #Create a dictionary to represent a the customer row
        create_customer_row = {
            'Reference': id_value if id_value is not None else np.nan,
            'AccName': name_value if name_value is not None else np.nan,
            'Email': email_value if email_value is not None else 'noemailprovided@testco.com',
            'Address1': address_value if address_value is not None else '123 No St.',
            'City': city_value if city_value is not None else 'NoTown',
            'State': state_value if state_value is not None else 'NS',
            'Zip': zip_value if zip_value is not None else '55555',
            'Country': country_value if country_value is not None else 'US',
            'Phone': phone_value if phone_value is not None else '555-555-5555'
        }

        #Convert the dictionary to a dataframe
        create_customer_row_df = pd.DataFrame([create_customer_row])

        #Concat the create customer row dataFrame to the create customer dataFrame
        customer_create_df = pd.concat([customer_create_df, create_customer_row_df], ignore_index=True)

        #Store the salesforce account id to check if AB customer record already exists
        salesforce_customer_id = final_salesforce_df['AccountID'].iloc[0]

        #Instantiatite the AB client
        client = AdvancedBillingClient(
            basic_auth_credentials=BasicAuthCredentials(
                username=maxio_prod_api_key,
                password='x'
            ),
            subdomain=maxio_ab_domain,
            domain='chargify.com'
        )

        #Instantiatite the customer controller for the customer search, and if needed AB customer record creation
        customers_controller = client.customers

        #See if the customer already exists in AB
        collect = {
            'page': 1,
            'per_page': 1,
            'q': salesforce_customer_id
        }
        customer_search_result = customers_controller.list_customers(collect)

        #In the event the customer already exists, store the reference for use later
        try:
            customer_reference = [customer.customer.reference for customer in customer_search_result][0]
        except IndexError:
            customer_reference = None
        try:
            customer_ab_id = [customer.customer.id for customer in customer_search_result][0]
        except IndexError:
            customer_ab_id = None

        if customer_reference == None:
            #Create the customer record in AB
            customer_body = CreateCustomerRequest(
            customer=CreateCustomer(
                first_name='Accounts',
                last_name='Payable',
                email=customer_create_df['Email'].iloc[0],
                organization=customer_create_df['AccName'].iloc[0],
                reference=customer_create_df['Reference'].iloc[0],
                address=customer_create_df['Address1'].iloc[0],
                city=customer_create_df['City'].iloc[0],
                state=customer_create_df['State'].iloc[0],
                zip=customer_create_df['Zip'].iloc[0],
                country='US',
                phone=customer_create_df['Phone'].iloc[0],
                locale='en-US'))

            customer_response = customers_controller.create_customer(
            body=customer_body)

            #Store the customer reference for the previously created AB customer record for use later
            customer_reference = customer_response.customer.reference

            unique_products = final_salesforce_df['ProductName'].unique()

            product_specific_dataframes = {}

            #Split the dataframe into unique dataframes per product
            for product in unique_products:
                product_specific_dataframes[product] = final_salesforce_df[final_salesforce_df['ProductName'] == product]

            component_dictionary = {'Pavillio Subscription - Core Billing':'2544422','Pavillio Subscription - County Billing':'2544424',
                                    'Managed Billing Subscription':'2554766','Pavillio Per Client Fee':'2544421','Pavillio Platform - Basic':'2544427',
                                    'Pavillio Platform - Lite':'2544427','Billing Services Subscription (By Claim)': '2544425'}

            #Instantiate the component price points controller
            component_price_points_controller = client.component_price_points

            #Create a dictionary to store created price points
            created_price_points = {}

            for product, df in product_specific_dataframes.items():
                component_id = component_dictionary.get(product)
                
                if component_id is None:
                    continue
                
                prices_list = []
                
                #Build the consumption price point from the specific product salesforce consumption schedules
                for i in range(len(df) - 1):
                    starting_quantity = df['LowerBound'].iloc[i]
                    ending_quantity = df['LowerBound'].iloc[i + 1] - 1
                    unit_price = df['PricePer'].iloc[i]
                    
                    #Build the dynamic Maxio AB price object
                    prices_list.append(Price(
                        starting_quantity=str(starting_quantity),
                        ending_quantity=str(ending_quantity),
                        unit_price=str(unit_price)
                    ))

                #Handle the last row (no ending quantity limit for the last tier
                prices_list.append(Price(
                    starting_quantity=str(df['LowerBound'].iloc[-1]),
                    ending_quantity=None,
                    unit_price=str(df['PricePer'].iloc[-1])
                ))

                #Assign a unique name to the component price point
                price_point_name = f"{df['AccountName'].iloc[0]}_{random.randint(45205,985478)}"

                price_point_body = CreateComponentPricePointRequest(
                    price_point=CreateComponentPricePoint(
                        name=price_point_name,
                        pricing_scheme=PricingScheme.VOLUME,
                        prices=prices_list,
                        use_site_exchange_rate=False
                    )
                )

                try:
                    price_point_response = component_price_points_controller.create_component_price_point(
                        component_id,
                        body=price_point_body
                    )

                    #Store the created price point in the dictionary for use later
                    created_price_points[product] = {
                        'component_id': component_id,
                        'price_point_id': price_point_response.price_point.id,
                        'price_point_name': price_point_name
                    }
                except Exception as e:
                    print(f"Failed to create price point for {product}: {str(e)}")

            #Instantiate the subscriptions controller
            subscriptions_controller = client.subscriptions

            #Create a list to hold all the subscription components
            subscription_components = []

            #Iterate through the created_price_points dictionary to create each products subscription components
            for product, details in created_price_points.items():
                subscription_components.append(
                    CreateSubscriptionComponent(
                        component_id=details['component_id'],
                        enabled=True,
                        price_point_id=details['price_point_id']
                    )
                )

            #Make the subscription request
            subscription_body = CreateSubscriptionRequest(
                subscription=CreateSubscription(
                    product_handle='monthly-usage',  #this parameter will always be the same
                    customer_reference=customer_reference,
                    components=subscription_components
                )
            )

            subscription_result = subscriptions_controller.create_subscription(
                body=subscription_body
            )

            #Build snowflake import dataframe
            snowflake_df = pd.DataFrame()
            snowflake_df['AB_REFERENCE'] = []
            snowflake_df['AB_SUBSCRIPTION'] = []
            snowflake_df['AB_CONTRACT_ASSOC_COMPLETE'] = []

            snowflake_df.loc[0, 'AB_REFERENCE'] = customer_response.customer.reference
            snowflake_df.loc[0, 'AB_SUBSCRIPTION'] = subscription_result.subscription.id
            snowflake_df['AB_SUBSCRIPTION'] = snowflake_df['AB_SUBSCRIPTION'].astype(int).astype(str)
            snowflake_df['AB_CONTRACT_ASSOC_COMPLETE'] = 'FALSE'

            #Instantiate SQLAlchemy engine with the private key
            engine = create_engine(
                connection_string,
                connect_args={
                    "private_key": private_key_bytes
                }
            )

            table_name = 'INTEGRATION_STAGING' 

            with engine.connect() as conn:
                table_name = 'INTEGRATION_STAGING'

                #Ensure column names are consistent
                snowflake_df.columns = [col.upper() for col in snowflake_df.columns]

                #Construct the insert statement
                columns = ', '.join(snowflake_df.columns)
                placeholders = ', '.join(['%s'] * len(snowflake_df.columns))

                #Convert chunk data to list of tuples
                data_tuples = [tuple(row) for row in snowflake_df.itertuples(index=False, name=None)]

                insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

                conn.execute(insert_query,data_tuples)

        #If the AB customer already exists, follow many of the steps above, with the exception of the AB customer creation
        else:
            unique_products = final_salesforce_df['ProductName'].unique()

            product_specific_dataframes = {}

            #Split the dataframe into unique dataframes per product
            for product in unique_products:
                product_specific_dataframes[product] = final_salesforce_df[final_salesforce_df['ProductName'] == product]

            component_dictionary = {'Pavillio Subscription - Core Billing':'2544422','Pavillio Subscription - County Billing':'2544424',
                                    'Managed Billing Subscription':'2554766','Pavillio Per Client Fee':'2544421','Pavillio Platform - Basic':'2544427',
                                    'Pavillio Platform - Lite':'2544427','Billing Services Subscription (By Claim)': '2544425'}

            #Instantiate the component price points controller
            component_price_points_controller = client.component_price_points

            #Create a dictionary to store created price points
            created_price_points = {}

            for product, df in product_specific_dataframes.items():
                component_id = component_dictionary.get(product)
                
                if component_id is None:
                    continue
                
                prices_list = []
                
                #Build the consumption price point from the specific product salesforce consumption schedules
                for i in range(len(df) - 1):
                    starting_quantity = df['LowerBound'].iloc[i]
                    ending_quantity = df['LowerBound'].iloc[i + 1] - 1
                    unit_price = df['PricePer'].iloc[i]
                    
                    #Build the dynamic Maxio AB price object
                    prices_list.append(Price(
                        starting_quantity=str(starting_quantity),
                        ending_quantity=str(ending_quantity),
                        unit_price=str(unit_price)
                    ))

                #Handle the last row (no ending quantity limit for the last tier
                prices_list.append(Price(
                    starting_quantity=str(df['LowerBound'].iloc[-1]),
                    ending_quantity=None,
                    unit_price=str(df['PricePer'].iloc[-1])
                ))

                #Assign a unique name to the component price point
                price_point_name = f"{df['AccountName'].iloc[0]}_{random.randint(45205,985478)}"

                price_point_body = CreateComponentPricePointRequest(
                    price_point=CreateComponentPricePoint(
                        name=price_point_name,
                        pricing_scheme=PricingScheme.VOLUME,
                        prices=prices_list,
                        use_site_exchange_rate=False
                    )
                )

                try:
                    price_point_response = component_price_points_controller.create_component_price_point(
                        component_id,
                        body=price_point_body
                    )

                    #Store the created price point in the dictionary for use later
                    created_price_points[product] = {
                        'component_id': component_id,
                        'price_point_id': price_point_response.price_point.id,
                        'price_point_name': price_point_name
                    }
                except Exception as e:
                    print(f"Failed to create price point for {product}: {str(e)}")

            #Instantiate the subscriptions controller
            subscriptions_controller = client.subscriptions

            #Create a list to hold all the subscription components
            subscription_components = []

            #Iterate through the created_price_points dictionary to create each products subscription components
            for product, details in created_price_points.items():
                subscription_components.append(
                    CreateSubscriptionComponent(
                        component_id=details['component_id'],
                        enabled=True,
                        price_point_id=details['price_point_id']
                    )
                )

            #Make the subscription request
            subscription_body = CreateSubscriptionRequest(
                subscription=CreateSubscription(
                    product_handle='monthly-usage',  #this parameter will always be the same
                    customer_reference=customer_reference,
                    components=subscription_components
                )
            )

            subscription_result = subscriptions_controller.create_subscription(
                body=subscription_body
            )

            #Build snowflake import dataframe
            snowflake_df = pd.DataFrame()
            snowflake_df['AB_REFERENCE'] = []
            snowflake_df['AB_SUBSCRIPTION'] = []
            snowflake_df['AB_CONTRACT_ASSOC_COMPLETE'] = []

            snowflake_df.loc[0, 'AB_REFERENCE'] = customer_reference
            snowflake_df.loc[0, 'AB_SUBSCRIPTION'] = subscription_result.subscription.id
            snowflake_df['AB_SUBSCRIPTION'] = snowflake_df['AB_SUBSCRIPTION'].astype(int).astype(str)
            #Load as false, we will check in the other script
            snowflake_df['AB_CONTRACT_ASSOC_COMPLETE'] = 'FALSE'

            #Instantiate SQLAlchemy engine with the private key
            engine = create_engine(
                connection_string,
                connect_args={
                    "private_key": private_key_bytes
                }
            )

            with engine.connect() as conn:
                table_name = 'INTEGRATION_STAGING'

                #Ensure column names are consistent
                snowflake_df.columns = [col.upper() for col in snowflake_df.columns]

                #Construct the insert statement
                columns = ', '.join(snowflake_df.columns)
                placeholders = ', '.join(['%s'] * len(snowflake_df.columns))

                #Convert chunk data to list of tuples
                data_tuples = [tuple(row) for row in snowflake_df.itertuples(index=False, name=None)]

                insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

                conn.execute(insert_query,data_tuples)


    except Exception as e:
        logger.error(f"Error in lambda_handler: {e}")
        raise

    return {"statusCode": 200, "body": json.dumps({"message": "Success"})}
