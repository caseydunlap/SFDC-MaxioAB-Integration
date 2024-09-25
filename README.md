# SFDC-MaxioAB-Integration
This AWS Lambda function integrates Salesforce data with Maxio Advanced Billing (AB) and Core. It processes closed won opportunities in Salesforce and creates corresponding customer records, component price points, and subscriptions in AB. It then establishes a relationsip from AB to Maxio Core.

## Process Design
![image](https://github.com/user-attachments/assets/d6cab57d-adbb-487f-a82a-1c4a034ad129)


## Features
- Retrieves Salesforce data utilzing Salesforce API
- Processes Salesforce Opportunity, Quote, Order, and related objects
- Creates or updates customer records in Maxio Advanced Billing
- Generates custom pricing tiers based on Salesforce consumption schedules
- Creates subscriptions in Maxio with appropriate components and price points
- Establishes relationship with Maxio Core customer and contract

## Prerequisites
- AWS account with Lambda access
- Salesforce developer account
- Maxio Advanced Billing account with API access
- Maxio Core Account with API access
- Python 3.9
- Snowflake login and role with write permissions
- Required Python packages (see requirements.txt)

## Configuration
Set up AWS Secrets Manager with the following secrets:
- Salesforce Client Id
- Salesforce Secret Id
- Maxio Advanced Billing API Key
- Maxio Core API Key
- Snowflake username
- Snowflake password
- Snowflake account
- Snowflake warehouse
- Snowflake schema
- Snowflake database
- Snowflake PEM key
- Snowflake PEM password

## Usage
The Lambda function is triggered by API Gateway receipt of a POST request from Salesforce. The event should contain a JSON payload with the Salesforce Opportunity ID:
![image](https://github.com/user-attachments/assets/4528fa39-9358-4f09-b7c0-ed6e17877f92)

## Function Flow
1. Salesforce Authentication and Data Retrieval
   - Authenticate with Salesforce API using OAuth 2.0
   - Retrieve Opportunity data based on the provided Opportunity ID
   - Fetch related Quote, Order, OrderItem, and Consumption Schedule data
     
2. Data Processing and Transformation
   - Extract relevant information from Salesforce objects
   - Transform data into a format suitable for AB
    
3. Maxio Customer Management/Creation
   - Check if the customer already exists in AB
   - If not, create a new AB customer record using Salesforce account information
   
4. Price Point Generation
   - For each product in the opportunity:
      - Create custom price points based on Salesforce consumption schedules
      - Map Salesforce products to corresponding AB components
        
6. Subscription Creation in Maxio Advanced Billing
   - Create a new subscription for the AB customer
   - Add components to the subscription based on the opportunity products
   - Apply the custom price points to each component

7. Store Results in Snowflake Table
   - Implement a data persistence layer, utilizing Snowflake table for robust storage and efficient retrieval of integration outcomes
     
## Asynchronous Job
1. Establish Relationship with Maxio Core
   - Leverage Maxio Core API and Snowflake to map AB subscriptions/customers to Maxio Core contracts/customers
