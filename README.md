# SFDC-MaxioAB-Integration
## Design
![image](https://github.com/user-attachments/assets/67f51bff-7d73-4608-875f-871a9fe0ad83)

Salesforce to Maxio Integration Lambda Function
This AWS Lambda function integrates Salesforce data with Maxio Advanced Billing (formerly Chargify). It processes closed opportunities in Salesforce and creates corresponding customer records and subscriptions in Maxio.
Features

Retrieves Salesforce data using OAuth 2.0 authentication
Processes Salesforce Opportunity, Quote, Order, and related objects
Creates or updates customer records in Maxio Advanced Billing
Generates custom pricing tiers based on Salesforce consumption schedules
Creates subscriptions in Maxio with appropriate components and price points

Prerequisites

AWS account with Lambda access
Salesforce developer account
Maxio Advanced Billing account
Python 3.8+
Required Python packages (see requirements.txt)

Configuration

Set up AWS Secrets Manager with the following secrets:

sfdc_non_prod_client_id
sfdc_non_prod_secret_id
maxio_non_prod_api_key


Configure Salesforce Connected App for OAuth 2.0 authentication
Set up Maxio Advanced Billing API access

Usage
The Lambda function is triggered by an API Gateway event. The event should contain a JSON payload with the Salesforce Opportunity ID:
![image](https://github.com/user-attachments/assets/25bbeac0-9342-4e4b-a3c5-d0454d592ada)

Function Flow

Authenticate with Salesforce API
Retrieve Opportunity, Quote, Order, and related data from Salesforce
Process and transform Salesforce data
Check if customer exists in Maxio, create if necessary
Generate custom price points based on Salesforce consumption schedules
Create subscription in Maxio with appropriate components and price points

Error Handling
The function includes basic error handling and logging. Errors are logged to CloudWatch for troubleshooting.

