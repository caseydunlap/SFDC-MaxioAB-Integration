# SFDC-MaxioAB-Integration
This AWS Lambda function integrates Salesforce data with Maxio Advanced Billing and Core. It processes closed won opportunities in Salesforce and creates corresponding customer records, component price points, and subscriptions in Maxio Advanced Billing. It then establishes a relationsip from AB to Maxio Core.

## Features
- Retrieves Salesforce data utilzing Salesforce API
- Processes Salesforce Opportunity, Quote, Order, and related objects
- Creates or updates customer records in Maxio Advanced Billing
- Generates custom pricing tiers based on Salesforce consumption schedules
- Creates subscriptions in Maxio with appropriate components and price points

## Prerequisites
AWS account with Lambda access
Salesforce developer account
Maxio Advanced Billing account
Python 3.9
Required Python packages (see requirements.txt)

## Configuration
Set up AWS Secrets Manager with the following secrets:
- Salesforce Client Id
- Salesforce Secret Id
- Maxio Advanced Billing API Key
- Maxio Core API Key

## Usage
The Lambda function is triggered by an API Gateway event. The event should contain a JSON payload with the Salesforce Opportunity ID:
![image](https://github.com/user-attachments/assets/67f51bff-7d73-4608-875f-871a9fe0ad83)

Function Flow

Authenticate with Salesforce API
Retrieve Opportunity, Quote, Order, and related data from Salesforce
Process and transform Salesforce data
Check if customer exists in Maxio, create if necessary
Generate custom price points based on Salesforce consumption schedules
Create subscription in Maxio with appropriate components and price points

Error Handling
The function includes basic error handling and logging. Errors are logged to CloudWatch for troubleshooting.

