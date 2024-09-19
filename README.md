# SFDC-MaxioAB-Integration
This AWS Lambda function integrates Salesforce data with Maxio Advanced Billing and Core. It processes closed won opportunities in Salesforce and creates corresponding customer records, component price points, and subscriptions in Maxio Advanced Billing. It then establishes a relationsip from AB to Maxio Core.

## Process Design
![image](https://github.com/user-attachments/assets/60c86327-45a2-4f1d-94df-dda2257f418b)

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
![image](https://github.com/user-attachments/assets/4528fa39-9358-4f09-b7c0-ed6e17877f92)

## Function Flow
1. Salesforce Authentication and Data Retrieval
   - Authenticate with Salesforce API using OAuth 2.0
   - Retrieve Opportunity data based on the provided Opportunity ID
   - Fetch related Quote, Order, OrderItem, and Consumption Schedule data
     
2. Data Processing and Transformation
  - Extract relevant information from Salesforce objects
  - Transform data into a format suitable for Maxio Advanced Billing
    
3. Maxio Customer Management/Creation
  - Check if the customer already exists in Maxio Advanced Billing
  - If not, create a new customer record using Salesforce account information
   
4. Price Point Generation
  - For each product in the opportunity:
      - Create custom price points based on Salesforce consumption schedules
      - Map Salesforce products to corresponding Maxio components
        
5. Subscription Creation in Maxio Advanced Billing
 - Create a new subscription for the AB customer
 - Add components to the subscription based on the opportunity products
 - Apply the custom price points to each component

6. Maxio Core Transaction Integration
  - Utilize Maxio Core API to try to establish relationship with associated Maxio Core customer transaction
