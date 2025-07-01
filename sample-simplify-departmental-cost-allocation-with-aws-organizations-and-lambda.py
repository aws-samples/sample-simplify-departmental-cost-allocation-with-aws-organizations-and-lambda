import boto3
import json
from botocore.exceptions import ClientError

def get_accounts_by_ou(organizations_client, ou_id):
    """Get all accounts in an OU and its child OUs"""
    accounts = []
    
    # List accounts in current OU
    paginator = organizations_client.get_paginator('list_accounts_for_parent')
    try:
        for page in paginator.paginate(ParentId=ou_id):
            for account in page['Accounts']:
                if account['Status'] == 'ACTIVE':
                    accounts.append({
                        'Id': account['Id'],
                        'Name': account['Name']
                    })
    except ClientError as e:
        print(f"Error getting accounts for OU {ou_id}: {str(e)}")
        return accounts

    # List child OUs and get their accounts recursively
    try:
        paginator = organizations_client.get_paginator('list_children')
        for page in paginator.paginate(ParentId=ou_id, ChildType='ORGANIZATIONAL_UNIT'):
            for child in page['Children']:
                child_accounts = get_accounts_by_ou(organizations_client, child['Id'])
                accounts.extend(child_accounts)
    except ClientError as e:
        print(f"Error getting child OUs for {ou_id}: {str(e)}")

    return accounts

def get_ou_details(organizations_client, ou_id):
    """Get OU name and details"""
    try:
        response = organizations_client.describe_organizational_unit(
            OrganizationalUnitId=ou_id
        )
        return response['OrganizationalUnit']['Name']
    except ClientError as e:
        print(f"Error getting OU details for {ou_id}: {str(e)}")
        return None

def create_or_update_cost_category(ce_client, ou_name, account_ids):
    """Create or update cost category for an OU"""
    category_name = f"OU-{ou_name}"
    
    try:
        # Check if cost category exists
        existing_categories = ce_client.list_cost_category_definitions()
        category_exists = any(cat['Name'] == category_name 
                            for cat in existing_categories.get('CostCategoryReferences', []))

        # Prepare the rules structure
        rules = [{
            'Value': ou_name,
            'Rule': {
                'Dimensions': {
                    'Key': 'LINKED_ACCOUNT',
                    'Values': account_ids,
                    'MatchOptions': ['EQUALS']
                }
            }
        }]

        if category_exists:
            # Update existing category
            ce_client.update_cost_category_definition(
                CostCategoryArn=[cat['CostCategoryArn'] 
                                for cat in existing_categories['CostCategoryReferences'] 
                                if cat['Name'] == category_name][0],
                RuleVersion='CostCategoryExpression.v1',
                Rules=rules
            )
            print(f"Updated cost category: {category_name}")
        else:
            # Create new category
            ce_client.create_cost_category_definition(
                Name=category_name,
                RuleVersion='CostCategoryExpression.v1',
                Rules=rules,
                DefaultValue='Other'
            )
            print(f"Created cost category: {category_name}")

    except ClientError as e:
        print(f"Error managing cost category {category_name}: {str(e)}")

def lambda_handler(event, context):
    organizations_client = boto3.client('organizations')
    ce_client = boto3.client('ce')
    
    try:
        # Get root OU ID
        root_response = organizations_client.list_roots()
        root_id = root_response['Roots'][0]['Id']
        
        # Get all top-level OUs
        paginator = organizations_client.get_paginator('list_children')
        for page in paginator.paginate(ParentId=root_id, ChildType='ORGANIZATIONAL_UNIT'):
            for ou in page['Children']:
                # Get OU name
                ou_name = get_ou_details(organizations_client, ou['Id'])
                if not ou_name:
                    continue
                
                # Get accounts in this OU
                accounts = get_accounts_by_ou(organizations_client, ou['Id'])
                account_ids = [account['Id'] for account in accounts]
                
                if account_ids:
                    # Create or update cost category for this OU
                    create_or_update_cost_category(ce_client, ou_name, account_ids)
        
        return {
            'statusCode': 200,
            'body': json.dumps('Cost categories updated successfully')
        }
        
    except ClientError as e:
        print(f"Error in lambda execution: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
