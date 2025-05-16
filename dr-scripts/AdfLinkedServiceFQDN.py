import json
import argparse
import tempfile
import os
from typing import Dict, List, Optional, Union
from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import LinkedServiceResource

def get_linked_service_by_adf(
    resource_group_name: str,
    factory_name: str,
    linked_service_name: str
) -> Dict:
    """
    Get linked service details from Azure Data Factory.
    
    Args:
        resource_group_name: Name of the resource group
        factory_name: Name of the data factory
        linked_service_name: Name of the linked service
        
    Returns:
        Dict containing the linked service details
    """
    try:
        # Get credentials and create client
        credential = DefaultAzureCredential()
        subscription_id = os.environ.get('AZURE_SUBSCRIPTION_ID')
        client = DataFactoryManagementClient(credential, subscription_id)
        
        # Get the linked service
        response = client.linked_services.get(
            resource_group_name=resource_group_name,
            factory_name=factory_name,
            linked_service_name=linked_service_name
        )
        
        return response.as_dict()
    except Exception as e:
        print(f"Error getting linked service details: {str(e)}")
        raise

def get_snowflake_linked_services(
    adf_name: str,
    resource_group_name: str
) -> List[Dict]:
    """
    Get all Snowflake linked services from an Azure Data Factory.
    
    Args:
        adf_name: Name of the data factory
        resource_group_name: Name of the resource group
        
    Returns:
        List of Snowflake linked service details
    """
    try:
        # Get credentials and create client
        credential = DefaultAzureCredential()
        subscription_id = os.environ.get('AZURE_SUBSCRIPTION_ID')
        client = DataFactoryManagementClient(credential, subscription_id)
        
        # Get all linked services
        linked_services = client.linked_services.list_by_factory(
            resource_group_name=resource_group_name,
            factory_name=adf_name
        )
        
        # Filter for Snowflake services
        snowflake_services = []
        for linked_service in linked_services:
            service_details = get_linked_service_by_adf(
                resource_group_name=resource_group_name,
                factory_name=adf_name,
                linked_service_name=linked_service.name
            )
            
            if service_details['properties']['type'] in ['Snowflake', 'SnowflakeV2']:
                snowflake_services.append(service_details)
        
        return snowflake_services
    except Exception as e:
        print(f"Error getting Snowflake linked services: {str(e)}")
        raise

def update_snowflake_account_name(
    linked_service: Dict,
    old_fqdn: str,
    new_fqdn: str
) -> Dict:
    """
    Update the account name in the Snowflake connection string.
    
    Args:
        linked_service: Linked service details
        old_fqdn: Old FQDN to replace
        new_fqdn: New FQDN to use
        
    Returns:
        Updated linked service details
    """
    import re
    
    # For Snowflake V1
    if linked_service['properties']['type'] == 'Snowflake':
        connection_string = linked_service['properties']['typeProperties']['connectionString']
        new_connection_string = re.sub(
            f"(?<=://){re.escape(old_fqdn)}(?=\.)",
            new_fqdn,
            connection_string
        )
        print(f"ConnectionString: {new_connection_string}")
        linked_service['properties']['typeProperties']['connectionString'] = new_connection_string
    else:
        # For Snowflake V2
        current_identifier = linked_service['properties']['typeProperties']['accountIdentifier']
        new_identifier = re.sub(
            f"(?<=://){re.escape(old_fqdn)}(?=\.)",
            new_fqdn,
            current_identifier
        )
        linked_service['properties']['typeProperties']['accountIdentifier'] = new_identifier
    
    return linked_service

def deploy_snowflake_linked_service(
    linked_service: Dict,
    adf_name: str,
    resource_group_name: str,
    dry_run: bool = True
) -> None:
    """
    Deploy the updated Snowflake linked service.
    
    Args:
        linked_service: Linked service details
        adf_name: Name of the data factory
        resource_group_name: Name of the resource group
        dry_run: If True, only show what would be done without making changes
    """
    try:
        # Get credentials and create client
        credential = DefaultAzureCredential()
        subscription_id = os.environ.get('AZURE_SUBSCRIPTION_ID')
        client = DataFactoryManagementClient(credential, subscription_id)
        
        # Create a temporary file for the linked service definition
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as temp_file:
            json.dump(linked_service, temp_file, indent=2)
            temp_file_path = temp_file.name
        
        try:
            # Create LinkedServiceResource object
            linked_service_resource = LinkedServiceResource(properties=linked_service['properties'])
            
            if dry_run:
                print(f"What if: Would update linked service {linked_service['name']} in {adf_name}")
            else:
                client.linked_services.create_or_update(
                    resource_group_name=resource_group_name,
                    factory_name=adf_name,
                    linked_service_name=linked_service['name'],
                    linked_service=linked_service_resource
                )
                print(f"Updated linked service {linked_service['name']} in {adf_name}")
        finally:
            # Clean up the temporary file
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
    except Exception as e:
        print(f"Error deploying linked service: {str(e)}")
        raise

def main():
    parser = argparse.ArgumentParser(description='Update Snowflake linked services in Azure Data Factory')
    parser.add_argument('--dry-run', action='store_true', default=True,
                      help='Show what would be done without making changes (default: True)')
    parser.add_argument('--old-fqdn', required=True,
                      help='Old FQDN to replace')
    parser.add_argument('--new-fqdn', required=True,
                      help='New FQDN to use')
    parser.add_argument('--config-path', default='..',
                      help='Path to the build.json file (default: ..)')
    
    args = parser.parse_args()
    
    # Read the JSON file
    config_path = os.path.join(args.config_path, 'build.json')
    with open(config_path, 'r') as f:
        json_content = json.load(f)
    
    # Check if ADFLinkedServiceFQDN exists
    if 'ADFLinkedServiceFQDN' in json_content:
        adf_values = json_content['ADFLinkedServiceFQDN']
        
        # Handle both list and single value cases
        if isinstance(adf_values, list):
            for adf_config in adf_values:
                print(f"Processing ADF Linked Service: {adf_config['adf']} with Resource Group: {adf_config['resourceGroup']}")
                
                # Get Snowflake linked services
                snowflake_services = get_snowflake_linked_services(
                    adf_name=adf_config['adf'],
                    resource_group_name=adf_config['resourceGroup']
                )
                
                # Update and deploy each service
                for service in snowflake_services:
                    updated_service = update_snowflake_account_name(
                        linked_service=service,
                        old_fqdn=args.old_fqdn,
                        new_fqdn=args.new_fqdn
                    )
                    
                    deploy_snowflake_linked_service(
                        linked_service=updated_service,
                        adf_name=adf_config['adf'],
                        resource_group_name=adf_config['resourceGroup'],
                        dry_run=args.dry_run
                    )
        else:
            print(f"Processing ADF Linked Service: {adf_values['adf']} with Resource Group: {adf_values['resourceGroup']}")
            
            # Get Snowflake linked services
            snowflake_services = get_snowflake_linked_services(
                adf_name=adf_values['adf'],
                resource_group_name=adf_values['resourceGroup']
            )
            
            # Update and deploy each service
            for service in snowflake_services:
                updated_service = update_snowflake_account_name(
                    linked_service=service,
                    old_fqdn=args.old_fqdn,
                    new_fqdn=args.new_fqdn
                )
                
                deploy_snowflake_linked_service(
                    linked_service=updated_service,
                    adf_name=adf_values['adf'],
                    resource_group_name=adf_values['resourceGroup'],
                    dry_run=args.dry_run
                )
    else:
        print("No ADFLinkedServiceFQDN found in build.json")

if __name__ == "__main__":
    main() 