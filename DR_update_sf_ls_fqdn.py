#!/usr/bin/env python3
import json
import argparse
from typing import List, Dict
from AzHelper import ADFLinkedServices

def get_adf_configs(file_path: str = "build.json") -> List[Dict]:
    """
    Read build.json file and extract ADF configurations.
    
    Args:
        file_path: Path to the build.json file
        
    Returns:
        List of ADF configurations
    """
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            
        if not isinstance(data.get("ADFLinkedServiceFQDN"), list):
            # Handle single domain case
            return [data["ADFLinkedServiceFQDN"]]
        return data["ADFLinkedServiceFQDN"]
    except Exception as e:
        print(f"Error reading or processing build.json: {str(e)}")
        raise

def update_snowflake_fqdns(config_file: str, old_fqdn: str, new_fqdn: str, dry_run: bool = True) -> None:
    """
    Update Snowflake FQDNs in ADF linked services based on configuration.
    
    Args:
        config_file (str): Path to the build.json configuration file
        old_fqdn (str): The old FQDN to replace
        new_fqdn (str): The new FQDN to use
        dry_run (bool): If True, only show what would be changed without making changes
    """
    # Get the ADF configurations
    adf_configs = get_adf_configs(config_file)

    # Process each ADF
    for adf_config in adf_configs:
        resource_group = adf_config['resourceGroup']
        factory_name = adf_config['adf']
        
        print(f"\nProcessing ADF: {factory_name} in Resource Group: {resource_group}")
        
        # Initialize ADFLinkedServices with new structure
        linked_services = ADFLinkedServices(
            resource_group_name=resource_group,
            resource_name=factory_name,
            resource_type='adf'
        )

        # Get all Snowflake linked services
        snowflake_services = linked_services.list_linked_services(filter_by_type=['Snowflake','SnowflakeV2'])
        
        if not snowflake_services:
            print(f"No Snowflake linked services found in {factory_name}")
            continue

        # Update each Snowflake linked service
        for service in snowflake_services:
            service_name = service['name']
            print(f"\nUpdating Snowflake linked service: {service_name}")
            
            try:
                linked_services.update_linked_service_sf_account(
                    linked_service_name=service_name,
                    old_fqdn=old_fqdn,
                    new_fqdn=new_fqdn,
                    dry_run=dry_run
                )
            except Exception as e:
                print(f"Error updating {service_name}: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description='Update Snowflake FQDNs in ADF linked services')
    parser.add_argument('--config', default='build.json', help='Path to build.json configuration file')
    parser.add_argument('--old-fqdn', required=True, help='Old Snowflake account FQDN')
    parser.add_argument('--new-fqdn', required=True, help='New Snowflake account FQDN')
    parser.add_argument('--dry-run', type=str, choices=['True', 'False'], default='True',
                      help='Set to True for dry run (default) or False to execute changes')

    args = parser.parse_args()

    print("Configuration:")
    print(f"Config file: {args.config}")
    print(f"Old FQDN: {args.old_fqdn}")
    print(f"New FQDN: {args.new_fqdn}")
    print(f"Mode: {'Execute' if args.dry_run == 'False' else 'Dry Run'}\n")

    update_snowflake_fqdns(
        config_file=args.config,
        old_fqdn=args.old_fqdn,
        new_fqdn=args.new_fqdn,
        dry_run=args.dry_run == 'True'
    )

if __name__ == "__main__":
    main() 