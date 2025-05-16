#!/usr/bin/env python3
import json
import argparse
from adfHelper import ADFLinkedServices,get_subscription_id

def update_snowflake_fqdns(config_file: str, old_fqdn: str, new_fqdn: str, dry_run: bool = True):
    """
    Update Snowflake FQDNs in ADF linked services based on configuration.
    
    Args:
        config_file (str): Path to the build.json configuration file
        old_fqdn (str): The old FQDN to replace
        new_fqdn (str): The new FQDN to use
        dry_run (bool): If True, only show what would be changed without making changes
    """
    # Read the configuration file
    with open(config_file, 'r') as f:
        config = json.load(f)

    subscription_id = get_subscription_id()

    # Get the ADF configurations
    adf_configs = config.get('ADFLinkedServiceFQDN', [])
    if not isinstance(adf_configs, list):
        adf_configs = [adf_configs]  # Convert single config to list

    # Process each ADF
    for adf_config in adf_configs:
        resource_group = adf_config['resourceGroup']
        factory_name = adf_config['adf']
        
        print(f"\nProcessing ADF: {factory_name} in Resource Group: {resource_group}")
        
        # Initialize ADFLinkedServices

        linked_services = ADFLinkedServices(
            subscription_id=subscription_id,
            resource_group_name=resource_group,
            factory_name=factory_name
        )

        # Get all Snowflake linked services
        snowflake_services = linked_services.list_linked_services(filter_by_type='Snowflake')
        
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
    parser.add_argument('--old-fqdn', required=True, help='Old FQDN to replace')
    parser.add_argument('--new-fqdn', required=True, help='New FQDN to use')
    parser.add_argument('--execute', action='store_true', help='Execute the changes (default is dry run)')

    args = parser.parse_args()

    print("Configuration:")
    print(f"Config file: {args.config}")
    print(f"Old FQDN: {args.old_fqdn}")
    print(f"New FQDN: {args.new_fqdn}")
    print(f"Mode: {'Execute' if args.execute else 'Dry Run'}")

    update_snowflake_fqdns(
        config_file=args.config,
        old_fqdn=args.old_fqdn,
        new_fqdn=args.new_fqdn,
        dry_run=not args.execute
    )

if __name__ == "__main__":
    main() 