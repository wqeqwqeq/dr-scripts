#!/usr/bin/env python3
import json
import argparse
from typing import List, Dict, Tuple
from AzHelper import ADFManagedPrivateEndpoint

def get_adf_configs_and_mode(file_path: str = "build.json") -> Tuple[List[Dict], str]:
    """
    Read build.json file and extract ADF configurations and mode.
    
    Args:
        file_path: Path to the build.json file
        
    Returns:
        Tuple containing:
        - List of ADF configurations
        - Mode ('failover' or 'failback')
    """
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            
        # Get ADF configurations
        if not isinstance(data.get("ADFLinkedServiceFQDN"), list):
            # Handle single domain case
            adf_configs = [data["ADFLinkedServiceFQDN"]]
        else:
            adf_configs = data["ADFLinkedServiceFQDN"]
            
        # Get mode
        mode = data.get("config", {}).get("mode", "failover")
        
        return adf_configs, mode
    except Exception as e:
        print(f"Error reading or processing build.json: {str(e)}")
        raise

def manage_private_endpoints(config_file: str, dry_run: bool = True) -> None:
    """
    Manage ADF private endpoints based on configuration.
    
    Args:
        config_file: Path to the build.json configuration file
        dry_run: If True, only show what would be changed without making changes
    """
    # Define constants
    domain = "kmx-qa"
    mpe_east = "snowflake_east"
    mpe_west = "snowflake_west"
    
    # Get ADF configurations and mode
    adf_configs, mode = get_adf_configs_and_mode(config_file)
    
    print(f"\nMode: {mode}")
    
    for config in adf_configs:
        try:
            # Initialize ADF private endpoint manager
            endpoint_mgr = ADFManagedPrivateEndpoint(
                resource_group_name=config["resourceGroup"],
                resource_name=config["adf"]
            )
            
            print(f"\nProcessing ADF private endpoints for {config['adf']} in {config['resourceGroup']}")
            
            # Get current endpoint configurations
            try:
                east_endpoint = endpoint_mgr.get_managed_private_endpoint(mpe_east)
                east_fqdns = east_endpoint.get('properties', {}).get('fqdns', [])
            except Exception as e:
                print(f"Error getting {mpe_east} endpoint: {str(e)}")
                east_fqdns = []
                
            try:
                west_endpoint = endpoint_mgr.get_managed_private_endpoint(mpe_west)
                west_fqdns = west_endpoint.get('properties', {}).get('fqdns', [])
            except Exception as e:
                print(f"Error getting {mpe_west} endpoint: {str(e)}")
                west_fqdns = []
            
            if mode == "failover":
                # Remove domain from east if present
                if domain in east_fqdns:
                    if dry_run:
                        print(f"What if: Would remove {domain} from {mpe_east} in {config['adf']}")
                    else:
                        new_fqdns = [fqdn for fqdn in east_fqdns if fqdn != domain]
                        endpoint_mgr.update_managed_private_endpoint_fqdn(mpe_east, new_fqdns)
                        print(f"Removed {domain} from {mpe_east} in {config['adf']}")
                
                # Add domain to west if not present
                if domain not in west_fqdns:
                    if dry_run:
                        print(f"What if: Would add {domain} to {mpe_west} in {config['adf']}")
                    else:
                        new_fqdns = west_fqdns + [domain]
                        endpoint_mgr.update_managed_private_endpoint_fqdn(mpe_west, new_fqdns)
                        print(f"Added {domain} to {mpe_west} in {config['adf']}")
                        
            elif mode == "failback":
                # Remove domain from west if present
                if domain in west_fqdns:
                    if dry_run:
                        print(f"What if: Would remove {domain} from {mpe_west} in {config['adf']}")
                    else:
                        new_fqdns = [fqdn for fqdn in west_fqdns if fqdn != domain]
                        endpoint_mgr.update_managed_private_endpoint_fqdn(mpe_west, new_fqdns)
                        print(f"Removed {domain} from {mpe_west} in {config['adf']}")
                
                # Add domain to east if not present
                if domain not in east_fqdns:
                    if dry_run:
                        print(f"What if: Would add {domain} to {mpe_east} in {config['adf']}")
                    else:
                        new_fqdns = east_fqdns + [domain]
                        endpoint_mgr.update_managed_private_endpoint_fqdn(mpe_east, new_fqdns)
                        print(f"Added {domain} to {mpe_east} in {config['adf']}")
                    
        except Exception as e:
            print(f"Error processing ADF {config['adf']}: {str(e)}")
            continue

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Manage ADF private endpoints based on configuration')
    parser.add_argument('--config', default='build.json', help='Path to build.json configuration file')
    parser.add_argument('--dry-run', type=str, choices=['True', 'False'], default='True',
                      help='Set to True for dry run (default) or False to execute changes')
    args = parser.parse_args()

    print("Configuration:")
    print(f"Config file: {args.config}")
    print(f"Mode: {'Execute' if args.dry_run == 'False' else 'Dry Run'}\n")

    manage_private_endpoints(
        config_file=args.config,
        dry_run=args.dry_run == 'True'
    )

if __name__ == "__main__":
    main() 