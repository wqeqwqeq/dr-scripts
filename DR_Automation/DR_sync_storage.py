#!/usr/bin/env python3
import json
import argparse
from typing import List, Dict
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from azure_tools.storage import AzureStorageCopy

def get_stg_sync_configs(file_path: str = "build.json") -> tuple[List[Dict], Dict]:
    """
    Read build.json file and extract storage sync configurations and config data.
    
    Args:
        file_path: Path to the build.json file
        
    Returns:
        Tuple containing:
        - List of storage sync configurations
        - Config data dictionary
    """
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            
        stg_sync = data.get("StgSync", [])
        if not isinstance(stg_sync, list):
            # Handle single domain case
            stg_sync = [stg_sync]
            
        return stg_sync, data.get("config", {})
    except Exception as e:
        print(f"Error reading or processing build.json: {str(e)}")
        raise

def sync_storage_accounts(config_file: str, dry_run: bool = True) -> None:
    """
    Sync containers between storage accounts based on configuration.
    
    Args:
        config_file: Path to the build.json configuration file
        dry_run: If True, only show what would be changed without making changes
    """
    # Get storage sync configurations and mode
    stg_configs, config = get_stg_sync_configs(config_file)
    
    if config.get("mode") == "failback":
        print("Skip syncing in failback mode")
        return
    
    # Define the containers to sync
    containers_to_sync = ["batch-pool", "scriptfiles"]
    
    for stg_config in stg_configs:
        try:
            # Initialize storage copy client
            storage_copy = AzureStorageCopy(
                sourceStgName=stg_config["from"]["stg"],
                targetStgName=stg_config["to"]["stg"]
            )
            
            print(f"\nProcessing storage sync from {stg_config['from']['stg']} to {stg_config['to']['stg']}")
            
            # Copy each container
            for container_name in containers_to_sync:
                print(f"\nProcessing container: {container_name}")
                
                try:
                    # Check if source container exists and has blobs
                    try:
                        source_blobs = storage_copy.sourceSTG.list_blobs(container_name)
                        if not source_blobs:
                            print(f"No blobs found in source container {container_name}")
                            continue
                    except Exception as e:
                        print(f"Source container {container_name} not found or inaccessible: {str(e)}")
                        continue
                    
                    if dry_run:
                        print(f"What if: Would copy container {container_name} from {stg_config['from']['stg']} to {stg_config['to']['stg']}")
                        print(f"What if: Would copy {len(source_blobs)} blobs")
                        continue
                    
                    # Copy container to target storage account
                    storage_copy.copy_container(
                        source_container=container_name,
                        target_container=container_name,
                        overwrite=True
                    )
                    
                except Exception as e:
                    print(f"Error processing container {container_name}: {str(e)}")
                    continue
                    
        except Exception as e:
            print(f"Error processing storage accounts: {str(e)}")
            continue

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Sync containers between Azure Storage Accounts')
    parser.add_argument('--config', default='build.json', help='Path to build.json configuration file')
    parser.add_argument('--dry-run', type=str, choices=['True', 'False'], default='True',
                      help='Set to True for dry run (default) or False to execute changes')
    args = parser.parse_args()

    print("Configuration:")
    print(f"Config file: {args.config}")
    print(f"Mode: {'Execute' if args.dry_run == 'False' else 'Dry Run'}\n")

    sync_storage_accounts(
        config_file=args.config,
        dry_run=args.dry_run == 'True'
    )

if __name__ == "__main__":
    main() 