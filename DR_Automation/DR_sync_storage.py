#!/usr/bin/env python3
import json
import argparse
from typing import Dict
import sys
import os

# Add the parent directory to the path to import azure_tools
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from azure_tools.storage import AzureStorageCopy


def get_stg_sync_config(file_path: str = "build.json") -> Dict:
    """
    Read build.json file and extract StgSync configuration.
    
    Args:
        file_path: Path to the build.json file
        
    Returns:
        StgSync configuration dictionary
    """
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            
        if "StgSync" not in data:
            raise ValueError("StgSync configuration not found in build.json")
            
        stg_sync_config = data["StgSync"]
        
        # Validate required fields
        required_fields = ["from", "to"]
        for field in required_fields:
            if field not in stg_sync_config:
                raise ValueError(f"Required field '{field}' not found in StgSync configuration")
                
        for direction in ["from", "to"]:
            if "stg" not in stg_sync_config[direction]:
                raise ValueError(f"Storage account name 'stg' not found in StgSync.{direction}")
                
        return stg_sync_config
        
    except Exception as e:
        print(f"Error reading or processing build.json: {str(e)}")
        raise


def run_storage_sync(config_file: str, containers: list = None) -> None:
    """
    Synchronize storage containers between source and target storage accounts based on configuration.
    
    Args:
        config_file (str): Path to the build.json configuration file
        containers (list): List of container names to sync. Defaults to ['batch-pool', 'scriptfiles']
    """
    # Default containers to sync
    if containers is None:
        containers = ['batch-pool', 'scriptfiles']
    
    # Get the StgSync configuration
    stg_sync_config = get_stg_sync_config(config_file)
    
    # Extract source and target storage account names
    source_stg_name = stg_sync_config['from']['stg']
    target_stg_name = stg_sync_config['to']['stg']
    source_rg = stg_sync_config['from']['resourceGroup']
    target_rg = stg_sync_config['to']['resourceGroup']
    
    print(f"Starting storage synchronization")
    print(f"Source Storage: {source_stg_name} (Resource Group: {source_rg})")
    print(f"Target Storage: {target_stg_name} (Resource Group: {target_rg})")
    print(f"Containers to sync: {', '.join(containers)}")
    print("=" * 80)
    
    # Track results for each container
    sync_results = {}
    
    try:
        # Initialize AzureStorageCopy
        print(f"Initializing storage copy operation...")
        storage_copy = AzureStorageCopy(
            sourceStgName=source_stg_name,
            targetStgName=target_stg_name
        )
        print(f"✓ Storage copy client initialized successfully")
        
        # Process each container
        for container_name in containers:
            print(f"\n--- Processing container: {container_name} ---")
            
            try:
                print(f"Copying container '{container_name}' from {source_stg_name} to {target_stg_name}")
                
                # Copy the container
                storage_copy.copy_container(
                    source_container=container_name,
                    target_container=container_name,
                    overwrite=True
                )
                
                # Record success
                sync_results[container_name] = {
                    'status': 'success',
                    'message': f'Container copied successfully'
                }
                
                print(f"✓ Container '{container_name}' copied successfully")
                
            except Exception as e:
                print(f"✗ Error copying container '{container_name}': {str(e)}")
                sync_results[container_name] = {
                    'status': 'error',
                    'message': str(e)
                }
                
    except Exception as e:
        print(f"✗ Error initializing storage copy operation: {str(e)}")
        # Mark all containers as failed if initialization fails
        for container_name in containers:
            sync_results[container_name] = {
                'status': 'error',
                'message': f'Storage copy initialization failed: {str(e)}'
            }
    
    # Print overall summary
    print("\n" + "=" * 80)
    print("STORAGE SYNCHRONIZATION SUMMARY")
    print("=" * 80)
    
    successful_containers = []
    failed_containers = []
    
    for container_name, result in sync_results.items():
        if result['status'] == 'success':
            successful_containers.append(container_name)
            print(f"✓ {container_name}: SUCCESS")
        else:
            failed_containers.append(container_name)
            print(f"✗ {container_name}: FAILED - {result['message']}")
    
    print(f"\nTotal containers processed: {len(sync_results)}")
    print(f"Successful: {len(successful_containers)}")
    print(f"Failed: {len(failed_containers)}")
    
    if failed_containers:
        print(f"\nFailed containers: {', '.join(failed_containers)}")
        
    if successful_containers:
        print(f"Successfully synced containers: {', '.join(successful_containers)}")


def main():
    parser = argparse.ArgumentParser(description='Synchronize storage containers between source and target storage accounts')
    parser.add_argument('--config', default='build.json', help='Path to build.json configuration file')
    parser.add_argument('--containers', nargs='+', default=['batch-pool', 'scriptfiles'], 
                       help='List of container names to sync (default: batch-pool scriptfiles)')

    args = parser.parse_args()

    # Run storage synchronization
    run_storage_sync(
        config_file=args.config,
        containers=args.containers
    )


if __name__ == "__main__":
    main() 