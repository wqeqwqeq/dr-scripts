#!/usr/bin/env python3
import json
import argparse
from typing import Dict, List, Tuple
from AzHelper import AzureBatchPool


def get_batch_scale_configs(file_path: str = "build.json") -> List[Dict]:
    """
    Read build.json file and extract batch scale configurations.
    
    Args:
        file_path: Path to the build.json file
        
    Returns:
        List of batch scale configurations
    """
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            
        if not isinstance(data.get("batchAccountScale"), list):
            # Handle single domain case
            return [data["batchAccountScale"]]
        return data["batchAccountScale"]
    except Exception as e:
        print(f"Error reading or processing build.json: {str(e)}")
        raise

def scale_batch_pools(config_file: str, dry_run: bool = True) -> None:
    """
    Scale batch pools according to the configuration.
    
    Args:
        config_file: Path to the build.json configuration file
        dry_run: If True, only show what would be changed without making changes
    """
    # Get batch scale configurations
    batch_configs = get_batch_scale_configs(config_file)
    
    for batch_config in batch_configs:
        try:
            # Initialize batch pool clients
            scale_down_pool = AzureBatchPool(
                resource_group_name=batch_config["scaleDown"]["resourceGroup"],
                resource_name=batch_config["scaleDown"]["batch"],
                pool_name=batch_config["scaleDown"]["pool"]
            )
            
            scale_up_pool = AzureBatchPool(
                resource_group_name=batch_config["scaleUp"]["resourceGroup"],
                resource_name=batch_config["scaleUp"]["batch"],
                pool_name=batch_config["scaleUp"]["pool"]
            )
            
            # Get current node count from scale down pool
            scale_down_config = scale_down_pool.get_pool_config()
            current_nodes = scale_down_config.get('scaleSettings', {}).get('fixedScale', {}).get('targetDedicatedNodes', 0)
            
            print(f"\nProcessing scale down for {batch_config['scaleDown']['pool']} and scale up for {batch_config['scaleUp']['pool']}")
            print(f"Current node count in scale down pool: {current_nodes}")
            
            # Scale down the first pool to 0
            print(f"\nScaling down pool {batch_config['scaleDown']['pool']} to 0 nodes...")
            scale_down_pool.scale_pool_nodes(target_nodes=0, dry_run=dry_run)
            
            # Scale up the second pool to the original node count or 1 if original was 0
            target_nodes = 1 if current_nodes == 0 else current_nodes
            print(f"\nScaling up pool {batch_config['scaleUp']['pool']} to {target_nodes} nodes...")
            scale_up_pool.scale_pool_nodes(target_nodes=target_nodes, dry_run=dry_run)
            
        except Exception as e:
            print(f"Error processing batch pools: {str(e)}")
            continue

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Scale Azure Batch pools up and down')
    parser.add_argument('--config', default='build.json', help='Path to build.json configuration file')
    parser.add_argument('--dry-run', type=str, choices=['True', 'False'], default='True',
                      help='Set to True for dry run (default) or False to execute changes')
    args = parser.parse_args()

    print("Configuration:")
    print(f"Config file: {args.config}")
    print(f"Mode: {'Execute' if args.dry_run == 'False' else 'Dry Run'}\n")

    scale_batch_pools(
        config_file=args.config,
        dry_run=args.dry_run == 'True'
    )

if __name__ == "__main__":
    main() 