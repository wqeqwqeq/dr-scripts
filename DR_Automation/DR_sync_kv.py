#!/usr/bin/env python3
import json
import argparse
from typing import List, Dict
from AzHelper import AzureKeyVault

def get_kv_sync_configs(file_path: str = "build.json") -> tuple[List[Dict], Dict]:
    """
    Read build.json file and extract key vault sync configurations and config data.
    
    Args:
        file_path: Path to the build.json file
        
    Returns:
        Tuple containing:
        - List of key vault sync configurations
        - Config data dictionary
    """
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            
        kv_sync = data.get("kvSync", [])
        if not isinstance(kv_sync, list):
            # Handle single domain case
            kv_sync = [kv_sync]
            
        return kv_sync, data.get("config", {})
    except Exception as e:
        print(f"Error reading or processing build.json: {str(e)}")
        raise

def sync_key_vaults(config_file: str, dry_run: bool = True) -> None:
    """
    Sync secrets between key vaults based on configuration.
    
    Args:
        config_file: Path to the build.json configuration file
        dry_run: If True, only show what would be changed without making changes
    """
    # Get key vault sync configurations and mode
    kv_configs, config = get_kv_sync_configs(config_file)
    
    if config.get("mode") == "failback":
        print("Skip syncing in failback mode")
        return
    
    for kv_config in kv_configs:
        try:
            # Initialize source and target key vault clients
            source_kv = AzureKeyVault(
                resource_group_name=kv_config["from"]["resourceGroup"],
                resource_name=kv_config["from"]["kv"],
                resource_type='keyvault'
            )
            
            target_kv = AzureKeyVault(
                resource_group_name=kv_config["to"]["resourceGroup"],
                resource_name=kv_config["to"]["kv"],
                resource_type='keyvault'
            )
            
            print(f"\nProcessing key vault sync from {kv_config['from']['kv']} to {kv_config['to']['kv']}")
            
            # Get all secrets from source vault
            secrets = source_kv.list_secrets()
            if not secrets:
                print(f"No secrets found in source vault {kv_config['from']['kv']}")
                continue
            
            # Copy each secret to target vault
            for secret in secrets:
                secret_name = secret['name']
                print(f"\nProcessing secret: {secret_name}")
                
                try:
                    # Get secret value from source vault
                    source_value = source_kv.get_secret(secret_name)
                    
                    # Try to get the secret from target vault to compare
                    try:
                        target_value = target_kv.get_secret(secret_name)
                        if source_value == target_value:
                            print(f"Skipping {secret_name} - values are identical")
                            continue
                    except Exception:
                        # If secret doesn't exist in target vault, continue with copying
                        pass
                    
                    if dry_run:
                        print(f"What if: Would copy secret {secret_name} from {kv_config['from']['kv']} to {kv_config['to']['kv']}")
                        continue
                    
                    # Set secret in target vault
                    target_kv.set_secret(secret_name, source_value)
                    
                except Exception as e:
                    print(f"Error processing secret {secret_name}: {str(e)}")
                    continue
                    
        except Exception as e:
            print(f"Error processing key vaults: {str(e)}")
            continue

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Sync secrets between Azure Key Vaults')
    parser.add_argument('--config', default='build.json', help='Path to build.json configuration file')
    parser.add_argument('--dry-run', type=str, choices=['True', 'False'], default='True',
                      help='Set to True for dry run (default) or False to execute changes')
    args = parser.parse_args()

    print("Configuration:")
    print(f"Config file: {args.config}")
    print(f"Mode: {'Execute' if args.dry_run == 'False' else 'Dry Run'}\n")

    sync_key_vaults(
        config_file=args.config,
        dry_run=args.dry_run == 'True'
    )

if __name__ == "__main__":
    main() 