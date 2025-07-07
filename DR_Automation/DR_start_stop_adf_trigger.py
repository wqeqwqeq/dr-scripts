#!/usr/bin/env python3
import json
import argparse
from typing import List, Dict
from datetime import datetime
from AzHelper import ADFTrigger, AzureResourceLock

def get_adf_trigger_configs(file_path: str = "build.json") -> List[Dict]:
    """
    Read build.json file and extract ADF trigger configurations.
    
    Args:
        file_path: Path to the build.json file
        
    Returns:
        List of ADF trigger configurations
    """
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            
        if not isinstance(data.get("ADFTrigger"), list):
            # Handle single domain case
            return [data["ADFTrigger"]]
        return data["ADFTrigger"]
    except Exception as e:
        print(f"Error reading or processing build.json: {str(e)}")
        raise

def manage_adf_triggers(config_file: str, action: str, dry_run: bool = True, start_time: datetime = None) -> None:
    """
    Manage ADF triggers based on configuration.
    
    Args:
        config_file: Path to the build.json configuration file
        action: Action to perform ('start' or 'stop')
        dry_run: If True, only show what would be changed without making changes
        start_time: Optional start time for tumbling triggers. If not provided, uses current time
    """
    # Get ADF trigger configurations
    trigger_configs = get_adf_trigger_configs(config_file)
    
    # If start_time is not provided and action is start, use current time
    start_time = datetime.now() if start_time is None else start_time
    
    for config in trigger_configs:
        try:
            # Get the appropriate ADF configuration based on action
            adf_config = config[action]
            resource_group = adf_config["resourceGroup"]
            
            # Check and handle resource locks only if not in dry run mode
            lock_mgr = None
            locks = None
            if not dry_run:
                lock_mgr = AzureResourceLock(resource_group_name=resource_group)
                locks = lock_mgr.get_locks()
                if locks:
                    print(f"Found {len(locks)} locks in resource group {resource_group}")
                    print(f"Temporarily releasing {len(locks)} locks...")
                    lock_mgr.release_locks()
            
            # Initialize ADF trigger manager
            trigger_mgr = ADFTrigger(
                resource_group_name=resource_group,
                resource_name=adf_config["adf"]
            )
            
            print(f"\nProcessing ADF triggers for {adf_config['adf']} in {resource_group}")
            
            # Get all triggers
            triggers = trigger_mgr.list_triggers()
            if not triggers:
                print(f"No triggers found in ADF {adf_config['adf']}")
                continue
            
            print(f"Found {len(triggers)} triggers")
            
            if dry_run:
                print(f"What if: Would {action} all triggers in {adf_config['adf']}")
                if action == "start":
                    print(f"What if: Would reset all tumbling triggers to start at {start_time}")
                continue
            
            # Manage triggers
            trigger_mgr.manage_all_triggers(action)
            print(f"Successfully {action}ed all triggers in {adf_config['adf']}")
            
            # If action is start, reset tumbling triggers
            if action == "start":
                # Get tumbling triggers
                tumbling_triggers = trigger_mgr.list_triggers(trigger_type="TumblingWindowTrigger")
                if tumbling_triggers:
                    print(f"\nResetting {len(tumbling_triggers)} tumbling triggers to start at {start_time}")
                    for trigger in tumbling_triggers:
                        try:
                            trigger_mgr.reset_tumbling_with_start_time(trigger.name, start_time)
                            print(f"Successfully reset tumbling trigger {trigger.name}")
                        except Exception as e:
                            print(f"Error resetting tumbling trigger {trigger.name}: {str(e)}")
                else:
                    print("No tumbling triggers found to reset")
            
            # Verify triggers state
            current_triggers = trigger_mgr.list_triggers()
            for trigger in current_triggers:
                trigger_obj = trigger_mgr.client.triggers.get(
                    trigger_mgr.resource_group_name,
                    trigger_mgr.resource_name,
                    trigger.name
                )
                expected_state = "Started" if action == "start" else "Stopped"
                if trigger_obj.properties.runtime_state != expected_state:
                    print(f"Warning: Trigger {trigger.name} is not {expected_state.lower()}")
            
            # Recreate locks if not in dry run mode and locks were found
            if not dry_run and locks:
                print(f"Recreating {len(locks)} locks in resource group {resource_group}...")
                lock_mgr.recreate_locks()
                    
        except Exception as e:
            print(f"Error processing ADF {adf_config['adf']}: {str(e)}")
            # Try to recreate locks even if there was an error
            if not dry_run and locks:
                print(f"Recreating {len(locks)} locks in resource group {resource_group}...")
                lock_mgr.recreate_locks()
            continue

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Manage ADF triggers based on configuration')
    parser.add_argument('--config', default='build.json', help='Path to build.json configuration file')
    parser.add_argument('--dry-run', type=str, choices=['True', 'False'], default='True',
                      help='Set to True for dry run (default) or False to execute changes')
    parser.add_argument('--action', type=str, choices=['start', 'stop'], required=True,
                      help='Action to perform: start or stop triggers')
    parser.add_argument('--start-time', type=str,
                      help='Start time for tumbling triggers in ISO format (e.g., "2024-03-20T10:00:00")')
    args = parser.parse_args()

    # Parse start_time if provided
    start_time = None
    if args.start_time:
        try:
            start_time = datetime.fromisoformat(args.start_time)
        except ValueError as e:
            print(f"Error parsing start time: {str(e)}")
            print("Please provide start time in ISO format (e.g., '2024-03-20T10:00:00')")
            return

    print("Configuration:")
    print(f"Config file: {args.config}")
    print(f"Action: {args.action}")
    print(f"Mode: {'Execute' if args.dry_run == 'False' else 'Dry Run'}")
    if args.start_time:
        print(f"Start time: {args.start_time}")
    print()

    manage_adf_triggers(
        config_file=args.config,
        action=args.action,
        dry_run=args.dry_run == 'True',
        start_time=start_time
    )

if __name__ == "__main__":
    main() 