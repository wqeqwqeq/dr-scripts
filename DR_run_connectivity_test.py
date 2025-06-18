#!/usr/bin/env python3
import json
import argparse
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from AzHelper import ADFPipeline

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

def run_connectivity_tests(config_file: str, parameters: Dict = None) -> None:
    """
    Run Snowflake connectivity test pipeline across ADF instances based on configuration.
    
    Args:
        config_file (str): Path to the build.json configuration file
        parameters (Dict): Optional parameters to pass to the pipeline
    """
    # Get the ADF configurations
    adf_configs = get_adf_configs(config_file)
    
    # Pipeline and activity names
    pipeline_name = "PPL_Snowflake_connectivitytest"
    activity_name = "Snowflake connectivity test"
    
    # Track results across all ADFs
    all_results = {}

    # Process each ADF
    for adf_config in adf_configs:
        resource_group = adf_config['resourceGroup']
        factory_name = adf_config['adf']
        
        print(f"\nProcessing ADF: {factory_name} in Resource Group: {resource_group}")
        print("=" * 80)
        
        # Initialize ADFPipeline
        pipeline_runner = ADFPipeline(
            resource_group_name=resource_group,
            resource_name=factory_name
        )

        try:
            print(f"Running connectivity test pipeline: {pipeline_name}")
            
            # Run pipeline and fetch specific activity result
            activity_result = pipeline_runner.run_and_fetch(
                pipeline_name=pipeline_name,
                activity_name=activity_name,
                parameters=parameters
            )
            
            # Store result for this ADF
            all_results[f"{resource_group}/{factory_name}"] = {
                'status': 'success',
                'run_id': pipeline_runner.run_id,
                'activity_result': activity_result
            }
            
            # Print summary for this ADF
            print(f"\n Connectivity test completed successfully")
            print(f"Run ID: {pipeline_runner.run_id}")
            print(f"Activity Status: {activity_result.get('status', 'Unknown')}")
            
            # Print activity output if available
            if 'output' in activity_result:
                print(f"Activity Output: {json.dumps(activity_result['output'], indent=2)}")
            
        except Exception as e:
            print(f" Error running connectivity test in {factory_name}: {str(e)}")
            all_results[f"{resource_group}/{factory_name}"] = {
                'status': 'error',
                'error': str(e),
                'run_id': getattr(pipeline_runner, 'run_id', None)
            }
    
    # Print overall summary
    print("\n" + "=" * 80)
    print("CONNECTIVITY TEST SUMMARY")
    print("=" * 80)
    
    successful_adfs = []
    failed_adfs = []
    
    for adf_key, result in all_results.items():
        if result['status'] == 'success':
            successful_adfs.append(adf_key)
            print(f" {adf_key}: SUCCESS (Run ID: {result['run_id']})")
        else:
            failed_adfs.append(adf_key)
            print(f" {adf_key}: FAILED - {result['error']}")
    
    print(f"\nTotal ADFs processed: {len(all_results)}")
    print(f"Successful: {len(successful_adfs)}")
    print(f"Failed: {len(failed_adfs)}")
    
    if failed_adfs:
        print(f"\nFailed ADFs: {', '.join(failed_adfs)}")


def main():
    parser = argparse.ArgumentParser(description='Run Snowflake connectivity test pipeline across ADF instances')
    parser.add_argument('--config', default='build.json', help='Path to build.json configuration file')

    args = parser.parse_args()


    # Run in single ADF mode or batch mode
    run_connectivity_tests(
        config_file=args.config,
    )

if __name__ == "__main__":
    main() 