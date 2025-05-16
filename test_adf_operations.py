from adfHelper import ADFLinkedServices, ADFIntegrationRuntime
import json

def test_linked_services():
    # Initialize the ADFLinkedServices class
    linked_services = ADFLinkedServices(
        subscription_id="ee5f77a1-2e59-4335-8bdf-f7ea476f6523",
        resource_group_name="adf",
        factory_name="adf-stanley"
    )

    # Test 1: Get detailed information about all linked services
    print("\nTest 1: Get detailed information about all linked services")
    all_services = linked_services.list_linked_services()
    for service in all_services:
        service_name = service['name']
        print(f"\nGetting details for {service_name}:")
        try:
            details = linked_services.get_linked_service_details(service_name)
            print(f"Type: {details.get('properties', {}).get('type', 'Unknown')}")
            print(f"Description: {details.get('properties', {}).get('description', 'No description')}")
            print("Properties:")
            print(json.dumps(details.get('properties', {}), indent=2))
        except Exception as e:
            print(f"Error getting details for {service_name}: {str(e)}")

    # Test 2: List all linked services
    print("\nTest 2: List all linked services")
    all_services = linked_services.list_linked_services()
    print("All linked services:")
    for service in all_services:
        print(f"- {service['name']} ({service['properties']['type']})")

    # Test 3: List only Snowflake linked services
    print("\nTest 3: List Snowflake linked services")
    snowflake_services = linked_services.list_linked_services(filter_by_type='Snowflake')
    print("Snowflake linked services:")
    for service in snowflake_services:
        print(f"- {service['name']}")

    # Test 4: Get details of Snowflake1
    print("\nTest 4: Get Snowflake1 details")
    snowflake1_details = linked_services.get_linked_service_details("Snowflake1")
    print("Snowflake1 details:")
    print(json.dumps(snowflake1_details, indent=2))

    # Test 5: Test connection of Snowflake1
    print("\nTest 5: Test Snowflake1 connection")
    connection_test = linked_services.test_linked_service_connection("Snowflake1")
    print("Connection test result:")
    print(json.dumps(connection_test, indent=2))

    # Test 6: Update Snowflake1 FQDN (dry run)
    print("\nTest 6: Update Snowflake1 FQDN (dry run)")
    old_fqdn = "test"  # Replace with actual old FQDN
    new_fqdn = "qqqq"  # Replace with actual new FQDN
    updated_service = linked_services.update_linked_service_sf_account(
        linked_service_name="Snowflake1",
        old_fqdn=old_fqdn,
        new_fqdn=new_fqdn,
        dry_run=True
    )

    # Test 7: Update Snowflake2 FQDN (dry run)
    print("\nTest 7: Update Snowflake2 FQDN (dry run)")
    updated_service = linked_services.update_linked_service_sf_account(
        linked_service_name="Snowflake2",
        old_fqdn=old_fqdn,
        new_fqdn=new_fqdn,
        dry_run=True
    )

def test_integration_runtime():
    # Initialize the ADFIntegrationRuntime class
    ir = ADFIntegrationRuntime(
        subscription_id="ee5f77a1-2e59-4335-8bdf-f7ea476f6523",
        resource_group_name="adf",
        factory_name="adf-stanley"
    )

    # Test 1: Get status of ManagedVnetIR
    print("\nTest 1: Get ManagedVnetIR status")
    ir_status = ir.get_ir_status("ManagedVnetIR")
    print(f"ManagedVnetIR status: {ir_status}")

    # Test 2: Get status of ManagedVnetIR2
    print("\nTest 2: Get ManagedVnetIR2 status")
    ir_status = ir.get_ir_status("ManagedVnetIR2")
    print(f"ManagedVnetIR2 status: {ir_status}")

    # Test 3: Enable interactive authoring for ManagedVnetIR
    print("\nTest 3: Enable interactive authoring for ManagedVnetIR")
    ir.enable_interactive_authoring("ManagedVnetIR", minutes=10)

    # Test 4: Get type of ManagedVnetIR
    print("\nTest 4: Get ManagedVnetIR type")
    ir_type = ir.get_ir_type("ManagedVnetIR2")
    print(f"ManagedVnetIR type: {ir_type}")

def main():
    print("Testing Linked Services Operations")
    print("=" * 50)
    test_linked_services()

    print("\nTesting Integration Runtime Operations")
    print("=" * 50)
    test_integration_runtime()

if __name__ == "__main__":
    main() 