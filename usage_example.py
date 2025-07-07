"""
Usage example demonstrating shared authentication across multiple Azure resource classes.
"""

from AzHelper import (
    AzureAuthentication, 
    ADFLinkedServices, 
    AzureBatchPool, 
    AzureKeyVault,
    ADFPipeline,
    AzureResourceLock
)

def example_shared_authentication():
    """
    Example showing how to share authentication across multiple Azure resource instances.
    This approach is more efficient as it reuses credentials and tokens.
    """
    
    # Create a single authentication instance
    # This will be shared across all resource instances
    auth = AzureAuthentication(subscription_id="your-subscription-id")  # Optional: specify subscription
    
    # Now create multiple resource instances using the shared auth
    resource_group = "your-resource-group"
    
    # ADF related operations
    adf_linked_services = ADFLinkedServices(
        resource_group_name=resource_group,
        resource_name="your-adf-name",
        auth=auth  # Pass the shared auth instance
    )
    
    adf_pipeline = ADFPipeline(
        resource_group_name=resource_group,
        resource_name="your-adf-name", 
        auth=auth  # Same auth instance
    )
    
    # Batch operations  
    batch_pool = AzureBatchPool(
        resource_group_name=resource_group,
        resource_name="your-batch-account",
        pool_name="your-pool-name",
        auth=auth  # Same auth instance
    )
    
    # Key Vault operations
    key_vault = AzureKeyVault(
        resource_group_name=resource_group,
        resource_name="your-keyvault-name",
        auth=auth  # Same auth instance
    )
    
    # Resource Lock operations
    resource_locks = AzureResourceLock(
        resource_group_name=resource_group,
        auth=auth  # Same auth instance
    )
    
    # All these instances now share the same authentication and token
    # The token will be cached and reused, reducing authentication overhead
    
    # Example operations
    try:
        # List linked services
        linked_services = adf_linked_services.list_linked_services()
        print(f"Found {len(linked_services)} linked services")
        
        # Get pool configuration  
        pool_config = batch_pool.get_pool_config()
        print(f"Pool configuration retrieved: {pool_config.get('name')}")
        
        # List secrets
        secrets = key_vault.list_secrets()
        print(f"Found {len(secrets)} secrets")
        
        # Get locks
        locks = resource_locks.get_locks()
        print(f"Found {len(locks)} resource locks")
        
    except Exception as e:
        print(f"Error during operations: {str(e)}")


def example_without_shared_authentication():
    """
    Example showing the old way (still supported for backward compatibility).
    Each instance creates its own authentication - less efficient.
    """
    
    resource_group = "your-resource-group"
    
    # Each instance creates its own authentication
    adf_linked_services = ADFLinkedServices(
        resource_group_name=resource_group,
        resource_name="your-adf-name"
        # No auth parameter - creates its own
    )
    
    batch_pool = AzureBatchPool(
        resource_group_name=resource_group,
        resource_name="your-batch-account", 
        pool_name="your-pool-name"
        # No auth parameter - creates its own
    )
    
    # This approach still works but is less efficient


def example_multiple_subscriptions():
    """
    Example showing how to work with multiple subscriptions.
    """
    
    # Create auth instances for different subscriptions
    auth_subscription_1 = AzureAuthentication(subscription_id="subscription-1-id")
    auth_subscription_2 = AzureAuthentication(subscription_id="subscription-2-id")
    
    # Resources from subscription 1
    adf_sub1 = ADFLinkedServices(
        resource_group_name="rg-sub1",
        resource_name="adf-sub1",
        auth=auth_subscription_1
    )
    
    # Resources from subscription 2  
    adf_sub2 = ADFLinkedServices(
        resource_group_name="rg-sub2",
        resource_name="adf-sub2", 
        auth=auth_subscription_2
    )


if __name__ == "__main__":
    print("Example usage of shared Azure authentication")
    print("=" * 50)
    
    # Uncomment to run examples (make sure to update resource names)
    # example_shared_authentication()
    # example_without_shared_authentication() 
    # example_multiple_subscriptions()
    
    print("See function definitions above for usage patterns") 