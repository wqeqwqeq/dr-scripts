# %%
from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.batch import BatchManagementClient
from azure.keyvault.secrets import SecretClient
from azure.keyvault.keys import KeyClient
from azure.keyvault.certificates import CertificateClient
from azure.mgmt.keyvault import KeyVaultManagementClient
import requests
import time
import json
import re
import tempfile
import os
from datetime import datetime, timedelta
from typing import List, Dict, Union, Literal, Tuple
from subprocess import PIPE, run


class AzureResourceBase:


    def get_subscription_id(self):
        """Get the current subscription ID using Azure CLI"""
        cmd = "az account show --query id --output tsv"
        return self.run_cmd(cmd).stdout.strip()

    def __init__(
        self, 
        resource_group_name: str, 
        resource_name: str,
        resource_type: Literal['adf', 'batch', 'keyvault'],
        subscription_id: str = None
    ):
        """
        Base class for Azure resource operations.
        
        Args:
            resource_group_name: Name of the resource group
            resource_name: Name of the resource (ADF factory, Batch account, or Key Vault)
            resource_type: Type of resource ('adf', 'batch', or 'keyvault')
            subscription_id: Azure subscription ID. If not provided, will be retrieved from Azure CLI
        """
        self.resource_group_name = resource_group_name
        self.resource_name = resource_name
        self.resource_type = resource_type.lower()
        self.subscription_id = subscription_id or self.get_subscription_id()
        self.credential = DefaultAzureCredential()
        self.token = None
        self.token_expiry = None
        
        # Initialize appropriate client based on resource type
        if self.resource_type == 'adf':
            self.client = DataFactoryManagementClient(
                credential=self.credential, 
                subscription_id=self.subscription_id
            )
        elif self.resource_type == 'batch':
            self.client = BatchManagementClient(
                credential=self.credential, 
                subscription_id=self.subscription_id
            )
        elif self.resource_type == 'keyvault':
            self.kv_client = KeyVaultManagementClient(
                credential=self.credential,
                subscription_id=self.subscription_id
            )
            self.secret_client = SecretClient(
                vault_url=f"https://{resource_name}.vault.azure.net",
                credential=self.credential
            )
            self.key_client = KeyClient(
                vault_url=f"https://{resource_name}.vault.azure.net",
                credential=self.credential
            )
            self.certificate_client = CertificateClient(
                vault_url=f"https://{resource_name}.vault.azure.net",
                credential=self.credential
            )
        else:
            raise ValueError(f"Unsupported resource type: {resource_type}. Must be 'adf', 'batch', or 'keyvault'")

    def _get_token(self):
        """
        Get a new token if current one is expired or doesn't exist
        """
        now = datetime.now()
        if (
            self.token is None
            or self.token_expiry is None
            or (self.token_expiry is not None and now >= self.token_expiry)
        ):
            print("Generating new token...")
            token_response = self.credential.get_token(
                "https://management.azure.com/.default"
            )
            self.token = token_response.token
            # Convert expires_on (Unix timestamp) to datetime
            self.token_expiry = datetime.fromtimestamp(
                token_response.expires_on
            ) - timedelta(minutes=5)
        return self.token

    def get_resource_details(self):
        """
        Get details of the resource based on its type
        """
        try:
            if self.resource_type == 'adf':
                return self.client.factories.get(
                    resource_group_name=self.resource_group_name,
                    factory_name=self.resource_name
                )
            elif self.resource_type == 'batch':
                return self.client.batch_account.get(
                    resource_group_name=self.resource_group_name,
                    account_name=self.resource_name
                )
        except Exception as e:
            print(f"Error getting {self.resource_type} details: {str(e)}")
            raise
    
    @staticmethod
    def run_cmd(msg):
        """Run a shell command and return the result"""
        return run(
            args=msg, stdout=PIPE, stderr=PIPE, universal_newlines=True, shell=True
        )


class ADFLinkedServices(AzureResourceBase):

    def list_linked_services(self, filter_by_type: Union[str, List[str]] = None) -> List[Dict]:
        """
        List all linked services in the Azure Data Factory.
        """
        try:
            # Get all linked services
            linked_services = self.client.linked_services.list_by_factory(
                resource_group_name=self.resource_group_name,
                factory_name=self.resource_name
            )
            
            # Convert to list of dictionaries and filter if needed
            services_list = []
            for service in linked_services:
                service_dict = service.as_dict()
                
                # If filter_by_type is specified, check if service type matches
                if filter_by_type:
                    service_type = service_dict.get('properties', {}).get('type')
                    if isinstance(filter_by_type, str):
                        # Single type filter
                        if service_type == filter_by_type:
                            services_list.append(service_dict)
                    elif isinstance(filter_by_type, list):
                        # Multiple type filter
                        if service_type in filter_by_type:
                            services_list.append(service_dict)
                else:
                    services_list.append(service_dict)
            
            return services_list
            
        except Exception as e:
            print(f"Error listing linked services: {str(e)}")
            raise

    def get_linked_service_details(self, linked_service_name):
        """
        Get the details of a linked service using API calls.
        """
        try:
            # Construct the API URL
            api_url = f"https://management.azure.com/subscriptions/{self.subscription_id}/resourcegroups/{self.resource_group_name}/providers/Microsoft.DataFactory/factories/{self.resource_name}/linkedservices/{linked_service_name}?api-version=2018-06-01"

            # Make the API call
            headers = {
                "Authorization": f"Bearer {self._get_token()}",
                "Content-Type": "application/json",
            }

            response = requests.get(api_url, headers=headers)
            response.raise_for_status()

            return response.json()
        except Exception as e:
            print(f"Error getting linked service details: {str(e)}")
            raise

    def get_linked_service_sdk(self, linked_service_name):
        """
        Get the details of a linked service using Azure SDK.
        """
        try:
            response = self.client.linked_services.get(
                resource_group_name=self.resource_group_name,
                factory_name=self.resource_name,
                linked_service_name=linked_service_name
            )
            return response.as_dict()
        except Exception as e:
            print(f"Error getting linked service details using SDK: {str(e)}")
            raise

    def update_linked_service_sf_account(
        self,
        linked_service_name: str,
        old_fqdn: str,
        new_fqdn: str,
        dry_run: bool = True
    ) -> Dict:
        """
        Update the Snowflake account FQDN in a linked service.
        """
        try:
            # Get the current linked service details
            linked_service = self.get_linked_service_details(linked_service_name)
            
            # Check if it's a Snowflake service
            service_type = linked_service.get('properties', {}).get('type')
            print(f"Updating {service_type} Linked Service {linked_service_name} from {old_fqdn} to {new_fqdn}")
            
            # Update the connection string based on Snowflake version
            if service_type == 'Snowflake':
                # For Snowflake V1
                connection_string = linked_service['properties']['typeProperties']['connectionString']
                new_connection_string = re.sub(
                    f"(?<=://){re.escape(old_fqdn)}(?=\.)",
                    new_fqdn,
                    connection_string
                )
                # Check if the regex found a match, no replacement happened
                if new_connection_string == connection_string:
                    print(f"Warning: Could not find exact match for '{old_fqdn}' in connection string")
                    return
                print(f"New ConnectionString: {new_connection_string}")
                linked_service['properties']['typeProperties']['connectionString'] = new_connection_string
            
            else:
                # For Snowflake V2
                current_identifier = linked_service['properties']['typeProperties']['accountIdentifier']
                new_identifier = re.sub(
                    f"(?<=://){re.escape(old_fqdn)}(?=\.)",
                    new_fqdn,
                    current_identifier
                )
                # Check if the regex found a match, no replacement happened
                if new_identifier == current_identifier:
                    print(f"Warning: Could not find exact match for '{old_fqdn}' in account identifier")
                    return
                linked_service['properties']['typeProperties']['accountIdentifier'] = new_identifier
            
            if dry_run:
                print(f"What if: Would update linked service {linked_service_name}")
                print("New configuration:")
                print(json.dumps(linked_service, indent=2))
                return
            
            # Update the linked service using Azure SDK
            response = self.client.linked_services.create_or_update(
                resource_group_name=self.resource_group_name,
                factory_name=self.resource_name,
                linked_service_name=linked_service_name,
                linked_service=linked_service
            )
            
            print(f"Successfully updated linked service: {linked_service_name}")
            return response.as_dict()
            
        except Exception as e:
            print(f"Error updating linked service: {str(e)}")
            raise

    def test_linked_service_connection(self, linked_service_name, parameters=None):
        """
        Test the connection of a linked service
        """
        try:
            # First get the linked service details
            linked_service = self.get_linked_service_details(linked_service_name)

            # If parameters are provided, update the linked service properties
            if parameters:
                linked_service["properties"]["parameters"] = parameters

            # Construct the test connectivity request body
            body = {"linkedService": linked_service}

            # Construct the API URL
            api_url = f"https://management.azure.com/subscriptions/{self.subscription_id}/resourcegroups/{self.resource_group_name}/providers/Microsoft.DataFactory/factories/{self.resource_name}/testConnectivity?api-version=2018-06-01"

            # Make the API call
            headers = {
                "Authorization": f"Bearer {self._get_token()}",
                "Content-Type": "application/json",
            }

            print("Testing linked service connection with the following configuration:")
            print(json.dumps(body, indent=2))

            response = requests.post(api_url, headers=headers, json=body)
            response.raise_for_status()

            result = response.json()
            if result.get("succeeded"):
                print("Linked service connection test successful")
            else:
                print(
                    f"Linked service connection test failed: {result.get('errors', [{}])[0].get('message', 'Unknown error')}"
                )

            return result
        except Exception as e:
            print(f"Error testing linked service connection: {str(e)}")
            raise


class ADFManagedPrivateEndpoint(AzureResourceBase):

    def get_managed_private_endpoint(
        self,
        managed_private_endpoint_name: str,
        managed_vnet_name: str = "default"
    ) -> Dict:
        """
        Get details of a managed private endpoint in Azure Data Factory.
        """
        try:
            response = self.client.managed_private_endpoints.get(
                resource_group_name=self.resource_group_name,
                factory_name=self.resource_name,
                managed_virtual_network_name=managed_vnet_name,
                managed_private_endpoint_name=managed_private_endpoint_name
            )
            return response.as_dict()
        except Exception as e:
            print(f"Error getting managed private endpoint details: {str(e)}")
            raise

    def update_managed_private_endpoint_fqdn(
        self,
        managed_private_endpoint_name,
        fqdns,
        managed_vnet_name = "default"
    ):
        """
        Update the FQDN in a managed private endpoint while preserving other properties.
        """
        try:
            existing_endpoint = self.get_managed_private_endpoint(
                managed_private_endpoint_name=managed_private_endpoint_name,
                managed_vnet_name=managed_vnet_name
            )
            
            response = self.client.managed_private_endpoints.create_or_update(
                resource_group_name=self.resource_group_name,
                factory_name=self.resource_name,
                managed_virtual_network_name=managed_vnet_name,
                managed_private_endpoint_name=managed_private_endpoint_name,
                managed_private_endpoint={
                    "properties": {
                        "fqdns": fqdns,
                        "groupId": existing_endpoint['properties']['groupId'],
                        "privateLinkResourceId": existing_endpoint['properties']['privateLinkResourceId']
                    }
                },
            )
            print(f"Successfully updated managed private endpoint: {managed_private_endpoint_name}")
            return response
        except Exception as e:
            print(f"Error updating managed private endpoint: {str(e)}")
            raise


class ADFIntegrationRuntime(AzureResourceBase):
    def get_ir(self, ir_name):
        """
        Get the details of an integration runtime
        Returns the JSON response from the API
        """
        try:
            # Construct the API URL
            ir_resource_id = f"subscriptions/{self.subscription_id}/resourcegroups/{self.resource_group_name}/providers/Microsoft.DataFactory/factories/{self.resource_name}/integrationruntimes/{ir_name}"
            api_url = f"https://management.azure.com/{ir_resource_id}/getStatus?api-version=2018-06-01"

            # Make the API call
            headers = {
                "Authorization": f"Bearer {self._get_token()}",
                "Content-Type": "application/json",
            }

            response = requests.post(api_url, headers=headers)
            response.raise_for_status()

            return response.json()
        except Exception as e:
            print(f"Error getting integration runtime details: {str(e)}")
            raise

    def get_ir_status(self, ir_name):
        """
        Get the status of an integration runtime
        Returns True if interactive authoring is enabled, False otherwise
        """
        try:
            status_data = self.get_ir(ir_name)
            interactive_status = (
                status_data.get("properties", {})
                .get("typeProperties", {})
                .get("interactiveQuery", {})
                .get("status")
            )

            is_enabled = interactive_status == "Enabled"
            return is_enabled
        except Exception as e:
            print(f"Error getting integration runtime status: {str(e)}")
            raise

    def get_ir_type(self, ir_name):
        """
        Get the type of an integration runtime
        Returns the type as a string (e.g., "Managed", "SelfHosted", etc.)
        """
        try:
            # Fetch the integration runtime details
            ir_details = self.get_ir(ir_name)

            # Extract the type from the JSON response
            ir_type = ir_details.get("properties", {}).get("type", None)

            if ir_type is None:
                raise ValueError(f"Integration runtime type not found for {ir_name}")

            return ir_type
        except Exception as e:
            print(f"Error getting integration runtime type: {str(e)}")
            raise

    def enable_interactive_authoring(self, ir_name, minutes=10):
        """
        Enable interactive authoring for the specified integration runtime.
        Only works for Managed integration runtimes.
        """
        # First check if it's a Managed integration runtime
        ir_type = self.get_ir_type(ir_name)
        if ir_type != "Managed":
            print(f"Interactive authoring is only supported for Managed integration runtimes. Current type: {ir_type}")
            return

        # Check if interactive authoring is already enabled
        if self.get_ir_status(ir_name):
            print(
                f"Interactive authoring is already enabled for integration runtime {ir_name}"
            )
            return

        # Construct the API URL
        ir_resource_id = f"subscriptions/{self.subscription_id}/resourcegroups/{self.resource_group_name}/providers/Microsoft.DataFactory/factories/{self.resource_name}/integrationruntimes/{ir_name}"
        api_url = f"https://management.azure.com/{ir_resource_id}/enableInteractiveQuery?api-version=2018-06-01"

        # Make the API call
        headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
        }
        body = {"autoTerminationMinutes": minutes}

        response = requests.post(api_url, headers=headers, json=body)
        response.raise_for_status()

        print(f"Successfully triggered interactive authoring for {minutes} minutes")
        while not self.get_ir_status(ir_name):
            print("Waiting for interactive authoring to be enabled...")
            time.sleep(10)
        print("Interactive authoring is now enabled")


class AzureBatchPool(AzureResourceBase):
    def __init__(
        self,
        resource_group_name: str,
        resource_name: str,
        pool_name: str,
        subscription_id: str = None
    ):
        """
        Initialize Azure Batch Pool operations.
        
        Args:
            resource_group_name: Name of the resource group
            resource_name: Name of the batch account
            pool_name: Name of the pool
            subscription_id: Optional subscription ID
        """
        super().__init__(
            resource_group_name=resource_group_name,
            resource_name=resource_name,
            resource_type='batch',
            subscription_id=subscription_id
        )
        self.pool_name = pool_name

    def get_pool_config(self) -> Dict:
        """
        Get the current configuration of the batch pool.
        
        Returns:
            Dict containing the pool configuration
        """
        try:
            response = self.client.pool.get(
                resource_group_name=self.resource_group_name,
                account_name=self.resource_name,
                pool_name=self.pool_name
            )
            return response.as_dict()
        except Exception as e:
            print(f"Error getting pool configuration: {str(e)}")
            raise

    def scale_pool_nodes(self, target_nodes: int, dry_run: bool = True) -> Dict:
        """
        Scale the number of nodes in the batch pool.
        
        Args:
            target_nodes: Target number of nodes (0 or positive integer)
            dry_run: If True, only show what would be changed without making changes
            
        Returns:
            Dict containing the updated pool configuration
        """
        try:
            if target_nodes < 0:
                raise ValueError("Target nodes must be 0 or a positive integer")

            # Get current pool configuration
            pool_config = self.get_pool_config()
            current_nodes = pool_config.get('scaleSettings', {}).get('fixedScale', {}).get('targetDedicatedNodes', 0)
            
            print(f"Current node count: {current_nodes}")
            print(f"Target node count: {target_nodes}")
            
            if current_nodes == target_nodes:
                print(f"Pool {self.pool_name} already has {target_nodes} nodes. No changes needed.")
                return pool_config

            # Update the scale settings
            pool_config['scaleSettings'] = {
                'fixedScale': {
                    'targetDedicatedNodes': target_nodes
                }
            }

            if dry_run:
                print(f"What if: Would scale pool {self.pool_name} to {target_nodes} nodes")
                print("New configuration:")
                print(json.dumps(pool_config, indent=2))
                return pool_config

            # Update the pool
            response = self.client.pool.update(
                resource_group_name=self.resource_group_name,
                account_name=self.resource_name,
                pool_name=self.pool_name,
                parameters=pool_config
            )
            
            print(f"Successfully scaled pool {self.pool_name} to {target_nodes} nodes")
            return response.as_dict()
            
        except Exception as e:
            print(f"Error scaling pool nodes: {str(e)}")
            raise


class AzureKeyVault(AzureResourceBase):
    def get_secret(self, secret_name: str) -> str:
        """
        Get a secret from the key vault.
        
        Args:
            secret_name: Name of the secret to retrieve
            
        Returns:
            The secret value as a string
        """
        try:
            secret = self.secret_client.get_secret(secret_name)
            return secret.value
        except Exception as e:
            print(f"Error getting secret {secret_name}: {str(e)}")
            raise


    def list_secrets(self) -> List[Dict]:
        """
        List all secrets in the current key vault.
        
        Returns:
            List of dictionaries containing secret properties (name, created_on, updated_on, enabled)
        """
        try:
            secrets = []
            for secret in self.secret_client.list_properties_of_secrets():
                secrets.append({
                    'name': secret.name,
                    'created_on': secret.created_on,
                    'updated_on': secret.updated_on,
                    'enabled': secret.enabled
                })
            return secrets
        except Exception as e:
            print(f"Error listing secrets: {str(e)}")
            raise

    def set_secret(self, secret_name: str, secret_value: str) -> None:
        """
        Set a secret in the key vault.
        
        Args:
            secret_name: Name of the secret to set
            secret_value: Value of the secret to set
        """
        try:
            self.secret_client.set_secret(secret_name, secret_value)
            print(f"Successfully set secret {secret_name} in {self.resource_name} under {self.resource_group_name}")
        except Exception as e:
            print(f"Error setting secret {secret_name}: {str(e)}")
            raise

 # %%