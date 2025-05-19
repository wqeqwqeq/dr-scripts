import json
import argparse
from typing import Dict, List, Union, Optional

def create_maps() -> Dict[str, Dict[str, Dict[str, str]]]:
    """Create the mapping dictionaries for resources."""
    batch_map = {
        "Sales": {
            "qa": "qaBatchSales",
            "prod": "prodBatchSales",
            "DR": "DRBatchSales",
            "pool": {
                "qa": "qapoolBatchSales",
                "prod": "prodpoolBatchSales",
                "DR": "DRpoolBatchSales"
            }
        },
        "Finance": {
            "qa": "qaBatchFinance",
            "prod": "prodBatchFinance",
            "DR": "DRBatchFinance",
            "pool": {
                "qa": "qapoolBatchFinance",
                "prod": "prodpoolBatchFinance",
                "DR": "DRpoolBatchFinance"
            }
        },
        "Customer": {
            "qa": "qaBatchCustomer",
            "prod": "prodBatchCustomer",
            "DR": "DRBatchCustomer",
            "pool": {
                "qa": "qapoolBatchCustomer",
                "prod": "prodpoolBatchCustomer",
                "DR": "DRpoolBatchCustomer"
            }
        },
        "Accounting": {
            "qa": "qaBatchAccounting",
            "prod": "prodBatchAccounting",
            "DR": "DRBatchAccounting",
            "pool": {
                "qa": "qapoolBatchAccounting",
                "prod": "prodpoolBatchAccounting",
                "DR": "DRpoolBatchAccounting"
            }
        },
        "Retail": {
            "qa": "qaBatchRetail",
            "prod": "prodBatchRetail",
            "DR": "DRBatchRetail",
            "pool": {
                "qa": "qapoolBatchRetail",
                "prod": "prodpoolBatchRetail",
                "DR": "DRpoolBatchRetail"
            }
        },
        "Nonedw": {
            "qa": "qaBatchNonedw",
            "prod": "prodBatchNonedw",
            "DR": "DRBatchNonedw",
            "pool": {
                "qa": "qapoolBatchNonedw",
                "prod": "prodpoolBatchNonedw",
                "DR": "DRpoolBatchNonedw"
            }
        },
        "Associates": {
            "qa": "qaBatchAssociates",
            "prod": "prodBatchAssociates",
            "DR": "DRBatchAssociates",
            "pool": {
                "qa": "qapoolBatchAssociates",
                "prod": "prodpoolBatchAssociates",
                "DR": "DRpoolBatchAssociates"
            }
        }
    }

    storage_map = {
        "Sales": {"qa": "qaStorageSales", "prod": "prodStorageSales", "DR": "DRStorageSales"},
        "Finance": {"qa": "qaStorageFinance", "prod": "prodStorageFinance", "DR": "DRStorageFinance"},
        "Customer": {"qa": "qaStorageCustomer", "prod": "prodStorageCustomer", "DR": "DRStorageCustomer"},
        "Accounting": {"qa": "qaStorageAccounting", "prod": "prodStorageAccounting", "DR": "DRStorageAccounting"},
        "Retail": {"qa": "qaStorageRetail", "prod": "prodStorageRetail", "DR": "DRStorageRetail"},
        "Nonedw": {"qa": "qaStorageNonedw", "prod": "prodStorageNonedw", "DR": "DRStorageNonedw"},
        "Associates": {"qa": "qaStorageAssociates", "prod": "prodStorageAssociates", "DR": "DRStorageAssociates"}
    }

    kv_map = {
        "Sales": {"qa": "qaKvSales", "prod": "prodKvSales", "DR": "DRKvSales"},
        "Finance": {"qa": "qaKvFinance", "prod": "prodKvFinance", "DR": "DRKvFinance"},
        "Customer": {"qa": "qaKvCustomer", "prod": "prodKvCustomer", "DR": "DRKvCustomer"},
        "Accounting": {"qa": "qaKvAccounting", "prod": "prodKvAccounting", "DR": "DRKvAccounting"},
        "Retail": {"qa": "qaKvRetail", "prod": "prodKvRetail", "DR": "DRKvRetail"},
        "Nonedw": {"qa": "qaKvNonedw", "prod": "prodKvNonedw", "DR": "DRKvNonedw"},
        "Associates": {"qa": "qaKvAssociates", "prod": "prodKvAssociates", "DR": "DRKvAssociates"}
    }

    adf_map = {
        "Sales": {"qa": "qaSalesADF", "prod": "prodSalesADF", "DR": "DRSalesADF"},
        "Finance": {"qa": "qaFinanceADF", "prod": "prodFinanceADF", "DR": "DRFinanceADF"},
        "Customer": {"qa": "qaCustomerADF", "prod": "prodCustomerADF", "DR": "DRCustomerADF"},
        "Accounting": {"qa": "qaAccountingADF", "prod": "prodAccountingADF", "DR": "DRAccountingADF"},
        "Retail": {"qa": "qaRetailADF", "prod": "prodRetailADF", "DR": "DRRetailADF"},
        "Nonedw": {"qa": "qaNonedwADF", "prod": "prodNonedwADF", "DR": "DRNonedwADF"},
        "Associates": {"qa": "qaAssociatesADF", "prod": "prodAssociatesADF", "DR": "DRAssociatesADF"}
    }

    rg_map = {
        "Sales": {"qa": "qaSalesRG", "prod": "prodSalesRG", "DR": "DRSalesRG"},
        "Finance": {"qa": "qaFinanceRG", "prod": "prodFinanceRG", "DR": "DRFinanceRG"},
        "Customer": {"qa": "qaCustomerRG", "prod": "prodCustomerRG", "DR": "DRCustomerRG"},
        "Accounting": {"qa": "qaAccountingRG", "prod": "prodAccountingRG", "DR": "DRAccountingRG"},
        "Retail": {"qa": "qaRetailRG", "prod": "prodRetailRG", "DR": "DRRetailRG"},
        "Nonedw": {"qa": "qaNonedwRG", "prod": "prodNonedwRG", "DR": "DRNonedwRG"},
        "Associates": {"qa": "qaAssociatesRG", "prod": "prodAssociatesRG", "DR": "DRAssociatesRG"}
    }

    return {
        "batch_map": batch_map,
        "storage_map": storage_map,
        "kv_map": kv_map,
        "adf_map": adf_map,
        "rg_map": rg_map
    }

def generate_json(
    mode: str = 'failover',
    storage: bool = False,
    snowflake: bool = False,
    azure: bool = True,
    domain: str = 'Sales',
    environment: str = 'qa',
    customer_json: Optional[str] = None
) -> Dict:
    """Generate the JSON configuration based on input parameters."""
    if customer_json:
        return json.loads(customer_json)

    maps = create_maps()
    batch_map = maps["batch_map"]
    storage_map = maps["storage_map"]
    kv_map = maps["kv_map"]
    adf_map = maps["adf_map"]
    rg_map = maps["rg_map"]

    json_data = {}
    ordered_domains = ["Sales", "Finance", "Customer", "Accounting", "Retail", "Nonedw", "Associates"]

    if domain == "All":
        if storage:
            json_data["storageGRS"] = [
                {
                    "resourceGroup": rg_map[d][environment.lower()],
                    "storage": storage_map[d][environment.lower()]
                }
                for d in ordered_domains
            ]

        if snowflake:
            json_data["ADFLinkedServiceFQDN"] = [
                {
                    "resourceGroup": rg_map[d]["DR"] if azure else rg_map[d][environment.lower()],
                    "adf": adf_map[d]["DR"] if azure else adf_map[d][environment.lower()]
                }
                for d in ordered_domains
            ]

        if azure:
            # Create new batchAccountScale structure
            json_data["batchAccountScale"] = [
                {
                    "scaleUp": {
                        "resourceGroup": rg_map[d]["DR"],
                        "batch": batch_map[d]["DR"],
                        "pool": batch_map[d]["pool"]["DR"]
                    },
                    "scaleDown": {
                        "resourceGroup": rg_map[d][environment.lower()],
                        "batch": batch_map[d][environment.lower()],
                        "pool": batch_map[d]["pool"][environment.lower()]
                    }
                }
                for d in ordered_domains
            ]

            # Create new kvSync structure
            json_data["kvSync"] = [
                {
                    "from": {
                        "resourceGroup": rg_map[d][environment.lower()],
                        "kv": kv_map[d][environment.lower()]
                    },
                    "to": {
                        "resourceGroup": rg_map[d]["DR"],
                        "kv": kv_map[d]["DR"]
                    }
                }
                for d in ordered_domains
            ]

            # Create new ADFTrigger structure
            json_data["ADFTrigger"] = [
                {
                    "start": {
                        "resourceGroup": rg_map[d]["DR"],
                        "adf": adf_map[d]["DR"]
                    },
                    "stop": {
                        "resourceGroup": rg_map[d][environment.lower()],
                        "adf": adf_map[d][environment.lower()]
                    }
                }
                for d in ordered_domains
            ]
    else:
        if storage:
            json_data["storageGRS"] = {
                "resourceGroup": rg_map[domain][environment.lower()],
                "storage": storage_map[domain][environment.lower()]
            }

        if snowflake:
            json_data["ADFLinkedServiceFQDN"] = {
                "resourceGroup": rg_map[domain]["DR"] if azure else rg_map[domain][environment.lower()],
                "adf": adf_map[domain]["DR"] if azure else adf_map[domain][environment.lower()]
            }

        if azure:
            # Create new batchAccountScale structure for single domain
            json_data["batchAccountScale"] = {
                "scaleUp": {
                    "resourceGroup": rg_map[domain]["DR"],
                    "batch": batch_map[domain]["DR"],
                    "pool": batch_map[domain]["pool"]["DR"]
                },
                "scaleDown": {
                    "resourceGroup": rg_map[domain][environment.lower()],
                    "batch": batch_map[domain][environment.lower()],
                    "pool": batch_map[domain]["pool"][environment.lower()]
                }
            }

            # Create new kvSync structure for single domain
            json_data["kvSync"] = {
                "from": {
                    "resourceGroup": rg_map[domain][environment.lower()],
                    "kv": kv_map[domain][environment.lower()]
                },
                "to": {
                    "resourceGroup": rg_map[domain]["DR"],
                    "kv": kv_map[domain]["DR"]
                }
            }

            # Create new ADFTrigger structure for single domain
            json_data["ADFTrigger"] = {
                "start": {
                    "resourceGroup": rg_map[domain]["DR"],
                    "adf": adf_map[domain]["DR"]
                },
                "stop": {
                    "resourceGroup": rg_map[domain][environment.lower()],
                    "adf": adf_map[domain][environment.lower()]
                }
            }

    json_data["config"] = {
        "mode": mode,
        "storage": storage,
        "snowflake": snowflake,
        "azure": azure,
        "domain": domain,
        "environment": environment
    }

    return json_data

def main():
    parser = argparse.ArgumentParser(description='Generate configuration JSON for DR pipeline')
    parser.add_argument('--mode', choices=['failover', 'failback'], default='failover',
                      help='Operation mode (default: failover)')
    parser.add_argument('--storage', type=str, choices=['True', 'False'], default='False',
                      help='Include storage configuration (True/False)')
    parser.add_argument('--snowflake', type=str, choices=['True', 'False'], default='False',
                      help='Include Snowflake configuration (True/False)')
    parser.add_argument('--azure', type=str, choices=['True', 'False'], default='True',
                      help='Include Azure configuration (True/False)')
    parser.add_argument('--domain', choices=['All', 'Sales', 'Finance', 'Customer', 'Accounting', 'Retail', 'Nonedw', 'Associates'],
                      default='Sales', help='Target domain (default: Sales)')
    parser.add_argument('--environment', choices=['qa', 'prod'], default='qa',
                      help='Target environment (default: qa)')
    parser.add_argument('--customer-json', help='Customer JSON input')

    args = parser.parse_args()

    # Convert string boolean values to actual booleans
    storage = args.storage.lower() == 'true'
    snowflake = args.snowflake.lower() == 'true'
    azure = args.azure.lower() == 'true'

    json_data = generate_json(
        mode=args.mode,
        storage=storage,
        snowflake=snowflake,
        azure=azure,
        domain=args.domain,
        environment=args.environment,
        customer_json=args.customer_json
    )

    # Print configuration for debugging
    print("Mode:", args.mode)
    print("Storage Down:", storage)
    print("Snowflake Down:", snowflake)
    print("Azure Down:", azure)
    print("Domain:", args.domain)
    print("Environment:", args.environment)

    # Write JSON to file
    with open('build.json', 'w', encoding='utf-8') as f:
        json.dump(json_data, f, indent=2)

    # Print generated JSON for debugging
    print("\nGenerated JSON:")
    print(json.dumps(json_data, indent=2))

if __name__ == "__main__":
    main() 