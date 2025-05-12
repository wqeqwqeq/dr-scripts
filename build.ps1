param(
    [Parameter(Mandatory=$false)]
    [ValidateSet('failover', 'failback')]
    [string]$Mode = 'failover',

    [Parameter(Mandatory=$false)]
    [bool]$Storage = $false,

    [Parameter(Mandatory=$false)]
    [bool]$Snowflake = $false,

    [Parameter(Mandatory=$false)]
    [bool]$Azure = $true,

    [Parameter(Mandatory=$false)]
    [ValidateSet('All', 'Sales', 'Finance', 'Customer', 'Accounting', 'Retail', 'Nonedw', 'Associates')]
    [string]$Domain = 'Sales',

    [Parameter(Mandatory=$false)]
    [ValidateSet('qa', 'prod')]
    [string]$Environment = 'qa'
)

# assume one batch only has one pool
$batchMap = @{
    "Sales" = @{
        "qa"   = "qaBatchSales"
        "prod" = "prodBatchSales"
        "DR"   = "DRBatchSales"
        "pool" = @{
            "qa"   = "qapoolBatchSales"
            "prod" = "prodpoolBatchSales"
            "DR"   = "DRpoolBatchSales"
        }
    }
    "Finance" = @{
        "qa"   = "qaBatchFinance"
        "prod" = "prodBatchFinance"
        "DR"   = "DRBatchFinance"
        "pool" = @{
            "qa"   = "qapoolBatchFinance"
            "prod" = "prodpoolBatchFinance"
            "DR"   = "DRpoolBatchFinance"
        }   
    }
    "Customer" = @{
        "qa"   = "qaBatchCustomer"
        "prod" = "prodBatchCustomer"
        "DR"   = "DRBatchCustomer"
        "pool" = @{
            "qa"   = "qapoolBatchCustomer"
            "prod" = "prodpoolBatchCustomer"
            "DR"   = "DRpoolBatchCustomer"
        }
    }     
    "Accounting" = @{
        "qa"   = "qaBatchAccounting"
        "prod" = "prodBatchAccounting"
        "DR"   = "DRBatchAccounting"
        "pool" = @{
            "qa"   = "qapoolBatchAccounting"
            "prod" = "prodpoolBatchAccounting"
            "DR"   = "DRpoolBatchAccounting"
        }
    }
    "Retail" = @{
        "qa"   = "qaBatchRetail"
        "prod" = "prodBatchRetail"
        "DR"   = "DRBatchRetail"
        "pool" = @{
            "qa"   = "qapoolBatchRetail"
            "prod" = "prodpoolBatchRetail"
            "DR"   = "DRpoolBatchRetail"
        }
    } 
    "Nonedw" = @{
        "qa"   = "qaBatchNonedw"
        "prod" = "prodBatchNonedw"
        "DR"   = "DRBatchNonedw"
        "pool" = @{
            "qa"   = "qapoolBatchNonedw"    
            "prod" = "prodpoolBatchNonedw"
            "DR"   = "DRpoolBatchNonedw"
        }
    }
    "Associates" = @{
        "qa"   = "qaBatchAssociates"
        "prod" = "prodBatchAssociates"
        "DR"   = "DRBatchAssociates"
        "pool" = @{
            "qa"   = "qapoolBatchAssociates"
            "prod" = "prodpoolBatchAssociates"
            "DR"   = "DRpoolBatchAssociates"
        }
    }
}
$storageMap = @{
    "Sales" = @{
        "qa"   = "qaStorageSales"
        "prod" = "prodStorageSales"
        "DR"   = "DRStorageSales"
    }
    "Finance" = @{
        "qa"   = "qaStorageFinance"
        "prod" = "prodStorageFinance"
        "DR"   = "DRStorageFinance"
    }
    "Customer" = @{
        "qa"   = "qaStorageCustomer"
        "prod" = "prodStorageCustomer"
        "DR"   = "DRStorageCustomer"
    }     
    "Accounting" = @{
        "qa"   = "qaStorageAccounting"
        "prod" = "prodStorageAccounting"
        "DR"   = "DRStorageAccounting"
    }
    "Retail" = @{
        "qa"   = "qaStorageRetail"
        "prod" = "prodStorageRetail"
        "DR"   = "DRStorageRetail"
    } 
    "Nonedw" = @{
        "qa"   = "qaStorageNonedw"
        "prod" = "prodStorageNonedw"
        "DR"   = "DRStorageNonedw"
        }
    "Associates" = @{
        "qa"   = "qaStorageAssociates"
        "prod" = "prodStorageAssociates"
        "DR"   = "DRStorageAssociates"
    }
}
$kvMap = @{
    "Sales" = @{
        "qa"   = "qaKvSales"
        "prod" = "prodKvSales"
        "DR"   = "DRKvSales"
    }
    "Finance" = @{
        "qa"   = "qaKvFinance"
        "prod" = "prodKvFinance"
        "DR"   = "DRKvFinance"
    }
    "Customer" = @{
        "qa"   = "qaKvCustomer"
        "prod" = "prodKvCustomer"
        "DR"   = "DRKvCustomer"
    }     
    "Accounting" = @{
        "qa"   = "qaKvAccounting"
        "prod" = "prodKvAccounting"
        "DR"   = "DRKvAccounting"
    }
    "Retail" = @{
        "qa"   = "qaKvRetail"
        "prod" = "prodKvRetail"
        "DR"   = "DRKvRetail"
    } 
    "Nonedw" = @{
        "qa"   = "qaKvNonedw"
        "prod" = "prodKvNonedw"
        "DR"   = "DRKvNonedw"
        }
    "Associates" = @{
        "qa"   = "qaKvAssociates"
        "prod" = "prodKvAssociates"
        "DR"   = "DRKvAssociates"
    }
}
$ADFMap = @{
    "Sales" = @{
        "qa"   = "qaSalesADF"
        "prod" = "prodSalesADF"
        "DR"   = "DRSalesADF"
    }
    "Finance" = @{
        "qa"   = "qaFinanceADF"
        "prod" = "prodFinanceADF"
        "DR"   = "DRFinanceADF"
    }
    "Customer" = @{
        "qa"   = "qaCustomerADF"
        "prod" = "prodCustomerADF"
        "DR"   = "DRCustomerADF"
    }     
    "Accounting" = @{
        "qa"   = "qaAccountingADF"
        "prod" = "prodAccountingADF"
        "DR"   = "DRAccountingADF"
    }
    "Retail" = @{
        "qa"   = "qaRetailADF"
        "prod" = "prodRetailADF"
        "DR"   = "DRRetailADF"
    } 
    "Nonedw" = @{
        "qa"   = "qaNonedwADF"
        "prod" = "prodNonedwADF"
        "DR"   = "DRNonedwADF"
        }
    "Associates" = @{
        "qa"   = "qaAssociatesADF"
        "prod" = "prodAssociatesADF"
        "DR"   = "DRAssociatesADF"
    }
}

$rgMap = @{
    "Sales" = @{
        "qa"   = "qaSalesRG"
        "prod" = "prodSalesRG"
        "DR"   = "DRSalesRG"
    }
    "Finance" = @{
        "qa"   = "qaFinanceRG"
        "prod" = "prodFinanceRG"
        "DR"   = "DRFinanceRG"
    }
    "Customer" = @{
        "qa"   = "qaCustomerRG"
        "prod" = "prodCustomerRG"
        "DR"   = "DRCustomerRG"
    }     
    "Accounting" = @{
        "qa"   = "qaAccountingRG"
        "prod" = "prodAccountingRG"
        "DR"   = "DRAccountingRG"
    }
    "Retail" = @{
        "qa"   = "qaRetailRG"
        "prod" = "prodRetailRG"
        "DR"   = "DRRetailRG"
    } 
    "Nonedw" = @{
        "qa"   = "qaNonedwRG"
        "prod" = "prodNonedwRG"
        "DR"   = "DRNonedwRG"
        }
    "Associates" = @{
        "qa"   = "qaAssociatesRG"
        "prod" = "prodAssociatesRG"
        "DR"   = "DRAssociatesRG"
    }
}

Write-Host "Mode: $Mode"
Write-Host "Storage Down: $Storage"
Write-Host "Snowflake Down: $Snowflake"
Write-Host "Azure Down: $Azure"
Write-Host "Domain: $Domain"
Write-Host "Environment: $Environment"

# Create a hashtable to store the JSON data
$jsonData = @{}

# Define ordered list of domains
$orderedDomains = @("Sales", "Finance", "Customer", "Accounting", "Retail", "Nonedw", "Associates")

# Process domains
if ($Domain -eq "All") {
    if ($Storage) {
        $jsonData["storageGRS"] = @()
        $orderedDomains | ForEach-Object {
            $jsonData["storageGRS"] += $storageMap[$_][$Environment.ToLower()]
        }
    }
    
    if ($Snowflake) {
        $jsonData["ADFLinkedServiceFQDN"] = @()
        $orderedDomains | ForEach-Object {
            if ($Azure) {
                # When both Snowflake and Azure are true, use 'DR' value
                $jsonData["ADFLinkedServiceFQDN"] += $ADFMap[$_]["DR"]
            } else {
                # When only Snowflake is true, use environment value
                $jsonData["ADFLinkedServiceFQDN"] += $ADFMap[$_][$Environment.ToLower()]
            }
        }
    }
    
    if ($Azure) {
        $jsonData["batchAccountScaleUp"] = @()
        $jsonData["batchAccountScaleDown"] = @()
        $jsonData["batchPoolScaleUp"] = @()
        $jsonData["batchPoolScaleDown"] = @()
        $jsonData["kvSyncFrom"] = @()
        $jsonData["kvSyncTo"] = @()
        $jsonData["ADFTriggerStop"] = @()
        $jsonData["ADFTriggerStart"] = @()
        
        $orderedDomains | ForEach-Object {
            $domain = $_
            $jsonData["batchAccountScaleUp"] += $batchMap[$domain]["DR"]
            $jsonData["batchAccountScaleDown"] += $batchMap[$domain][$Environment.ToLower()]
            $jsonData["batchPoolScaleUp"] += $batchMap[$domain]["pool"]["DR"]
            $jsonData["batchPoolScaleDown"] += $batchMap[$domain]["pool"][$Environment.ToLower()]
            $jsonData["kvSyncFrom"] += $kvMap[$domain][$Environment.ToLower()]
            $jsonData["kvSyncTo"] += $kvMap[$domain]["DR"]
            $jsonData["ADFTriggerStop"] += $ADFMap[$domain][$Environment.ToLower()]
            $jsonData["ADFTriggerStart"] += $ADFMap[$domain]["DR"]
        }
    }

    # Initialize resource group arrays for All domains
    $rgEastEnvs = @()
    $rgWestEnvs = @()
    $orderedDomains | ForEach-Object {
        $rgEastEnvs += $rgMap[$_][$Environment.ToLower()]
        $rgWestEnvs += $rgMap[$_]["DR"]
    }
} else {
    if ($Storage) {
        $jsonData["storageGRS"] = $storageMap[$Domain][$Environment.ToLower()]
    }
    
    if ($Snowflake) {
        if ($Azure) {
            # When both Snowflake and Azure are true, use 'DR' value
            $jsonData["ADFLinkedServiceFQDN"] = $ADFMap[$Domain]["DR"]
        } else {
            # When only Snowflake is true, use environment value
            $jsonData["ADFLinkedServiceFQDN"] = $ADFMap[$Domain][$Environment.ToLower()]
        }
    }
    
    if ($Azure) {
        $jsonData["batchAccountScaleUp"] = $batchMap[$Domain]["DR"]
        $jsonData["batchAccountScaleDown"] = $batchMap[$Domain][$Environment.ToLower()]
        $jsonData["batchPoolScaleUp"] = $batchMap[$Domain]["pool"]["DR"]
        $jsonData["batchPoolScaleDown"] = $batchMap[$Domain]["pool"][$Environment.ToLower()]
        $jsonData["kvSyncFrom"] = $kvMap[$Domain][$Environment.ToLower()]
        $jsonData["kvSyncTo"] = $kvMap[$Domain]["DR"]
        $jsonData["ADFTriggerStop"] = $ADFMap[$Domain][$Environment.ToLower()]
        $jsonData["ADFTriggerStart"] = $ADFMap[$Domain]["DR"]
    }

    # Set single values for specific domain
    $rgEastEnvs = $rgMap[$Domain][$Environment.ToLower()]
    $rgWestEnvs = $rgMap[$Domain]["DR"]
}

# Add config section with all parameters
$jsonData["config"] = @{
    "mode" = $Mode
    "storage" = $Storage
    "snowflake" = $Snowflake
    "azure" = $Azure
    "domain" = $Domain
    "environment" = $Environment
    "rgEast" = $rgEastEnvs
    "rgWest" = $rgWestEnvs
}

# Convert the hashtable to JSON
$jsonOutput = $jsonData | ConvertTo-Json -Depth 10

# Write the JSON to a file
$jsonOutput | Out-File -FilePath build.json -Encoding UTF8

# For debugging purposes
Write-Host "Generated JSON:"
Write-Host $jsonOutput
