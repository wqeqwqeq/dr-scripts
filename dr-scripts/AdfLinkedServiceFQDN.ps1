# Function to get Snowflake linked services
function Get-SnowflakeLinkedServices {
    param (
        [string]$adfName,
        [string]$resourceGroupName
    )
    
    $linkedServices = Get-AzDataFactoryLinkedService -DataFactoryName $adfName -ResourceGroupName $resourceGroupName
    $snowflakeServices = $linkedServices | Where-Object { 
        $_.Properties.Type -eq "Snowflake" -or $_.Properties.Type -eq "SnowflakeV2"
    }
    
    return $snowflakeServices
}

# Function to change account name in connection string
function Update-SnowflakeAccountName {
    param (
        [object]$linkedService
    )
    
    if ($linkedService.Properties.Type -eq "Snowflake") {
        # For Snowflake V1
        $connectionString = $linkedService.Properties.TypeProperties.ConnectionString
        $newConnectionString = $connectionString -replace "company.privatelink", "company2.privatelink"
        $linkedService.Properties.TypeProperties.ConnectionString = $newConnectionString
    }
    else {
        # For Snowflake V2
        $accountIdentifier = $linkedService.Properties.TypeProperties.AccountIdentifier
        $newAccountIdentifier = $accountIdentifier -replace "company.privatelink", "company2.privatelink"
        $linkedService.Properties.TypeProperties.AccountIdentifier = $newAccountIdentifier
    }
    
    return $linkedService
}

# Function to redeploy linked service
function Deploy-SnowflakeLinkedService {
    param (
        [object]$linkedService,
        [string]$adfName,
        [string]$resourceGroupName
    )
    
    Set-AzDataFactoryV2LinkedService -DataFactoryName $adfName -ResourceGroupName $resourceGroupName -Name $linkedService.Name -DefinitionFile $linkedService
}

# Define config path
$configPath = "."

# Read the JSON file
$jsonContent = Get-Content -Path "build.json" | ConvertFrom-Json

# Get resource groups from config
$rgEast = $jsonContent.config.rgEast
$rgWest = $jsonContent.config.rgWest

# Check if ADFLinkedServiceFQDN exists
if ($jsonContent.ADFLinkedServiceFQDN) {
    # Handle both array and single value cases
    $adfValues = $jsonContent.ADFLinkedServiceFQDN
    if ($adfValues -is [Array]) {
        # Handle array case
        for ($i = 0; $i -lt $adfValues.Count; $i++) {
            $adf = $adfValues[$i]
            $resourceGroup = if ($jsonContent.config.snowflake -and $jsonContent.config.azure) {
                $rgWest[$i]
            } else {
                $rgEast[$i]
            }
            
            Write-Host "Processing ADF Linked Service: $adf with Resource Group: $resourceGroup"
            
            # Step 1: Get Snowflake linked services
            $snowflakeServices = Get-SnowflakeLinkedServices -adfName $adf -resourceGroupName $resourceGroup
            
            # Step 2: Update account name for each service
            foreach ($service in $snowflakeServices) {
                $updatedService = Update-SnowflakeAccountName -linkedService $service
                
                # Step 3: Redeploy the updated service
                Deploy-SnowflakeLinkedService -linkedService $updatedService -adfName $adf -resourceGroupName $resourceGroup
            }
        }
    } else {
        # Handle single value case
        $resourceGroup = if ($jsonContent.config.snowflake -and $jsonContent.config.azure) {
            $rgWest
        } else {
            $rgEast
        }
        
        Write-Host "Processing ADF Linked Service: $adfValues with Resource Group: $resourceGroup"
        
        # Step 1: Get Snowflake linked services
        $snowflakeServices = Get-SnowflakeLinkedServices -adfName $adfValues -resourceGroupName $resourceGroup
        
        # Step 2: Update account name for each service
        foreach ($service in $snowflakeServices) {
            $updatedService = Update-SnowflakeAccountName -linkedService $service
            
            # Step 3: Redeploy the updated service
            Deploy-SnowflakeLinkedService -linkedService $updatedService -adfName $adfValues -resourceGroupName $resourceGroup
        }
    }
} else {
    Write-Host "No ADFLinkedServiceFQDN found in build.json"
}
