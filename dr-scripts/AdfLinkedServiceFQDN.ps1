# Script parameter
param(
    [Parameter(Mandatory=$false)]
    [bool]$DryRun = $true
)

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
        [string]$resourceGroupName,
        [bool]$DryRun = $true
    )
    
    # Create a temporary file for the linked service definition
    $tempFile = [System.IO.Path]::GetTempFileName()
    $linkedService | ConvertTo-Json -Depth 10 | Out-File -FilePath $tempFile -Encoding UTF8
    
    try {
        if ($DryRun) {
            Set-AzDataFactoryV2LinkedService -ResourceGroupName $resourceGroupName -DataFactoryName $adfName -Name $linkedService.Name -DefinitionFile $tempFile -WhatIf
        } else {
            Set-AzDataFactoryV2LinkedService -ResourceGroupName $resourceGroupName -DataFactoryName $adfName -Name $linkedService.Name -DefinitionFile $tempFile
        }
    }
    finally {
        # Clean up the temporary file
        if (Test-Path $tempFile) {
            Remove-Item $tempFile -Force
        }
    }
}

# Define config path
$configPath = "..\"

# Read the JSON file
$jsonContent = Get-Content -Path "$configPath\build.json" | ConvertFrom-Json

# Check if ADFLinkedServiceFQDN exists
if ($jsonContent.ADFLinkedServiceFQDN) {
    # Handle both array and single value cases
    $adfValues = $jsonContent.ADFLinkedServiceFQDN
    if ($adfValues -is [Array]) {
        # Handle array case
        foreach ($adfConfig in $adfValues) {
            Write-Host "Processing ADF Linked Service: $($adfConfig.adf) with Resource Group: $($adfConfig.resourceGroup)"
            
            # Step 1: Get Snowflake linked services
            $snowflakeServices = Get-SnowflakeLinkedServices -adfName $adfConfig.adf -resourceGroupName $adfConfig.resourceGroup
            
            # Step 2: Update account name for each service
            foreach ($service in $snowflakeServices) {
                $updatedService = Update-SnowflakeAccountName -linkedService $service
                
                # Step 3: Redeploy the updated service
                Deploy-SnowflakeLinkedService -linkedService $updatedService -adfName $adfConfig.adf -resourceGroupName $adfConfig.resourceGroup -DryRun $DryRun
            }
        }
    } else {
        # Handle single value case
        Write-Host "Processing ADF Linked Service: $($adfValues.adf) with Resource Group: $($adfValues.resourceGroup)"
        
        # Step 1: Get Snowflake linked services
        $snowflakeServices = Get-SnowflakeLinkedServices -adfName $adfValues.adf -resourceGroupName $adfValues.resourceGroup
        
        # Step 2: Update account name for each service
        foreach ($service in $snowflakeServices) {
            $updatedService = Update-SnowflakeAccountName -linkedService $service
            
            # Step 3: Redeploy the updated service
            Deploy-SnowflakeLinkedService -linkedService $updatedService -adfName $adfValues.adf -resourceGroupName $adfValues.resourceGroup -DryRun $DryRun
        }
    }
} else {
    Write-Host "No ADFLinkedServiceFQDN found in build.json"
}
