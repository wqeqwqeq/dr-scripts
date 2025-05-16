# Script parameter
param(
    [Parameter(Mandatory=$false)]
    [bool]$DryRun = $true,

    [Parameter(Mandatory=$true)]
    [string]$oldFQDN,

    [Parameter(Mandatory=$tru)]
    [string]$newFQDN,

    [Parameter(Mandatory=$false)]
    [string]$configPath = ".."
    
)
function GetLinkedServiceByADF {
    param (
        [string]$ResourceGroupName,
        [string]$FactoryName,
        [string]$LinkedServiceName
    )

    try {
        # Get access token
        $Token = (Get-AzAccessToken).token
        $SubscriptionId = (Get-AzContext).Subscription.id

        # Construct the API URL
        $ApiUrl = "https://management.azure.com/subscriptions/$SubscriptionId/resourcegroups/$ResourceGroupName/providers/Microsoft.DataFactory/factories/$FactoryName/linkedservices/$LinkedServiceName"
        $ApiUrl = $ApiUrl + "?api-version=2018-06-01"

        # Set headers
        $Headers = @{
            "Authorization" = "Bearer $Token"
            "Content-Type"  = "application/json"
        }

        # Make the API call
        $Response = Invoke-RestMethod -Uri $ApiUrl -Headers $Headers -Method Get

        # Return the response
        return $Response
    } catch {
        Write-Error "Error getting linked service details: $_"
        throw
    }
}

# Function to get Snowflake linked services
function Get-SnowflakeLinkedServices {
    param (
        [string]$adfName,
        [string]$resourceGroupName
    )
    
    $linkedServices = Get-AzDataFactoryV2LinkedService -DataFactoryName $adfName -ResourceGroupName $resourceGroupName
    
    $snowflakeServices = @()
    foreach ($linkedService in $linkedServices) {
        $linkedServiceDetails = GetLinkedServiceByADF -ResourceGroupName $resourceGroupName -FactoryName $adfName -LinkedServiceName $linkedService.Name
        if ($linkedServiceDetails.properties.type -eq 'Snowflake' -or $linkedServiceDetails.properties.type -eq 'SnowflakeV2') {
            $snowflakeServices += $linkedServiceDetails
        }
    }
    
    return $snowflakeServices
}

# Function to change account name in connection string
function Update-SnowflakeAccountName {
    param (
        [object]$linkedService,
        [string]$oldFQDN,
        [string]$newFQDN
    )

        # For Snowflake V1
        if ($linkedService.Properties.type -eq "Snowflake") {
            $connectionString = $linkedService.Properties.typeProperties.ConnectionString
            $newConnectionString = $connectionString -replace "(?<=://)$([regex]::Escape($oldFQDN))(?=\.)", $newFQDN

            write-host "ConnectionString: $newConnectionString"
            $linkedService.Properties.typeProperties.ConnectionString = $newConnectionString
            
        } else {
        # For Snowflake V2
            $currentIdentifier = $linkedService.Properties.typeProperties.AccountIdentifier
            $newIdentifier = $currentIdentifier -replace "(?<=://)$([regex]::Escape($oldFQDN))(?=\.)", $newFQDN  
            $linkedService.Properties.typeProperties.AccountIdentifier = $newIdentifier      
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
            Set-AzDataFactoryV2LinkedService -ResourceGroupName $resourceGroupName -DataFactoryName $adfName -Name $linkedService.Name -DefinitionFile $tempFile -force
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
