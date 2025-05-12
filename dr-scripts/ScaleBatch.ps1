# assume scale up and scale down batch in the same subscription
# this can be compaitable by swap the scale up and scale down in the build.json according to different mode 
# Define config path
$configPath = "."

# Read the JSON file
$jsonContent = Get-Content -Path "build.json" | ConvertFrom-Json



# Process scale down operations
$accountScaleDown = $jsonContent.batchAccountScaleDown
$poolScaleDown = $jsonContent.batchPoolScaleDown

if ($accountScaleDown -is [Array]) {
    for ($i = 0; $i -lt $accountScaleDown.Count; $i++) {
        Write-Host "Scaling down batch pool: $($poolScaleDown[$i])"
        $context = Get-AzBatchAccount -AccountName $accountScaleDown[$i]
        Start-AzBatchPoolResize -Id $poolScaleDown[$i] -TargetDedicatedComputeNodes 0 -BatchContext $context
    }
} else {
    Write-Host "Scaling down batch pool: $poolScaleDown"
    $context = Get-AzBatchAccount -AccountName $accountScaleDown
    Start-AzBatchPoolResize -Id $poolScaleDown -TargetDedicatedComputeNodes 0 -BatchContext $context
}


# Process scale up operations
$accountScaleUp = $jsonContent.batchAccountScaleUp
$poolScaleUp = $jsonContent.batchPoolScaleUp

if ($accountScaleUp -is [Array]) {
    for ($i = 0; $i -lt $accountScaleUp.Count; $i++) {
        Write-Host "Scaling up batch pool: $($poolScaleUp[$i])"
        $context = Get-AzBatchAccount -AccountName $accountScaleUp[$i]
        Start-AzBatchPoolResize -Id $poolScaleUp[$i] -TargetDedicatedComputeNodes 1 -BatchContext $context
    }
} else {
    Write-Host "Scaling up batch pool: $poolScaleUp"
    $context = Get-AzBatchAccount -AccountName $accountScaleUp
    Start-AzBatchPoolResize -Id $poolScaleUp -TargetDedicatedComputeNodes 1 -BatchContext $context
}