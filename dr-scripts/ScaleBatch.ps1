# assume scale up and scale down batch in the same subscription
# this can be compaitable by swap the scale up and scale down in the build.json according to different mode 
# Define config path
$configPath = "..\"

# Read the JSON file
$jsonContent = Get-Content -Path "$configPath\build.json" | ConvertFrom-Json

# Process scale down operations
$batchScaleDown = $jsonContent.batchAccountScaleDown

if ($batchScaleDown -is [Array]) {
    foreach ($batchConfig in $batchScaleDown) {
        Write-Host "Scaling down batch account: $($batchConfig.batch) with pool: $($batchConfig.pool)"
        $context = Get-AzBatchAccount -AccountName $batchConfig.batch -ResourceGroupName $batchConfig.resourceGroup
        Start-AzBatchPoolResize -Id $batchConfig.pool -TargetDedicatedComputeNodes 0 -BatchContext $context
    }
} else {
    Write-Host "Scaling down batch account: $($batchScaleDown.batch) with pool: $($batchScaleDown.pool)"
    $context = Get-AzBatchAccount -AccountName $batchScaleDown.batch -ResourceGroupName $batchScaleDown.resourceGroup
    Start-AzBatchPoolResize -Id $batchScaleDown.pool -TargetDedicatedComputeNodes 0 -BatchContext $context
}

# Process scale up operations
$batchScaleUp = $jsonContent.batchAccountScaleUp

if ($batchScaleUp -is [Array]) {
    foreach ($batchConfig in $batchScaleUp) {
        Write-Host "Scaling up batch account: $($batchConfig.batch) with pool: $($batchConfig.pool)"
        $context = Get-AzBatchAccount -AccountName $batchConfig.batch -ResourceGroupName $batchConfig.resourceGroup
        Start-AzBatchPoolResize -Id $batchConfig.pool -TargetDedicatedComputeNodes 1 -BatchContext $context
    }
} else {
    Write-Host "Scaling up batch account: $($batchScaleUp.batch) with pool: $($batchScaleUp.pool)"
    $context = Get-AzBatchAccount -AccountName $batchScaleUp.batch -ResourceGroupName $batchScaleUp.resourceGroup
    Start-AzBatchPoolResize -Id $batchScaleUp.pool -TargetDedicatedComputeNodes 1 -BatchContext $context
}