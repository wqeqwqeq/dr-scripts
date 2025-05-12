# Define config path
$configPath = "."

# Read the JSON file
$jsonContent = Get-Content -Path "build.json" | ConvertFrom-Json

# Check if ADFTriggerStart exists
if ($jsonContent.ADFTriggerStart) {
    $triggerValues = $jsonContent.ADFTriggerStart
    if ($triggerValues -is [Array]) {
        foreach ($trigger in $triggerValues) {
            Write-Host "Starting ADF trigger: $trigger"
            # Add your ADF trigger start logic here
        }
    } else {
        Write-Host "Starting ADF trigger: $triggerValues"
        # Add your ADF trigger start logic here
    }
} else {
    Write-Host "No ADFTriggerStart found in build.json"
}
