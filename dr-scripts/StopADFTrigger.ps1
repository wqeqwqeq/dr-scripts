# Define config path
$configPath = "..\"

# Read the JSON file
$jsonContent = Get-Content -Path "$configPath\build.json" | ConvertFrom-Json

# Check if ADFTriggerStop exists
if ($jsonContent.ADFTriggerStop) {
    $triggerValues = $jsonContent.ADFTriggerStop
    if ($triggerValues -is [Array]) {
        foreach ($trigger in $triggerValues) {
            Write-Host "Stopping ADF trigger: $trigger"
            # Add your ADF trigger stop logic here
        }
    } else {
        Write-Host "Stopping ADF trigger: $triggerValues"
        # Add your ADF trigger stop logic here
    }
} else {
    Write-Host "No ADFTriggerStop found in build.json"
}
