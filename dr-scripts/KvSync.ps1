# Define config path
$configPath = "..\"

# Read the JSON file
$jsonContent = Get-Content -Path "$configPath\build.json" | ConvertFrom-Json

# Check if both kvSyncFrom and kvSyncTo exist
if ($jsonContent.kvSyncFrom -and $jsonContent.kvSyncTo) {
    # Handle both array and single value cases
    $fromValues = $jsonContent.kvSyncFrom
    $toValues = $jsonContent.kvSyncTo

    if ($fromValues -is [Array]) {
        # Zip the arrays together
        for ($i = 0; $i -lt $fromValues.Count; $i++) {
            $from = $fromValues[$i]
            $to = $toValues[$i]
            Write-Host "Syncing KV from $from to $to"
            # Add your KV sync logic here
        }
    } else {
        Write-Host "Syncing KV from $fromValues to $toValues"
        # Add your KV sync logic here
    }
} else {
    Write-Host "Missing kvSyncFrom or kvSyncTo in build.json"
}
