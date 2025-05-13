# Define config path
$configPath = "..\"

# Read the JSON file
$jsonContent = Get-Content -Path "$configPath\build.json" | ConvertFrom-Json

# Check if storageGRS exists
if ($jsonContent.storageGRS) {
    $storageValues = $jsonContent.storageGRS
    if ($storageValues -is [Array]) {
        foreach ($storage in $storageValues) {
            Write-Host "Processing storage GRS: $storage"
            # Add your storage GRS processing logic here
        }
    } else {
        Write-Host "Processing storage GRS: $storageValues"
        # Add your storage GRS processing logic here
    }
} else {
    Write-Host "No storageGRS found in build.json"
}
