Add-Type -AssemblyName 'OSIsoft.AFSDK, Version=4.0.0.0, Culture=neutral, PublicKeyToken=6238be57836698e6'

$piServers = New-Object OSIsoft.AF.PI.PIServers
$piServer = $piServers['piserver']
$piServer.Connect()
Write-Host "Connected to PI Server: $($piServer.Name)"

# Loop until all points are deleted
$totalDeleted = 0
do {
    $points = [OSIsoft.AF.PI.PIPoint]::FindPIPoints($piServer, '*')
    $count = @($points).Count
    Write-Host "Found $count PI Points remaining"
    if ($count -eq 0) { break }

    foreach ($pt in $points) {
        try {
            $piServer.DeletePIPoint($pt.Name)
            $totalDeleted++
        } catch {
            Write-Host "Failed: $($pt.Name) - $($_.Exception.Message)"
        }
    }
    Write-Host "Deleted so far: $totalDeleted"
} while ($count -gt 0)

Write-Host "Total PI Points deleted: $totalDeleted"

# Delete AF Database
$piSystems = New-Object OSIsoft.AF.PISystems
$piSystem = $piSystems['piserver']
$piSystem.Connect()
$db = $piSystem.Databases['Meters']
if ($db) {
    $piSystem.Databases.Remove($db)
    Write-Host "Deleted AF Database: Meters"
} else {
    Write-Host "AF Database Meters not found"
}

# Verify
$remaining = @([OSIsoft.AF.PI.PIPoint]::FindPIPoints($piServer, '*')).Count
Write-Host "Verification - Remaining PI Points: $remaining"
$dbCheck = $piSystem.Databases['Meters']
if ($dbCheck) { Write-Host "Verification - AF Database Meters: EXISTS" } else { Write-Host "Verification - AF Database Meters: DELETED" }

Write-Host "Cleanup complete!"
