<#
.SYNOPSIS
    Script that allows you to quickly restore a database by wrangling a bunch of files together.
.DESCRIPTION
    This function will take in a series of directories that contain files and attempt to "stich together" a series of database restore steps to execute. The function supports doing a full restore
    with the files provided or a "point in time" restore by utilizing the "stop at" functionality of the objects 
.PARAMETER ServerName
    The server name you want to restore the database to. Required.
.PARAMETER InstanceName
    The instance name, is using a named (non-default) instance. Optional, defaults to "default"
.PARAMETER DatabaseName
    The name of the daatabase you want to restore. Must exist on the server already. Required.
.PARAMETER FullBackupFileLocation
    A directory path that contains the full backups of the database you want to restore. Required.
.PARAMETER FullBackupFileLocation
    The file extension of the full backup files. Defaults to ".bak". Required.    
.PARAMETER DifferentialBackupFileLocation
    A directory path that contains the differential backups of the database you want to restore. Optional.
.PARAMETER DifferentialBackupFileExtension
    The file extension of the differential backup files. Defaults to ".dif". Optional. 
.PARAMETER logBackupFileLocation
    A directory path that contains the log backups of the database you want to restore. Optional.
.PARAMETER logBackupFileExenstion
    The file extension of the log backup files. Defaults to ".dif". Optional. 
.PARAMETER ToPointInTime
    A date and time to attempt to restore the database to. Timeframe should exist within the files you're attempting to restore. Optional.
.EXAMPLE
    Restore a local copy of AdventureWorks2014 by providing the location of full, differential, and log file backups and stopping at a point in time.
    .\Start-SQLRestore.ps1 -servername localhost -databaseName AdventureWorks2014 -fullBackupFileLocation C:\temp\backups\localhost\AdventureWorks2014\Full -differentialBackupFileLocation C:\temp\backups\localhost\AdventureWorks2014\Differential -logBackupFileLocation C:\temp\backups\localhost\AdventureWorks2014\Log -toPointInTime "09/30/2017 10:49:36 AM" -Verbose -ErrorAction Stop
.OUTPUTS
    None
.NOTES
    New features being addded; not all parameters fully defined yet.
#>
[cmdletbinding()]
param(
    [Parameter(Mandatory=$true)] [string] $servername,
    [Parameter(Mandatory=$false)] [string] $instanceName = "DEFAULT",
    [Parameter(Mandatory=$true)] [string] $databaseName,
    [Parameter(Mandatory=$true)] [string] $fullBackupFileLocation,
    [Parameter(Mandatory=$false)] [switch] $usefolderforFullBackup,
    [Parameter(Mandatory=$false)] [string] $fullBackupFileFileExtension = ".bak",
    [Parameter(Mandatory=$false)] [string] $differentialBackupFileLocation,
    [Parameter(Mandatory=$false)] [string] $differentialBackupFileExtension = ".dif",
    [Parameter(Mandatory=$false)] [string] $logBackupFileLocation,
    [Parameter(Mandatory=$false)] [string] $logBackupFileExenstion = ".trn",
    [Parameter(Mandatory=$false)] [string] $toPointInTime,
    [Parameter(Mandatory=$false)] [string] $PrerestoreProcedure,
	[Parameter(Mandatory=$false)] [string] $restoreLocationData,
	[Parameter(Mandatory=$false)] [string] $restoreLocationLog,
	[Parameter(Mandatory=$false)] [switch] $replace
)

function Get-FileList($fileslocation, $fileextension, $maxTime, $minTime, $isFullBackup)
{
   Write-Verbose "Fetching file list from $fileslocation..."
   if ($usefolderforFullBackup -and $isFullBackup)
   {
        $directoryList = @(Get-ChildItem $fileslocation | Where-Object {$_.LastWriteTime -le ($maxTime) -and $_.PSIsContainer} | Sort-Object $_.LastWriteTime -Descending)
        if ($directoryList.Count -gt 0) { $filesLocation = $filesLocation + "\" + $directoryList[0] }
   }
   $fileList = @(Get-ChildItem $fileslocation | Where-Object {$_.LastWriteTime -le ($maxTime) -and !$_.PSIsContainer -and $_.Extension -eq $fileextension} | Sort-Object $_.LastWriteTime -Descending)
   if ($minTime -ne $null -and $minTime -lt $maxTime) { $fileList = $fileList | Where-Object {$_.LastWriteTime -gt $minTime} }
   return $fileList
}

function Create-RestoreStep($rfiles, $restoretype)
{
	$timestamp = Get-Date -UFormat "%Y%m%d_%H%M%S"
    $restore = new-object("Microsoft.SqlServer.Management.Smo.Restore")
    if (!$usefolderforFullBackup -or $restoretype -eq "differential")
    {
        $fn = $rfiles[0].FullName
        $restoreFile = New-Object("Microsoft.SqlServer.Management.Smo.BackupDeviceItem")  $rfiles[0].FullName, "File"
        $restore.Devices.Add($restoreFile)
    } else {
        foreach ($rf in $rfiles)
        {
            $restoreFile = New-Object("Microsoft.SqlServer.Management.Smo.BackupDeviceItem")  $rf.FullName, "File"
            $restore.Devices.Add($restoreFile)
        }
    }
    
    if ($restoreType -ne "log")
    {
        try
        {
            $filelist = $restore.ReadFileList($srv)
        }
        catch
        {
            throw "Unable to read backup file; is this a valid backup file?"
        }
    }
    $restore.Database = $databaseName
    $restore.Action = "Database"
    if ($restoretype -eq "log") { $restore.Action = "Log" }
    $restore.NoRecovery = $true
	if ($replace) { $restore.ReplaceDatabase = $true }

    if ($restoretype -eq "full")
    {
		$files = @()
		if (!$restoreLocationData)
		{
			$restore.ReplaceDatabase = $true
			$filegroups = $srv.Databases[$databasename].Filegroups
			foreach ($g in $filegroups)
			{
				foreach ($f in $g.Files)
				{
					$file = New-Object System.Object
					$file | Add-Member -type NoteProperty -name logicalName -Value $f.Name
					$file | Add-Member -type NoteProperty -name fullPath -Value $f.FileName
					$files += $file
				}
			}
		}
		else
		{			
			foreach ($f in $filelist | Where-Object {$_.Type -ne "L"})
			{
				$file = New-Object System.Object
				$pathParts = $f.PhysicalName.Split("\")
				$newPath = ($restoreLocationData + "\" + $timestamp + ($pathParts[$pathParts.Length - 1]))
				$file | Add-Member -type NoteProperty -name logicalName -Value $f.LogicalName
				$file | Add-Member -type NoteProperty -name fullPath -Value $newPath
				$files += $file

			}
		}
        $relocater = @()
        foreach ($f in $filelist | Where-Object {$_.Type -ne "L"})
        {
            $relocate = new-object('Microsoft.SqlServer.Management.Smo.RelocateFile')    
            $relocate.LogicalFileName = $f.LogicalName
            $relocate.PhysicalFileName = ($files | Where-Object {$_.logicalName -eq $f.LogicalName}).fullpath
            $relocater += $relocate
            $logicalName = $relocate.LogicalFileName
            $physicalName = $relocate.PhysicalFileName
            Write-Verbose "Relocating $logicalname to $physicalname"
        }

		if (!$restoreLocationLog)
		{
			$logfiles = $srv.Databases[$databasename].LogFiles
			foreach ($l in $logfiles)
			{
				$relocate = new-object('Microsoft.SqlServer.Management.Smo.RelocateFile')    
				$relocate.LogicalFileName = $l.Name
				$relocate.PhysicalFileName = $l.FileName
				$relocater += $relocate
				$logicalName = $relocate.LogicalFileName
				$physicalName = $relocate.PhysicalFileName
				Write-Verbose "Relocating $logicalname to $physicalname"
			}
		}
		else
		{
			foreach ($f in $filelist | Where-Object {$_.Type -eq "L"})
			{
				$pathParts = $f.PhysicalName.Split("\")
				$relocate = new-object('Microsoft.SqlServer.Management.Smo.RelocateFile')    
				$relocate.LogicalFileName = $f.LogicalName
				$relocate.PhysicalFileName = ($restoreLocationData + "\" + $timestamp + ($pathParts[$pathParts.Length - 1]))
				$relocater += $relocate
				$logicalName = $relocate.LogicalFileName
				$physicalName = $relocate.PhysicalFileName
				Write-Verbose "Relocating $logicalname to $physicalname"
			}
		}
        foreach ($r in $relocater)
        {
            $restore.RelocateFiles.Add($r) | Out-Null
        }
    }
    return $restore
}

function Verify-LogChain($filelist, $targetLSN)
{
    $totalWarnings = 0
    Write-Verbose "Verifying log file chain for supplied log files..."
    $firstLSN = $null
    $LastLSN = $null
    $previousLSN = $null
    $attempts = 0
    $validFirstLogFile = $false
    while ($attempts -lt 5 -and $validFirstLogFile -eq $false)
    {
        $firstTransactionLog = $fileList[$fileList.Count -1]
        $firstFile = $firstTransactionLog.FullName
        Write-Verbose "Checking $firstFile for LSN consistency..."
        $verifyHeader = new-object("Microsoft.SqlServer.Management.Smo.Restore")
        $logFile = New-Object("Microsoft.SqlServer.Management.Smo.BackupDeviceItem") $fileList[$fileList.Count -1].FullName, "File"
        $verifyHeader.Devices.Add($logfile)    
        $header = $verifyHeader.ReadBackupHeader($srv)
        if ($header.FirstLSN -gt $targetLSN -or $header.LastLSN -lt $targetLSN)
        {
            Write-Warning "The first log file being applied does not contain a valid log sequence for $targetLSN. Getting the next oldest file..."
            $fileList += Get-ChildItem $logBackupFileLocation | Where-Object {$_.LastWriteTime -lt ($firstTransactionLog.LastWriteTime) -and !$_.PSIsContainer -and $_.Extension -eq $logBackupFileExtension} | Sort-Object $_.LastWriteTime -Descending | Select-Object -First 1
            $newFile = $fileList[$fileList.Count -1].FullName
            Write-Warning "Adding the next oldest found log file to the list of log files to restore..."
            $attempts++
        }
        else
        {
            $firstLSN = $header.FirstLSN
            $LastLSN = $header.LastLSN
            Write-Warning "The first log file being applied contains a valid sequence for $targetLSN, because the first LSN of the log file is $firstLSN and the last LSN of the file is $lastLSN"
            $validFirstLogFile = $true
        }
    }

    $fileList = $fileList | Sort-Object $_.LastWriteTime
    $firstLSN = $null
    $LastLSN = $null

    foreach ($f in $filelist)
    {
        $fullName = $f.fullName
        if ($LastLSN -ne $null)
        {
            $previousLSN = $LastLSN
        }
        $verifyHeader = new-object("Microsoft.SqlServer.Management.Smo.Restore")
        $logFile = New-Object("Microsoft.SqlServer.Management.Smo.BackupDeviceItem")  $fullName, "File"
        $verifyHeader.Devices.Add($logfile)    
        $header = $verifyHeader.ReadBackupHeader($srv)
        $firstLSN = $header.FirstLSN
        $LastLSN = $header.LastLSN
        if ($previousLSN -ne $null)
        {
            if ($firstLSN -ne $previousLSN)
            {
                Write-Warning "Warning: LSN not in sequence! Are you missing a file?"
                $totalwarnings++
            }
        }
        Write-Verbose "Log file: $fullName First LSN: $firstLSN, Last LSN: $lastLSN"
    }
    return $totalWarnings
}

if ($instanceName -ne "DEFAULT") {$serverName += "\" + $instanceName}
$srv = new-object ('Microsoft.SqlServer.Management.Smo.Server') $servername

$targetTime = Get-Date
if ($toPointinTime)
{
    $targetTime = $toPointInTime
}

$restorePlan = New-Object ('Microsoft.SqlServer.Management.Smo.RestorePlan') $serverName, $databaseName
$restoreFiles = Get-FileList $fullBackupFileLocation $fullBackupFileFileExtension $targetTime $null $true
if ($restoreFiles.Count -gt 0)
{
    $restorePlan.RestoreOperations.Add((Create-RestoreStep $restoreFiles "full"))
    $restoreLSN = $restorePlan.RestoreOperations.ReadBackupHeader($srv).lastlsn
}
if ($differentialBackupFileLocation) 
{
    $restoreFiles = Get-FileList $differentialBackupFileLocation $differentialBackupFileExtension $targetTime ((Get-Item $restorePlan.RestoreOperations[0].Devices[($restorePlan.RestoreOperations.Devices.Count - 1)].Name).LastWriteTime) $false
    if ($restoreFiles.Count -gt 0)
    {
        $restorePlan.RestoreOperations.Add((Create-RestoreStep $restoreFiles "differential"))
        $restoreLSN = $restorePlan.RestoreOperations[$restorePlan.RestoreOperations.Count - 1].ReadBackupHeader($srv).lastlsn
    }
}
if ($logBackupFileLocation) 
{
    $restoreFiles = Get-FileList $logBackupFileLocation $logBackupFileExenstion $targetTime ((Get-Item $restorePlan.RestoreOperations[$restorePlan.RestoreOperations.Count - 1].Devices[$restorePlan.RestoreOperations[$restorePlan.RestoreOperations.Count - 1].Devices.Count - 1].Name).LastWriteTime) $false
 
    $warnings = Verify-LogChain $restoreFiles $restoreLSN

    if ($warnings -gt 0)
    {
        $title = "Missing or corrupt log files?"
        $message = "The restore script encountered potentionally missing log files for the set provided and target restore point so it can't restore them all. What do you want to do?"
        $stop =  New-Object System.Management.Automation.Host.ChoiceDescription "&Stop", "Stop the restore; don't do anything"
        $partialrestoreRecovery = New-Object System.Management.Automation.Host.ChoiceDescription "Restore Without Log Files But Leave Database In Recovery Mode", "Continue the restore, but don't restore any log files. Leave the database in &RECOVERY mode"
        $partialRestoreNoRecovery = New-Object System.Management.Automation.Host.ChoiceDescription "&Restore Without Log Files", "Continue the restore without using any log files. Makes the database available to use (&NORECOVERY)"
        $options = [System.Management.Automation.Host.ChoiceDescription[]]($stop, $partialrestoreRecovery, $partialRestoreNoRecovery)
        $verifyResults = $host.ui.PromptForChoice($title, $message, $options, 0)
    }

    if ($verifyresults -eq 0)
    {
        Write-Verbose "Exiting..." 
        Exit
    }

    if ($verifyResults -eq $null)
    {
        $restoreFiles = $restoreFiles | Sort-Object $_.LastWriteTime
        foreach ($lf in $restoreFiles)
        {
            $restorePlan.RestoreOperations.Add((Create-RestoreStep $lf "log"))
        }
    }
    if ($toPointInTime)
    {
        Write-Verbose "Stopping log file restores at $toPointInTime in time"
        $RestorePlan.RestoreOperations[$RestorePlan.RestoreOperations.Count - 1].ToPointInTime = $toPointInTime
    }
}

$steps = $RestorePlan.RestoreOperations.Count
Write-Verbose "Total restore operations: $steps"
if ($steps -gt 0)
{
    if ($PrerestoreProcedure)
    {
        Write-Verbose "Executing the following stored procedure: $prerestoreProcedure"
        $srv.Databases["master"].ExecuteNonQuery("EXECUTE $prerestoreProcedure")    
    }
    Write-Verbose "Starting restore operation(s)..."
    $restorePlan.Execute()
    Write-Verbose "Restore steps completed!"
    if ($verifyResults -ne 1)
    {
        Write-Verbose "Bringing database online..."
        $srv.Databases["master"].ExecuteNonQuery("RESTORE DATABASE $DatabaseName WITH RECOVERY")
    }
} else {
    Write-Warning "No steps to restore! Did you specify a location with backups (or use the -useFolderForFullBackup switch)?"
}