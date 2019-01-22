# Azure SQL Data Sync Health Checker

This PowerShell script will check if all the metadata of a hub and member is in place and also validate the scopes against the information we have on the sync metadata database (among other validations). 
This script will not make any changes, will just validate data sync and user objects. 
It will also gather some useful information for faster troubleshooting.
It also has monitoring capabilities that will let us know which Data Sync stored procedures have run if monitoring is enabled (set to $true).

**In order to run it you need to:**

1 - Open Windows PowerShell ISE

2 - Open a New Script window

3 - Copy the script code from [https://raw.githubusercontent.com/vitomaz-msft/DataSyncHealthChecker/master/Data%20Sync%20Health%20Checker.ps1] and paste it in the script window you opened.

4 - Set the parameters on the top of the script, you need to set server names, database names, users and passwords:
```powershell
## Databases and credentials
# Sync metadata database credentials (Only SQL Authentication is supported)
$SyncDbServer = '.database.windows.net'
$SyncDbDatabase = ''
$SyncDbUser = ''
$SyncDbPassword = ''

# Hub credentials (Only SQL Authentication is supported)
$HubServer = '.database.windows.net'
$HubDatabase = ''
$HubUser = ''
$HubPassword = ''

# Member credentials (Azure SQL DB or SQL Server)
$MemberServer = ''
$MemberDatabase = ''
$MemberUser = ''
$MemberPassword = ''
# set MemberUseWindowsAuthentication to $true in case you wish to use integrated Windows authentication (MemberUser and MemberPassword will be ignored)
$MemberUseWindowsAuthentication = $false


## Health checks
$HealthChecksEnabled = $true  #Set as $true or $false

## Monitoring
$MonitoringMode = 'AUTO'  #Set as AUTO, ENABLED or DISABLED
$MonitoringIntervalInSeconds = 20
$MonitoringDurationInMinutes = 2

## Tracking Record Validations
# Set as "All" to validate all tables
# or pick the tables you need using '[dbo].[TableName1]','[dbo].[TableName2]'
$ExtendedValidationsTableFilter = @('All') 
$ExtendedValidationsEnabledForHub = $false  #Attention, this may cause high I/O impact
$ExtendedValidationsEnabledForMember = $false  #Attention, this may cause high I/O impact
$ExtendedValidationsCommandTimeout = 900 #seconds

## Other
$SendAnonymousUsageData = $true
$DumpMetadataSchemasForSyncGroup = '' #leave empty for automatic detection
$DumpMetadataObjectsForTable = '' #needs to be formatted like [SchemaName].[TableName]

```
5 – Run it.

6 – A folder with all the result files will be opened after the script completes, please send us all the files.