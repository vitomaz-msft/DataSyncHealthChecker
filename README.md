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
$HealthChecksEnabled = $true  #Set as $true or $false
$MonitoringEnabled = $false  #Set as $true or $false
$MonitoringIntervalInSeconds = 30

$ExtendedValidationsEnabledForHub = $false  #Attention, this may cause high I/O impact
$ExtendedValidationsEnabledForMember = $false  #Attention, this may cause high I/O impact
$ExtendedValidationsTableFilter = @("All") # To validate all tables
#$ExtendedValidationsTableFilter = @("[dbo].[TableName1]","[dbo].[TableName2]") #to filter tables you need to validade, needs to be formated like [SchemaName].[TableName]
$ExtendedValidationsCommandTimeout = 900 #seconds

#Sync metadata database
$SyncDbServer = '.database.windows.net'
$SyncDbDatabase = ''
$SyncDbUser = ''
$SyncDbPassword = ''

#Hub
$HubServer = '.database.windows.net'
$HubDatabase = ''
$HubUser = ''
$HubPassword = ''

#Member (Azure SQL DB or SQL Server)
$MemberServer = '.database.windows.net'
$MemberDatabase = ''
$MemberUser = ''
$MemberPassword = ''
```
5 – Run it and send us the results.
