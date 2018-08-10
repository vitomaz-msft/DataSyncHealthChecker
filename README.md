# Azure SQL Data Sync Health Checker

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, 
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

This PowerShell script will check if all the metadata of a hub and member is in place and also validate the scopes against the information we have on the sync metadata database (among other validations). 
This script will not make any changes, will just validate data sync and user objects. 
It will also gather some useful information for faster troubleshooting.
It also has monitoring capabilities that will let us know which Data Sync stored procedures have run if monitoring is enabled (set to $true).

**In order to run it you need to:**

1 - Open Windows PowerShell ISE

2- Open a New Script window

3- Copy the script code from [https://raw.githubusercontent.com/vitomaz-msft/DataSyncHealthChecker/master/Data%20Sync%20Health%20Checker.ps1] and paste it in the PowerShell ISE window.

4 - Set the parameters on the bottom of the script, you need to set server names, database names, users and passwords:
<code></code><code>PowerShell
$MonitoringEnabled = $false #Set as $true or $false
$MonitoringIntervalInSeconds = 60
$ExtendedValidationsEnabledForHub = $false #Attention, this may cause high I/O impact if set to true
$ExtendedValidationsEnabledForMember = $false #Attention, this may cause high I/O impact if set to true
$ExtendedValidationsCommandTimeout = 600 #seconds

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
</code><code></code>
5 – Run it and send us the results.
