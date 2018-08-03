#Azure SQL Data Sync Health Checker

#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, 
#WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

$HealthChecksEnabled = $true  #Set as $true or $false
$MonitoringEnabled = $false  #Set as $true or $false
$MonitoringIntervalInSeconds = 60

$ExtendedValidationsEnabledForHub = $false  #Attention, this may cause high I/O impact
$ExtendedValidationsEnabledForMember = $false  #Attention, this may cause high I/O impact
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

#Member
$MemberServer = '.database.windows.net' 
$MemberDatabase = '' 
$MemberUser = ''
$MemberPassword = ''





function ValidateTables([Array] $userTables){    
    foreach ($userTable in $userTables) 
    {
        $TablePKList = New-Object System.Collections.ArrayList       
        
        $query = "SELECT 
                     c.name 'ColumnName',
                     t.Name 'Datatype',
                     c.max_length 'MaxLength',
                     c.is_nullable 'IsNullable'
                     FROM sys.columns c
                     INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
                     WHERE c.object_id = OBJECT_ID('" + $userTable + "')" 
        $MemberCommand.CommandText = $query
        $result = $MemberCommand.ExecuteReader()
        $datatable = new-object “System.Data.DataTable”
        $datatable.Load($result)

        $syncGroupSchemaColumns = $global:sgSchemaXml | Where-Object {$_.QuotedTableName -eq $userTable} | Select -ExpandProperty ColumnsToSync
        
        foreach($syncGroupSchemaColumn in $syncGroupSchemaColumns.DssColumnDescription)
        {
                $scopeCol = $datatable | Where-Object ColumnName -eq $syncGroupSchemaColumn.Name
                if(!$scopeCol)
                {
                    $msg= "WARNING: "+ $userTable+ ".["+$syncGroupSchemaColumn.Name+"] is missing in this database but exist in sync group schema, maybe preventing re-provisioning!"
                    #$msg2= "ALTER TABLE " + $userTable + " ADD " +$syncGroupSchemaColumn.Name +" " + $syncGroupSchemaColumn.DataType
                    Write-Host $msg -foreground Red
                    #Write-Host $msg2 -foreground Yellow
                    [void]$errorSummary.AppendLine($msg)
                    #[void]$errorSummary.AppendLine($msg2)
                }                
           }

        foreach ($userColumn in $datatable)
        {
                $sbCol = New-Object -TypeName "System.Text.StringBuilder"
                $schemaObj = $global:scope_config_data.SqlSyncProviderScopeConfiguration.Adapter | Where-Object GlobalName -eq $userTable
                $schemaColumn = $schemaObj.Col | Where-Object Name -eq $userColumn.ColumnName
                if(!$schemaColumn)
                {
                    if($userColumn.IsNullable -eq $false)
                    {
                        $msg= "WARNING: "+ $userTable + ".["+$userColumn.ColumnName+"] is not included in the sync group but is NOT NULLABLE!"
                        Write-Host $msg -foreground "Red"
                        [void]$errorSummary.AppendLine($msg)
                    }
                    continue
                }

                [void]$sbCol.Append($userTable + ".[" + $userColumn.ColumnName + "] " + $schemaColumn.param)

                if($schemaColumn.pk)
                {
                    [void]$sbCol.Append(" PrimaryKey ")
                    [void]$TablePKList.Add($schemaColumn.name)
                }

                if($schemaColumn.type -ne $userColumn.Datatype)
                { 
                    [void]$sbCol.Append('  Type(' + $schemaColumn.type +'):NOK ')
                    $msg="WARNING: " + $userTable + ".["+$userColumn.ColumnName+"] has a different datatype as the one defined in the sync scope! ("+$schemaColumn.type+" VS "+$userColumn.Datatype+")"
                    Write-Host $msg -foreground "Red"
                    [void]$errorSummary.AppendLine($msg)                    
                } 
                else
                { 
                    #Write-Host $userTable ".["$userColumn.ColumnName"] has the same datatype as the one defined in the sync scope! ("$schemaColumn.type")" -foreground "Green" 
                    [void]$sbCol.Append('  Type(' + $schemaColumn.type +'):OK ')
                }
                
                $colMaxLen=$userColumn.MaxLength

                if($schemaColumn.type -eq 'nvarchar' -or $schemaColumn.type -eq 'nchar'){$colMaxLen=$colMaxLen/2}
                
                if($userColumn.MaxLength -eq -1 -and ($schemaColumn.type -eq 'nvarchar' -or $schemaColumn.type -eq 'nchar' -or $schemaColumn.type -eq 'varbinary' -or $schemaColumn.type -eq 'varchar' -or $schemaColumn.type -eq 'nvarchar')){$colMaxLen='max'}

                if($schemaColumn.size -ne $colMaxLen)
                {
                    [void]$sbCol.Append('  Size(' + $schemaColumn.size +'):NOK ') 
                    $msg= "WARNING: "+ $userTable+ ".["+$userColumn.ColumnName+"] has a different data size as the one defined in the sync scope!("+$schemaColumn.size+" VS "+$colMaxLen+")"
                    Write-Host $msg -foreground "Red"
                    [void]$errorSummary.AppendLine($msg)
                }
                else
                { 
                    #Write-Host $userTable ".["$userColumn.ColumnName"] has the same data size as the one defined in the sync scope! ("$schemaColumn.size")" -foreground "Green" 
                    [void]$sbCol.Append('  Size(' + $schemaColumn.size +'):OK ')
                }
                                
                if($schemaColumn.null)
                {
                    if($schemaColumn.null -ne $userColumn.IsNullable)
                    { 
                        [void]$sbCol.Append('  Nullable(' + $schemaColumn.null +'):NOK ')
                        $msg= "WARNING: " +$userTable +".["+$userColumn.ColumnName+"] has a different IsNullable as the one defined in the sync scope! ("+$schemaColumn.null+" VS "+$userColumn.IsNullable+")"
                        Write-Host $msg -foreground "Red"
                        [void]$errorSummary.AppendLine($msg) 
                    } 
                    else
                    { 
                        [void]$sbCol.Append('  Nullable(' + $schemaColumn.null +'):OK ')
                    }                    
                }
                
                $sbColString = $sbCol.ToString()
                if($sbColString -match 'NOK'){ Write-Host $sbColString -ForegroundColor Red } else { Write-Host $sbColString -ForegroundColor Green }
                
           }
        
        if($ExtendedValidationsEnabled){ ValidateTrackingRecords $userTable $TablePKList }
    }
}

function ValidateTrackingRecords([String] $table, [Array] $tablePKList){
    Try{
    Write-Host "Running ValidateTrackingRecords for" $table "..." -foreground Green
    $tableNameWithoutSchema = ($table.Replace("[","").Replace("]","").Split('.'))[1]
    
    $sbQuery = New-Object -TypeName "System.Text.StringBuilder"
    
    [void]$sbQuery.Append("SELECT COUNT(*) AS C FROM DataSync.")
    [void]$sbQuery.Append($tableNameWithoutSchema)
    [void]$sbQuery.Append("_dss_tracking t WITH (NOLOCK) WHERE sync_row_is_tombstone=0 AND NOT EXISTS (SELECT * FROM ")
    [void]$sbQuery.Append($table)
    [void]$sbQuery.Append(" s WITH (NOLOCK) WHERE ")
    for ($i=0; $i -lt $tablePKList.Length; $i++) {
        if($i -gt 0) { [void]$sbQuery.Append(" AND ") }
        [void]$sbQuery.Append("t."+$tablePKList[$i] + " = s."+$tablePKList[$i] )
    }
    [void]$sbQuery.Append(")")
    
    $previousMemberCommandTimeout = $MemberCommand.CommandTimeout
    $MemberCommand.CommandTimeout = $ExtendedValidationsCommandTimeout
    $MemberCommand.CommandText = $sbQuery.ToString()
    $result = $MemberCommand.ExecuteReader()
    $datatable = new-object “System.Data.DataTable”
    $datatable.Load($result)
    $count = $datatable | select C -ExpandProperty C
    $MemberCommand.CommandTimeout = $previousMemberCommandTimeout

    if($count -ne 0){
        $msg = "WARNING: Tracking Records for Table " + $table + " may have " + $count + " invalid records!" 
        Write-Host $msg -foreground Red
        [void]$errorSummary.AppendLine($msg) 
    }
    else{
        $msg = "No issues detected in Tracking Records for Table " + $table 
        Write-Host $msg -foreground Green
    }
    
    }
    Catch
    {
        Write-Host "Error at ValidateTrackingRecords" $table -foreground "Red"
        Write-Host $_.Exception.Message        
    }     
}

function ValidateTrackingTables([Array] $tables){
    $allTrackingTableList.AddRange($tables)  
    foreach ($table in $tables) 
    {
        ValidateTrackingTable($table)
    } 
}

function ValidateTrackingTable([String] $table){
    #Write-Host "Validating Tracking Table : " $table
    $query = "SELECT COUNT(*) AS C FROM INFORMATION_SCHEMA.TABLES WHERE '['+TABLE_SCHEMA+'].['+ TABLE_NAME + ']' = '" + $table + "'"

    $MemberCommand.CommandText = $query
    $result = $MemberCommand.ExecuteReader()
    $datatable = new-object “System.Data.DataTable”
    $datatable.Load($result)
    $count = $datatable | select C -ExpandProperty C
    if($count -eq 1){
        Write-Host "Tracking Table " $table "Exists" -foreground "Green" }
    if($count -eq 0){
        Write-Host "WARNING: Tracking Table " $table "IS MISSING!" -foreground "Red" }    
}

function ValidateTriggers([Array] $triggers){ 
    $allTriggersList.AddRange($triggers)
    foreach ($trigger in $triggers) 
    {
        ValidateTrigger($trigger)
    } 
}

function ValidateTrigger([String] $triggerName){
    #Write-Host "Validating Trigger : " $triggerName

    $query = "
    SELECT Count(*) as C
    FROM sys.triggers tr 
    INNER JOIN sys.tables t ON tr.parent_id = t.object_id 
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id 
    WHERE '['+s.name+'].['+ tr.name+']' = '" + $triggerName + "'"

    $MemberCommand.CommandText = $query
    $result = $MemberCommand.ExecuteReader()
    $table = new-object “System.Data.DataTable”
    $table.Load($result)
    $count = $table | select C -ExpandProperty C
    if($count -eq 1){
        Write-Host "Trigger " $triggerName "Exists" -foreground "Green" }
    if($count -eq 0){
        Write-Host "WARNING: Trigger " $triggerName "IS MISSING!" -foreground "Red" }
}

function ValidateSPs([Array] $SPs){ 
    $allSPsList.AddRange($SPs)
    foreach ($SP in $SPs) 
    {
        ValidateSP($SP)
    } 
}

function ValidateSP([String] $SP){
    #Write-Host "Validating Procedure : " $SP
    
    $query = "
    SELECT COUNT(*) AS C  
    FROM sys.procedures p
    INNER JOIN sys.schemas s ON p.schema_id = s.schema_id  
    WHERE '['+s.name+'].['+ p.name+']' = '" + $SP + "'"
    
    $MemberCommand.CommandText = $query
    $result = $MemberCommand.ExecuteReader()
    $table = new-object “System.Data.DataTable”
    $table.Load($result)
    $count = $table | select C -ExpandProperty C
    if($count -eq 1){
        Write-Host "Procedure " $SP "Exists" -foreground "Green" }
    if($count -eq 0){
        Write-Host "WARNING: Procedure " $SP "IS MISSING!" -foreground "Red" }
}

function DetectTrackingTableLeftovers(){
    $allTrackingTableString = "'$($allTrackingTableList -join "','")'"
    $query = "SELECT '['+TABLE_SCHEMA+'].['+ TABLE_NAME + ']' as FullTableName, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE '%_dss_tracking' AND '['+TABLE_SCHEMA+'].['+ TABLE_NAME + ']' NOT IN (" + $allTrackingTableString + ")"
    $MemberCommand.CommandText = $query
    $result = $MemberCommand.ExecuteReader()
    $datatable = new-object “System.Data.DataTable”
    $datatable.Load($result)

    if(($datatable.FullTableName).Count -eq 0)
    { 
        Write-Host "There are no Tracking Table leftovers" -foreground "Green"  
    }
    else 
    {
        foreach ($leftover in $datatable) 
        {
            Write-Host "WARNING: Tracking Table" $leftover.FullTableName "should be a leftover." -foreground "yellow"             
            $deleteStatement = "Drop Table " + $leftover.FullTableName + ";"
            #Write-Host $deleteStatement -ForegroundColor yellow
            [void]$runnableScript.AppendLine($deleteStatement)
            [void]$runnableScript.AppendLine("GO")

            $leftover.TABLE_NAME = ($leftover.TABLE_NAME -replace "_dss_tracking", "")
            $query = "SELECT [object_id] FROM [DataSync].[provision_marker_dss] WHERE [owner_scope_local_id] = 0 and object_name([object_id]) = '" + $leftover.TABLE_NAME +"'"
            $MemberCommand.CommandText = $query
            $provision_marker_result2 = $MemberCommand.ExecuteReader()
            $provision_marker_leftovers2 = new-object “System.Data.DataTable”
            $provision_marker_leftovers2.Load($provision_marker_result2)
            
            foreach($provision_marker_leftover2 in $provision_marker_leftovers2)
            {
                $deleteStatement = "DELETE FROM [DataSync].[provision_marker_dss] WHERE [owner_scope_local_id] = 0 and [object_id] = " + $provision_marker_leftover2.object_id + " --" + $leftover.TABLE_NAME
                Write-Host "WARNING: [DataSync].[provision_marker_dss] WHERE [owner_scope_local_id] = 0 and [object_id] = " $provision_marker_leftover2.object_id "("  $leftover.TABLE_NAME ") should be a leftover." -foreground "yellow"
                #Write-Host $deleteStatement -ForegroundColor yellow
                [void]$runnableScript.AppendLine($deleteStatement)
                [void]$runnableScript.AppendLine("GO")
            }
        }
    }  
}

function DetectTriggerLeftovers(){
    $allTriggersString = "'$($allTriggersList -join "','")'"
    $query = "SELECT '['+s.name+'].['+ trig.name+']'
    FROM sys.triggers trig
    INNER JOIN sys.tables t ON trig.parent_id = t.object_id 
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id 
    WHERE trig.name like '&_dss_&' AND '['+s.name+'].['+ trig.name+']' NOT IN (" + $allTriggersString + ")"
    
    $MemberCommand.CommandText = $query
    $result = $MemberCommand.ExecuteReader()
    $datatable = new-object “System.Data.DataTable”
    $datatable.Load($result)

    if(($datatable.Column1).Count -eq 0)
    { 
        Write-Host "There are no Trigger leftovers" -foreground "Green"  
    }
    else 
    {
        foreach ($leftover in $datatable.Column1) 
        {
            Write-Host "WARNING: Trigger" $leftover "should be a leftover." -foreground "yellow"             
            $deleteStatement = "Drop Trigger " + $leftover + ";"
            #Write-Host $deleteStatement -ForegroundColor yellow
            [void]$runnableScript.AppendLine($deleteStatement)
            [void]$runnableScript.AppendLine("GO")
        }
    } 
}

function DetectProcedureLeftovers(){
    $allSPsString = "'$($allSPsList -join "','")'"
    $query = "SELECT '['+s.name+'].['+ p.name+']' 
    FROM sys.procedures p
    INNER JOIN sys.schemas s ON p.schema_id = s.schema_id  
    WHERE p.name like '%_dss_%' AND '['+s.name+'].['+ p.name+']' NOT IN (" + $allSPsString + ")"
    
    $MemberCommand.CommandText = $query
    $result = $MemberCommand.ExecuteReader()
    $datatable = new-object “System.Data.DataTable”
    $datatable.Load($result)

    if(($datatable.Column1).Count -eq 0)
    { 
        Write-Host "There are no Procedure leftovers" -foreground "Green"  
    }
    else 
    {
        foreach ($leftover in $datatable.Column1) 
        {
            Write-Host "WARNING: Procedure" $leftover "should be a leftover." -foreground "yellow"             
            $deleteStatement = "Drop Procedure " + $leftover + ";"
            #Write-Host $deleteStatement -ForegroundColor yellow
            [void]$runnableScript.AppendLine($deleteStatement)
            [void]$runnableScript.AppendLine("GO") 
        }
    } 
}

function ValidateFKDependencies([Array] $userTables){    
           
    $allTablesFKString = "'$($userTables -join "','")'"
    
    $query = "SELECT 
OBJECT_NAME(fk.parent_object_id) TableName
,OBJECT_NAME(fk.constraint_object_id) FKName
,OBJECT_NAME(fk.referenced_object_id) ParentTableName
,t.name TrackingTableName
FROM sys.foreign_key_columns fk
INNER JOIN sys.tables t2 ON t2.name = OBJECT_NAME(fk.parent_object_id)
INNER JOIN sys.schemas s ON s.schema_id = t2.schema_id
LEFT OUTER JOIN sys.tables t ON t.name like OBJECT_NAME(fk.referenced_object_id)+'_dss_tracking'
WHERE t.name IS NULL AND '['+s.name +'].['+OBJECT_NAME(fk.parent_object_id)+']' IN (" + $allTablesFKString + ")" 
           
    $MemberCommand.CommandText = $query
    $result = $MemberCommand.ExecuteReader()
    $datatable = new-object “System.Data.DataTable”
    $datatable.Load($result)

    if($datatable.Rows.Count -gt 0)
    {
         $msg = "WARNING: Missing tables in the sync group due to FK references:" 
         Write-Host $msg -foreground Red
         [void]$errorSummary.AppendLine($msg)

         foreach ($fkrow in $datatable) 
         {
            $msg = "- The " + $fkrow.FKName + " in " + $fkrow.TableName + " needs " + $fkrow.ParentTableName
            Write-Host $msg -foreground Yellow
            [void]$errorSummary.AppendLine($msg)
         }
    }
    else
    {
        Write-Host "No FKs referencing tables not used in sync group detected" -ForegroundColor Green
    }
}

function ValidateProvisionMarker{
    $query = "select object_name(object_id) TableName, object_id, owner_scope_local_id 
from datasync.provision_marker_dss where object_id in (
select tbl1.object_id from sys.tables tbl1
left join sys.tables tbl2 on schema_name(tbl2.schema_id) = 'DataSync' and tbl2.name like (tbl1.name + '_dss_tracking')
left join datasync.provision_marker_dss marker on marker.owner_scope_local_id = 0 and marker.object_id = tbl1.object_id
where schema_name(tbl1.schema_id) <> 'DataSync'and tbl2.name is not null and marker.object_id is null )" 
           
    $MemberCommand.CommandText = $query
    $result = $MemberCommand.ExecuteReader()
    $datatable = new-object “System.Data.DataTable”
    $datatable.Load($result)

    if($datatable.Rows.Count -gt 0)
    {
         $msg = "WARNING: ValidateProvisionMarker found some possible issues with:"
         Write-Host $msg -foreground Yellow
         [void]$errorSummary.AppendLine($msg)
         
         foreach ($row in $datatable) 
         {
            $msg = "- " + $row.TableName + " | " + $row.object_id + " | " + $row.owner_scope_local_id
            Write-Host $msg -foreground Yellow
            [void]$errorSummary.AppendLine($msg)
         }
    }
    else
    {
        Write-Host "ValidateProvisionMarker did not detect any issue" -ForegroundColor Green
    }
}

function ValidateCircularReferences{
Try
{
    $query = "SELECT OBJECT_SCHEMA_NAME(fk1.parent_object_id) + '.' + OBJECT_NAME(fk1.parent_object_id) Table1, OBJECT_SCHEMA_NAME(fk2.parent_object_id) + '.' + OBJECT_NAME(fk2.parent_object_id) Table2
,fk1.name FK1Name, fk2.name FK2Name
FROM sys.foreign_keys AS fk1
INNER JOIN sys.foreign_keys AS fk2 ON fk1.parent_object_id = fk2.referenced_object_id AND fk2.parent_object_id = fk1.referenced_object_id
WHERE fk1.parent_object_id <> fk2.parent_object_id;" 
    $MemberCommand.CommandText = $query
    $result = $MemberCommand.ExecuteReader()
    $datatable = new-object “System.Data.DataTable”
    $datatable.Load($result)

    if($datatable.Rows.Count -gt 0)
    {
         $msg = "WARNING: ValidateCircularReferences found some circular references in this database:" 
         Write-Host $msg -foreground Yellow
         [void]$errorSummary.AppendLine($msg)
         
         foreach ($row in $datatable) 
         {
            $msg = "- " +$row.Table1 +" | " +$row.Table2 +" | " +$row.FK1Name +" | " +$row.FK2Name
            Write-Host $msg -foreground Yellow
            [void]$errorSummary.AppendLine($msg)
         }
    }
    else
    {
        Write-Host "ValidateCircularReferences did not detect any issue" -ForegroundColor Green
    }
}
Catch
{
    Write-Host ValidateCircularReferences exception:
    Write-Host $_.Exception.Message    
}
}

function ValidateTableNames{
Try
{
    $query = "SELECT DISTINCT t1.name AS TableName FROM sys.tables t1 LEFT JOIN sys.tables t2 ON t1.name = t2.name AND t1.object_id <> t2.object_id WHERE (t2.schema_id) IS NOT NULL" 
    $MemberCommand.CommandText = $query
    $result = $MemberCommand.ExecuteReader()
    $datatable = new-object “System.Data.DataTable”
    $datatable.Load($result)

    if($datatable.Rows.Count -gt 0)
    {
         $msg = "INFO: ValidateTableNames found some tables names in multiple schemas in this database:" 
         Write-Host $msg -foreground Yellow
         [void]$errorSummary.AppendLine($msg)
         
         foreach ($row in $datatable) 
         {
            $msg = "- " +$row.TableName + " seems to exist in multiple schemas!" 
            Write-Host $msg -foreground Yellow
            [void]$errorSummary.AppendLine($msg)
         }
    }
    else
    {
        Write-Host "ValidateTableNames did not detect any issue" -ForegroundColor Green
    }
}
Catch
{
    Write-Host ValidateTableNames exception:
    Write-Host $_.Exception.Message    
}
}

function ValidateObjectNames{
Try
{
    $query = "SELECT table_schema, 
       table_name, 
       column_name 
FROM   information_schema.columns 
WHERE  table_name LIKE '%.%' 
        OR table_name LIKE '%[[]%' 
        OR table_name LIKE '%]%' 
        OR column_name LIKE '%.%' 
        OR column_name LIKE '%[[]%' 
        OR column_name LIKE '%]%' " 
    $MemberCommand.CommandText = $query
    $result = $MemberCommand.ExecuteReader()
    $datatable = new-object “System.Data.DataTable”
    $datatable.Load($result)

    if($datatable.Rows.Count -gt 0)
    {
         $msg = "WARNING: ValidateObjectNames found some issues:" 
         Write-Host $msg -foreground Yellow
         [void]$errorSummary.AppendLine($msg)
         
         foreach ($row in $datatable) 
         {
            $msg = "- [" +$row.table_schema +"].[" +$row.table_name +"].[" +$row.column_name + "]"
            Write-Host $msg -foreground Yellow
            [void]$errorSummary.AppendLine($msg)
         }
    }
    else
    {
        Write-Host "ValidateObjectNames did not detect any issue" -ForegroundColor Green
    }
}
Catch
{
    Write-Host ValidateObjectNames exception:
    Write-Host $_.Exception.Message    
}
}

function DetectComputedColumns{
Try
{
    $query = "SELECT SCHEMA_NAME(T.schema_id) AS SchemaName, T.name AS TableName, C.name AS ColumnName FROM sys.objects AS T JOIN sys.columns AS C ON T.object_id = C.object_id WHERE  T.type = 'U' AND C.is_computed = 1;" 
    $MemberCommand.CommandText = $query
    $result = $MemberCommand.ExecuteReader()
    $datatable = new-object “System.Data.DataTable”
    $datatable.Load($result)

    if($datatable.Rows.Count -gt 0)
    {
         $msg = "INFO: Computed columns detected (they cannot be part of sync schema):" 
         Write-Host $msg -foreground Yellow
         [void]$errorSummary.AppendLine($msg)
         
         foreach ($row in $datatable) 
         {
            $msg = "- [" +$row.SchemaName +"].[" +$row.TableName +"].[" +$row.ColumnName + "]"
            Write-Host $msg -foreground Yellow
            [void]$errorSummary.AppendLine($msg)
         }
    }
    else
    {
        Write-Host "DetectComputedColumns did not detect any computed column" -ForegroundColor Green
    }
}
Catch
{
    Write-Host ValidateObjectNames exception:
    Write-Host $_.Exception.Message    
}
}

function GetUIHistory{
Try
{
    $query = "SELECT TOP(20) ui.[completionTime], sg.[name] SyncGroupName, ud.[database] DatabaseName, ui.[detailEnumId] OperationResult, 
CAST (ui.detailStringParameters as XML).value('(/ArrayOfString//string/node())[1]', 'nvarchar(max)') as Error
FROM [dss].[UIHistory] AS ui
INNER JOIN [dss].[syncgroup] AS sg on ui.syncgroupId = sg.id
INNER JOIN [dss].[userdatabase] AS ud on ui.databaseid = ud.id
ORDER BY ui.[completionTime] DESC"
 
    $SyncDbCommand.CommandText = $query
    $result = $SyncDbCommand.ExecuteReader()
    $datatable = new-object “System.Data.DataTable”
    $datatable.Load($result)

    if($datatable.Rows.Count -gt 0)
    {
         $msg = "UI History:" 
         Write-Host $msg -foreground White         
         $datatable | Format-Table -Wrap -AutoSize
    }
}
Catch
{
    Write-Host GetUIHistory exception:
    Write-Host $_.Exception.Message    
}    
}

function ValidateDSSMember(){
    Try
    {
        if(-not($HealthChecksEnabled)) {return}
        $runnableScript = New-Object -TypeName "System.Text.StringBuilder"
        $errorSummary = New-Object -TypeName "System.Text.StringBuilder"
        $allTrackingTableList = New-Object System.Collections.ArrayList
        $allTriggersList = New-Object System.Collections.ArrayList
        $allSPsList = New-Object System.Collections.ArrayList

        $SyncDbConnection = New-Object System.Data.SqlClient.SQLConnection
        $SyncDbConnection.ConnectionString = [string]::Format("Server=tcp:{0},1433;Initial Catalog={1};Persist Security Info=False;User ID={2};Password={3};MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;", $SyncDbServer, $SyncDbDatabase, $SyncDbUser, $SyncDbPassword)
        
        Write-Host Connecting to SyncDB $SyncDbServer"/"$SyncDbDatabase
        Try
        {
            $SyncDbConnection.Open()
        }
        Catch
        {
            Write-Host $_.Exception.Message
            Break
        }
        
        $SyncDbCommand = New-Object System.Data.SQLClient.SQLCommand
        $SyncDbCommand.Connection = $SyncDbConnection

        $SyncDbCommand.CommandText = "SELECT count(*) as C FROM [dss].[syncgroup]"
        $SyncDbMembersResult = $SyncDbCommand.ExecuteReader()
        $SyncDbMembersDataTableA = new-object “System.Data.DataTable”
        $SyncDbMembersDataTableA.Load($SyncDbMembersResult)        

        $SyncDbCommand.CommandText = "SELECT count(*) as C FROM [dss].[syncgroupmember]"
        $SyncDbMembersResult = $SyncDbCommand.ExecuteReader()
        $SyncDbMembersDataTableB = new-object “System.Data.DataTable”
        $SyncDbMembersDataTableB.Load($SyncDbMembersResult)        
        
        $SyncDbCommand.CommandText = "SELECT count(*) as C FROM [dss].[agent]"
        $SyncDbMembersResult = $SyncDbCommand.ExecuteReader()
        $SyncDbMembersDataTableC = new-object “System.Data.DataTable”
        $SyncDbMembersDataTableC.Load($SyncDbMembersResult)  
        
        Write-Host $SyncDbMembersDataTableA.C sync groups / $SyncDbMembersDataTableB.C sync group members / $SyncDbMembersDataTableC.C sync agents found in this sync metadata database
        Write-Host
        GetUIHistory        
        Write-Host
        Write-Host Getting scopes in SyncDB for this member database...
        
        $SyncDbCommand.CommandText = "SELECT m.[scopename]
        ,sg.name as SyncGroupName
        ,CAST(sg.schema_description as varchar(max)) as SchemaDescription
        --,ud.[database] as HubDatabase
        --,ud.server as HubServer
        ,m.[name] as MemberName
        --,m.[memberstate] as State
        --,m.[hubstate] as HubState
        ,m.[jobid] as JobId
        ,COUNT(mq.[MessageId]) as Messages
        --,ud2.[server] as MemberServer
        --,ud2.[database] as MemberDatabase
        ,enum1.Name as State
		,enum2.Name as HubState
        ,enum3.Name as SyncDirection
        FROM [dss].[syncgroup] as sg
        INNER JOIN [dss].[userdatabase] as ud on sg.hub_memberid = ud.id
        LEFT JOIN [dss].[syncgroupmember] as m on sg.id = m.syncgroupid
        LEFT JOIN [dss].[EnumType] as enum1 on (enum1.Type='SyncGroupMemberState' and enum1.EnumId = m.memberstate)
		LEFT JOIN [dss].[EnumType] as enum2 on (enum2.Type='SyncGroupMemberState' and enum2.EnumId = m.hubstate)
        LEFT JOIN [dss].[EnumType] as enum3 on (enum3.Type='DssSyncDirection' and enum3.EnumId = m.syncdirection)
        LEFT JOIN [dss].[userdatabase] as ud2 on m.databaseid = ud2.id
        left outer join [TaskHosting].[Job] job on m.JobId = job.JobId
        left outer join [TaskHosting].[MessageQueue] mq on job.JobId = mq.JobId 
        WHERE (ud.server = '" + $Server + "' and ud.[database] = '" + $Database + "') 
        or (ud2.[server] = '" + $Server + "' and ud2.[database] = '" + $Database + "')
        GROUP BY m.[scopename],sg.name,CAST(sg.schema_description as varchar(max)),m.[name],m.[memberstate],m.[hubstate],m.[jobid],enum1.Name,enum2.Name,enum3.Name"
        $SyncDbMembersResult = $SyncDbCommand.ExecuteReader()
        $SyncDbMembersDataTable = new-object “System.Data.DataTable”
        $SyncDbMembersDataTable.Load($SyncDbMembersResult)
        
        Write-Host $SyncDbMembersDataTable.Rows.Count members found in this sync metadata database
        $SyncDbMembersDataTable.Rows | Sort-Object -Property scopename | Select scopename, SyncGroupName, MemberName, SyncDirection, State, HubState, JobId, Messages | Format-Table -Wrap -AutoSize
        $scopesList = $SyncDbMembersDataTable.Rows | Select -ExpandProperty scopename
        
        if(($SyncDbMembersDataTable.Rows | Measure-Object Messages -Sum).Sum -gt 0)
        {
            $allJobIds = "'$(($SyncDbMembersDataTable.Rows | Select -ExpandProperty JobId | Where-Object { $_.ToString() -ne '' }) -join "','")'"
            $SyncDbCommand.CommandText = "select job.[JobId]
            ,job.[IsCancelled]
            --,job.[InitialInsertTimeUTC]
            ,job.[JobType]
            ,job.[TaskCount]
            ,job.[CompletedTaskCount]
            --,job.[TracingId]
            --,m.[JobId]
            ,m.[MessageId]
            ,m.[MessageType]
            --,m.[InitialInsertTimeUTC]
            --,m.[InsertTimeUTC]
            --,m.[UpdateTimeUTC]
            ,m.[ExecTimes]
            ,m.[ResetTimes]
            --,m.[Version]
            --,m.[TracingId]
            --,m.[QueueId]
            --,m.[WorkerId] 
            from [TaskHosting].[Job] job 
            left outer join [TaskHosting].[MessageQueue] m on job.JobId = m.JobId
            where job.JobId IN ("+$allJobIds+")"
            $SyncJobsResult = $SyncDbCommand.ExecuteReader()
            $SyncJobsDataTable = new-object “System.Data.DataTable”
            $SyncJobsDataTable.Load($SyncJobsResult)
            $SyncJobsDataTable | Format-Table -Wrap -AutoSize
        }
        
        $MemberConnection = New-Object System.Data.SqlClient.SQLConnection
        $MemberConnection.ConnectionString = [string]::Format("Server={0};Initial Catalog={1};Persist Security Info=False;User ID={2};Password={3};MultipleActiveResultSets=False;Connection Timeout=30;", $Server, $Database, $MbrUser, $MbrPassword)
        
        Write-Host
        Write-Host Connecting to Member $Server"/"$Database
        Try
        {
            $MemberConnection.Open()
        }
        Catch
        {
            Write-Host $_.Exception.Message
            Break
        }    

        $MemberCommand = New-Object System.Data.SQLClient.SQLCommand
        $MemberCommand.Connection = $MemberConnection
        
        ### Database Validations ###
        ValidateCircularReferences
        ValidateTableNames
        ValidateObjectNames
        DetectComputedColumns
                    
        Write-Host Getting scopes in this member database...
        
        $MemberCommand.CommandText = "SELECT [sync_scope_name], [scope_local_id], [scope_config_id],[config_data],[scope_status] FROM [DataSync].[scope_config_dss] AS sc LEFT OUTER JOIN [DataSync].[scope_info_dss] AS si ON si.scope_config_id = sc.config_id"
        $MemberResult = $MemberCommand.ExecuteReader()
        $MemberScopes = new-object “System.Data.DataTable”
        $MemberScopes.Load($MemberResult)
         
        Write-Host $MemberScopes.Rows.Count scopes found in member
        $MemberScopes.Rows | Select sync_scope_name, scope_config_id, scope_status, scope_local_id | Sort-Object -Property sync_scope_name | Format-Table -Wrap -AutoSize
        Write-Host
        
        $global:Connection = $MemberConnection
        
        foreach($scope in $MemberScopes)
        {
            Write-Host
            Write-Host "Validating scope " $scope.scope_config_id
            if($scope.sync_scope_name -notin $scopesList)
            {
                Write-Host "WARNING:" [DataSync].[scope_config_dss].[config_id] $scope.scope_config_id "should be a leftover." -foreground "yellow"  
                Write-Host "WARNING:" [DataSync].[scope_info_dss].[scope_local_id] $scope.scope_local_id "should be a leftover." -foreground "yellow"  
                
                $deleteStatement = "DELETE FROM [DataSync].[scope_config_dss] WHERE [config_id] = '"+ $scope.scope_config_id+ "'"
                #Write-Host $deleteStatement -ForegroundColor yellow
                [void]$runnableScript.AppendLine($deleteStatement)
                [void]$runnableScript.AppendLine("GO")
                    
                $deleteStatement = "DELETE FROM [DataSync].[scope_info_dss] WHERE [scope_local_id] = '"+ $scope.scope_local_id + "'" 
                #Write-Host $deleteStatement -ForegroundColor yellow
                [void]$runnableScript.AppendLine($deleteStatement)
                [void]$runnableScript.AppendLine("GO")

                $query = "SELECT [object_id], object_name([object_id]) as TableName FROM [DataSync].[provision_marker_dss] WHERE [owner_scope_local_id] = " + $scope.scope_local_id
                $MemberCommand.CommandText = $query
                $provision_marker_result = $MemberCommand.ExecuteReader()
                $provision_marker_leftovers = new-object “System.Data.DataTable”
                $provision_marker_leftovers.Load($provision_marker_result)

                foreach($provision_marker_leftover in $provision_marker_leftovers)
                {
                    $deleteStatement = "DELETE FROM [DataSync].[provision_marker_dss] WHERE [owner_scope_local_id] = "+$scope.scope_local_id+" and [object_id] = " + $provision_marker_leftover.object_id + " --" + $provision_marker_leftover.TableName
                    Write-Host "WARNING: [DataSync].[provision_marker_dss] WHERE [owner_scope_local_id] = " $scope.scope_local_id  " and [object_id] = " $provision_marker_leftover.object_id " (" $provision_marker_leftover.TableName ") should be a leftover." -foreground "yellow"
                    #Write-Host $deleteStatement -ForegroundColor yellow
                    [void]$runnableScript.AppendLine($deleteStatement)
                    [void]$runnableScript.AppendLine("GO")
                }
            }
            else
            {
                $xmlcontent = [xml]$scope.config_data                
                $global:scope_config_data = $xmlcontent

                Try
                {
                    $sgSchema = $SyncDbMembersDataTable | Where-Object {$_.scopename -eq $scope.sync_scope_name} | Select SchemaDescription
                    $global:sgSchemaXml = ([xml]$sgSchema.SchemaDescription).DssSyncScopeDescription.TableDescriptionCollection.DssTableDescription
                }
                Catch
                {
                    $global:sgSchemaXml = $null
                    $ErrorMessage = $_.Exception.Message
                    Write-Host "Was not able to get SchemaDescription:" + $ErrorMessage 
                }
                        
                ### Validations ###
        
                #Tables
                ValidateTables ($xmlcontent.SqlSyncProviderScopeConfiguration.Adapter | Select -ExpandProperty GlobalName)
        
                #Tracking Tables
                ValidateTrackingTables($xmlcontent.SqlSyncProviderScopeConfiguration.Adapter | Select -ExpandProperty TrackingTable)
                
                ##Triggers   
                ValidateTriggers($xmlcontent.SqlSyncProviderScopeConfiguration.Adapter | Select -ExpandProperty InsTrig)
                ValidateTriggers($xmlcontent.SqlSyncProviderScopeConfiguration.Adapter | Select -ExpandProperty UpdTrig)
                ValidateTriggers($xmlcontent.SqlSyncProviderScopeConfiguration.Adapter | Select -ExpandProperty DelTrig)
                
                ## Procedures
                if($xmlcontent.SqlSyncProviderScopeConfiguration.Adapter.SelChngProc){ ValidateSPs($xmlcontent.SqlSyncProviderScopeConfiguration.Adapter.SelChngProc) }
                if($xmlcontent.SqlSyncProviderScopeConfiguration.Adapter.SelRowProc){ ValidateSPs($xmlcontent.SqlSyncProviderScopeConfiguration.Adapter.SelRowProc) }
                if($xmlcontent.SqlSyncProviderScopeConfiguration.Adapter.InsProc){ ValidateSPs($xmlcontent.SqlSyncProviderScopeConfiguration.Adapter.InsProc) }
                if($xmlcontent.SqlSyncProviderScopeConfiguration.Adapter.UpdProc){ ValidateSPs($xmlcontent.SqlSyncProviderScopeConfiguration.Adapter.UpdProc) }
                if($xmlcontent.SqlSyncProviderScopeConfiguration.Adapter.DelProc){ ValidateSPs($xmlcontent.SqlSyncProviderScopeConfiguration.Adapter.DelProc) }
                if($xmlcontent.SqlSyncProviderScopeConfiguration.Adapter.InsMetaProc){ ValidateSPs($xmlcontent.SqlSyncProviderScopeConfiguration.Adapter.InsMetaProc) }
                if($xmlcontent.SqlSyncProviderScopeConfiguration.Adapter.UpdMetaProc){ ValidateSPs($xmlcontent.SqlSyncProviderScopeConfiguration.Adapter.UpdMetaProc) }
                if($xmlcontent.SqlSyncProviderScopeConfiguration.Adapter.DelMetaProc){ ValidateSPs($xmlcontent.SqlSyncProviderScopeConfiguration.Adapter.DelMetaProc) }
                if($xmlcontent.SqlSyncProviderScopeConfiguration.Adapter.BulkInsProc){ ValidateSPs($xmlcontent.SqlSyncProviderScopeConfiguration.Adapter.BulkInsProc) }
                if($xmlcontent.SqlSyncProviderScopeConfiguration.Adapter.BulkUpdProc){ ValidateSPs($xmlcontent.SqlSyncProviderScopeConfiguration.Adapter.BulkUpdProc) }
                if($xmlcontent.SqlSyncProviderScopeConfiguration.Adapter.BulkDelProc){ ValidateSPs($xmlcontent.SqlSyncProviderScopeConfiguration.Adapter.BulkDelProc) }
                
                #Constraints
                ValidateFKDependencies ($xmlcontent.SqlSyncProviderScopeConfiguration.Adapter | Select -ExpandProperty GlobalName)        
                }
        }
        
        ### Provisioning Issues ###
        foreach($syncDBscope in $SyncDbMembersDataTable){
            $scopeExistsInMember = $MemberScopes | Where-Object {$_.sync_scope_name -eq $syncDBscope.scopename}
            if(!$scopeExistsInMember)
            {                
                $unprovisionedSchema = ([xml]$syncDBscope.SchemaDescription).DssSyncScopeDescription.TableDescriptionCollection.DssTableDescription
                foreach($unprovisionedTable in $unprovisionedSchema)
                {
                    $query = "SELECT 
                     c.name 'ColumnName',
                     t.Name 'Datatype',
                     c.max_length 'MaxLength',
                     c.is_nullable 'IsNullable'--,
                     FROM sys.columns c
                     INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
                     WHERE c.object_id = OBJECT_ID('" + $unprovisionedTable.QuotedTableName + "')" 
        
                    $MemberCommand.CommandText = $query
                    $result = $MemberCommand.ExecuteReader()
                    $datatable = new-object “System.Data.DataTable”
                    $datatable.Load($result)

                    foreach($unprovisionedColumn in $unprovisionedTable.ColumnsToSync.DssColumnDescription)
                    {
                        $scopeCol = $datatable | Where-Object ColumnName -eq $unprovisionedColumn.Name
                        if(!$scopeCol)
                        {
                            $msg= "WARNING: "+ $unprovisionedTable.QuotedTableName + ".["+$unprovisionedColumn.Name+"] is missing in this database but exist in sync group schema, maybe preventing provisioning!"
                            Write-Host
                            Write-Host $msg -foreground Red
                            Write-Host
                            [void]$errorSummary.AppendLine($msg)
                        }                
                    }
                }               
            }            
        }

        ### Detect Leftovers ###
        DetectTrackingTableLeftovers
        DetectTriggerLeftovers
        DetectProcedureLeftovers
        
        ### Validations ###
        ValidateProvisionMarker

        if($runnableScript.Length -gt 0)
        {
            Write-Host
            Write-Host --***************************************************************************************************************** -foreground "Green"  
            Write-Host --LEFTOVERS CLEANUP SCRIPT : START                                   -foreground "Green"
            Write-Host --Only applicable when this database is not being used by any other sync group in other regions and/or subscription -foreground "Yellow"  
            Write-Host --***************************************************************************************************************** -foreground "Green"  
            $runnableScript.ToString()
            Write-Host --***************************************************************************************************************** -foreground "Green"  
            Write-Host --LEFTOVERS CLEANUP SCRIPT : END                                     -foreground "Green"  
            Write-Host --***************************************************************************************************************** -foreground "Green"  
            Write-Host
        }
        else
        {
            Write-Host
            Write-Host NO LEFTOVERS DETECTED!
        }

        if($errorSummary.Length -gt 0)
        {
            Write-Host
            Write-Host "*******************************************" -foreground Red  
            Write-Host "             WARNINGS SUMMARY" -foreground Red
            Write-Host "*******************************************" -foreground Red  
            Write-Host $errorSummary.ToString() -foreground Red
            Write-Host
        }
        else
        {
            Write-Host
            Write-Host NO ERRORS DETECTED!
        }
    }
    Finally
    {        
        if($SyncDbConnection){
            Write-Host Closing connecting to SyncDb...
            $SyncDbConnection.Close()
        }
        
        if($MemberConnection){
            Write-Host Closing connecting to Member...
            $MemberConnection.Close()
        }        
    }
}

function Monitor(){
    if($MonitoringEnabled)
    {
        Write-Host ****************************** -ForegroundColor Green
        Write-Host             MONITORING 
        Write-Host ****************************** -ForegroundColor Green
            
        $HubConnection = New-Object System.Data.SqlClient.SQLConnection
        $HubConnection.ConnectionString = [string]::Format("Server=tcp:{0},1433;Initial Catalog={1};Persist Security Info=False;User ID={2};Password={3};MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;", $HubServer, $HubDatabase, $HubUser, $HubPassword)
        $HubCommand = New-Object System.Data.SQLClient.SQLCommand
        $HubCommand.Connection = $HubConnection

        Write-Host Connecting to Hub $HubServer"/"$HubDatabase
        Try
        {
            $HubConnection.Open()
        }
        Catch
        {
            Write-Host $_.Exception.Message
            Break
        }  

        $MemberConnection = New-Object System.Data.SqlClient.SQLConnection
        $MemberConnection.ConnectionString = [string]::Format("Server=tcp:{0},1433;Initial Catalog={1};Persist Security Info=False;User ID={2};Password={3};MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;", $MemberServer, $MemberDatabase, $MemberUser, $MemberPassword)
        $MemberCommand = New-Object System.Data.SQLClient.SQLCommand
        $MemberCommand.Connection = $MemberConnection

        Write-Host Connecting to Member $MemberServer"/"$MemberDatabase
        Try
        {
            $MemberConnection.Open()
        }
        Catch
        {
            Write-Host $_.Exception.Message
            Break
        } 
        
        $HubCommand.CommandText = "SELECT GETUTCDATE() as now"
        $result = $HubCommand.ExecuteReader()
        $datatable = new-object “System.Data.DataTable”
        $datatable.Load($result)
        $lasttime = $datatable.Rows[0].now

        while($true){
            
            Write-Host "Waiting..." $MonitoringIntervalInSeconds "seconds..." -ForegroundColor Green
            Start-Sleep -s $MonitoringIntervalInSeconds
            $lastTimeString = ([DateTime]$lasttime).toString("yyyy-MM-dd HH:mm:ss")
            $lastTimeString = $lastTimeString.Replace('.',':')
            
            Write-Host "Monitoring ("$lastTimeString")..." -ForegroundColor Green

            $query = "select o.name AS What
                      ,p.last_execution_time AS LastExecutionTime
                      , p.execution_count AS ExecutionCount
                      from sys.dm_exec_procedure_stats p
                      inner join sys.objects o
                      on o.object_id = p.object_id
                      inner join sys.schemas s
                      on s.schema_id=o.schema_id
                      where s.name = 'DataSync'
                      and p.last_execution_time > '" + $lastTimeString +"' order by p.last_execution_time desc" 

            $HubCommand.CommandText = $query
            $HubResult = $HubCommand.ExecuteReader()
            $datatable = new-object “System.Data.DataTable”
            $datatable.Load($HubResult)

            if($datatable.Rows.Count -gt 0)
            {    
                 Write-Host "Hub Monitor (SPs) ("$lastTimeString"): new records:" -ForegroundColor Green
                 Write-Host ($datatable | Format-Table | Out-String)
            }
            

            $MemberCommand.CommandText = $query
            $MemberResult = $MemberCommand.ExecuteReader()
            $datatable = new-object “System.Data.DataTable”
            $datatable.Load($MemberResult)

            if($datatable.Rows.Count -gt 0)
            {    
                 Write-Host "Member Monitor (SPs) ("$lastTimeString"): new records:" -ForegroundColor Green
                 Write-Host ($datatable | Format-Table | Out-String)
            }
            

            $query = "SELECT req.session_id as Session,
                      req.status as Status,
                      req.command as Command,
                      req.cpu_time as CPUTime,
                      req.total_elapsed_time as TotalTime,
                      --sqltext.TEXT,
                      SUBSTRING(sqltext.TEXT, CHARINDEX('[DataSync]', sqltext.TEXT), 100) as What
                      FROM sys.dm_exec_requests req
                      CROSS APPLY sys.dm_exec_sql_text(sql_handle) AS sqltext
                      WHERE sqltext.TEXT like '%[DataSync]%'
                      AND sqltext.TEXT not like 'SELECT req.session_id%'" 

            $HubCommand.CommandText = $query
            $HubResult = $HubCommand.ExecuteReader()
            $datatable = new-object “System.Data.DataTable”
            $datatable.Load($HubResult)

            if($datatable.Rows.Count -gt 0)
            {    
                 Write-Host "Hub Monitor (running) ("$lastTimeString"): new records:" -ForegroundColor Green
                 Write-Host ($datatable | Format-Table | Out-String)
            }
            
            $MemberCommand.CommandText = $query
            $MemberResult = $MemberCommand.ExecuteReader()
            $datatable = new-object “System.Data.DataTable”
            $datatable.Load($MemberResult)

            if($datatable.Rows.Count -gt 0)
            {
                Write-Host "Member Monitor (running) ("$lastTimeString"): new records:" -ForegroundColor Green
                Write-Host ($datatable | Format-Table | Out-String)
            }
            
            $lasttime = $lasttime.AddSeconds($MonitoringIntervalInSeconds)
        }
    }
}

cls
Write-Host ************************************************************ -ForegroundColor Green
Write-Host "        Data Sync Health Checker v3.8 Results"              -ForegroundColor Green
Write-Host ************************************************************ -ForegroundColor Green
Write-Host

#Hub
$Server = $HubServer
$Database = $HubDatabase 
$MbrUser = $HubUser
$MbrPassword = $HubPassword
$ExtendedValidationsEnabled = $ExtendedValidationsEnabledForHub 
ValidateDSSMember

#Member
$Server = $MemberServer
$Database = $MemberDatabase 
$MbrUser = $MemberUser
$MbrPassword = $MemberPassword
$ExtendedValidationsEnabled = $ExtendedValidationsEnabledForMember 
ValidateDSSMember

#Monitor
Monitor