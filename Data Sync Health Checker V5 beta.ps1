#Azure SQL Data Sync Health Checker

#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, 
#WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

$HealthChecksEnabled = $true  #Set as $true or $false
$MonitoringMode = 'AUTO'  #Set as AUTO, ENABLED or DISABLED
$MonitoringIntervalInSeconds = 20
$MonitoringDurationInMinutes = 2
$SendAnonymousUsageData = $true

$ExtendedValidationsEnabledForHub = $false  #Attention, this may cause high I/O impact
$ExtendedValidationsEnabledForMember = $false  #Attention, this may cause high I/O impact
$ExtendedValidationsTableFilter = @("All") # To validate all tables
#$ExtendedValidationsTableFilter = @("[dbo].[TableName1]","[dbo].[TableName2]") #to filter tables you need to validate, needs to be formatted like [SchemaName].[TableName]
$ExtendedValidationsCommandTimeout = 900 #seconds
$DumpMetadataSchemasForSyncGroup = '' #leave empty for automatic detection
$DumpMetadataobjectsForTable = '' #needs to be formatted like [SchemaName].[TableName]

$SyncDbServer = '.database.windows.net'
$SyncDbDatabase = ''
$SyncDbUser = '' 
$SyncDbPassword = ''

#Hub (Only SQL Authentication is supported)
$HubServer = '.database.windows.net' 
$HubDatabase = ''
$HubUser = ''
$HubPassword = ''

#Member (Only SQL Authentication is supported)
$MemberServer = ''
$MemberDatabase = '' 
$MemberUser = ''
$MemberPassword = ''





function ValidateTablesVSLocalSchema([Array] $userTables){
    
    if($userTables.Count -eq 0)
    {
        $msg= "WARNING: member schema with 0 tables was detected, maybe related to provisioning issues."
        Write-Host $msg -foreground Red
        [void]$errorSummary.AppendLine($msg)
    }
    else
    {
        Write-Host Schema has $userTables.Count tables
    }
        
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
                    $msg="WARNING: " + $userTable + ".["+$userColumn.ColumnName+"] has a different datatype! (table:"+$userColumn.Datatype+" VS scope:"+$schemaColumn.type+")"
                    Write-Host $msg -foreground "Red"
                    [void]$errorSummary.AppendLine($msg)                    
                } 
                else
                { 
                    [void]$sbCol.Append('  Type(' + $schemaColumn.type +'):OK ')
                }
                
                $colMaxLen=$userColumn.MaxLength

                if($schemaColumn.type -eq 'nvarchar' -or $schemaColumn.type -eq 'nchar'){$colMaxLen=$colMaxLen/2}
                
                if($userColumn.MaxLength -eq -1 -and ($schemaColumn.type -eq 'nvarchar' -or $schemaColumn.type -eq 'nchar' -or $schemaColumn.type -eq 'varbinary' -or $schemaColumn.type -eq 'varchar' -or $schemaColumn.type -eq 'nvarchar')){$colMaxLen='max'}

                if($schemaColumn.size -ne $colMaxLen)
                {
                    [void]$sbCol.Append('  Size(' + $schemaColumn.size +'):NOK ') 
                    $msg= "WARNING: "+ $userTable+ ".["+$userColumn.ColumnName+"] has a different data size!(table:"+$colMaxLen+" VS scope:"+$schemaColumn.size+")"
                    Write-Host $msg -foreground "Red"
                    [void]$errorSummary.AppendLine($msg)
                }
                else
                { 
                    [void]$sbCol.Append('  Size(' + $schemaColumn.size +'):OK ')
                }
                                
                if($schemaColumn.null)
                {
                    if($schemaColumn.null -ne $userColumn.IsNullable)
                    { 
                        [void]$sbCol.Append('  Nullable(' + $schemaColumn.null +'):NOK ')
                        $msg= "WARNING: " +$userTable +".["+$userColumn.ColumnName+"] has a different IsNullable! (table:"+$userColumn.IsNullable+" VS scope:"+$schemaColumn.null+")"
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
        
        if($ExtendedValidationsEnabled -and (($ExtendedValidationsTableFilter -contains 'All') -or ($ExtendedValidationsTableFilter -contains $userTable)))
        { 
            ValidateTrackingRecords $userTable $TablePKList 
        }
    }
}

function ShowRowCount([Array] $userTables){    
    $tablesList = New-Object System.Collections.ArrayList
    foreach($item in $userTables){        
        $tablesList.Add($item) > $null
        $tablesList.Add('[DataSync].[' + ($item.Replace("[","").Replace("]","").Split('.')[1]) + '_dss_tracking]') > $null
    }
    $tablesListStr = "'$($tablesList -join "','")'"
    $query = "SELECT '['+s.name+'].['+ t.name+']' as TableName, SUM(p.rows) as Rows
FROM sys.partitions AS p INNER JOIN sys.tables AS t ON p.[object_id] = t.[object_id]
INNER JOIN sys.schemas AS s ON t.[schema_id] = s.[schema_id] 
WHERE p.index_id IN (0,1) AND '['+s.name+'].['+ t.name+']' IN ("+ $tablesListStr +")
GROUP BY s.name, t.name ORDER BY s.name, t.name" 
    $MemberCommand.CommandText = $query
    $result = $MemberCommand.ExecuteReader()
    $datatable = new-object “System.Data.DataTable”
    $datatable.Load($result) 
    if($datatable.Rows.Count -gt 0)
    {     
         $datatable | Format-Table -Wrap -AutoSize  
    } 
}

function ValidateTablesVSSyncDbSchema($SyncDbScopes){
Try
{
    foreach($SyncDbScope in $SyncDbScopes)
    {
        Write-Host 'Validating Table(s) VS SyncDB for' $SyncDbScope.SyncGroupName':' -foreground "White"
        $ValidateTablesVSSyncDbSchemaIssuesFound = $false
        $syncdbscopeobj = ([xml]$SyncDbScope.SchemaDescription).DssSyncScopeDescription.TableDescriptionCollection.DssTableDescription
        $syncGroupSchemaTables = $syncdbscopeobj | Select -ExpandProperty QuotedTableName
           
        foreach ($syncGroupSchemaTable in $syncGroupSchemaTables) 
        {        
            $syncGroupSchemaColumns = $syncdbscopeobj | Where-Object {$_.QuotedTableName -eq $syncGroupSchemaTable} | Select -ExpandProperty ColumnsToSync
        
            $query = "SELECT 
                         c.name 'ColumnName',
                         t.Name 'Datatype',
                         c.max_length 'MaxLength',
                         c.is_nullable 'IsNullable'
                         FROM sys.columns c
                         INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
                         WHERE c.object_id = OBJECT_ID('" + $syncGroupSchemaTable + "')" 
            $MemberCommand.CommandText = $query
            $result = $MemberCommand.ExecuteReader()
            $datatable = new-object “System.Data.DataTable”
            $datatable.Load($result)

            if($datatable.Rows.Count -eq 0)
            {
                $ValidateTablesVSSyncDbSchemaIssuesFound = $true
                $msg= "WARNING: "+ $syncGroupSchemaTable + " does not exist in the database but exist in the sync group schema."
                Write-Host $msg -foreground Red
                [void]$errorSummary.AppendLine($msg)
            }
            else
            {        
                foreach($syncGroupSchemaColumn in $syncGroupSchemaColumns.DssColumnDescription)
                {
                    $scopeCol = $datatable | Where-Object ColumnName -eq $syncGroupSchemaColumn.Name
                    if(!$scopeCol)
                    {
                        $ValidateTablesVSSyncDbSchemaIssuesFound = $true
                        $msg= "WARNING: "+ $syncGroupSchemaTable+ ".["+$syncGroupSchemaColumn.Name+"] is missing in this database but exist in sync group schema, maybe preventing provisioning/re-provisioning!"
                        Write-Host $msg -foreground Red
                        [void]$errorSummary.AppendLine($msg)                
                    }
                    else
                    {
                        if($syncGroupSchemaColumn.DataType -ne $scopeCol.Datatype)
                        {
                            $ValidateTablesVSSyncDbSchemaIssuesFound = $true 
                            $msg="WARNING: " + $syncGroupSchemaTable + ".["+$syncGroupSchemaColumn.Name+"] has a different datatype! ("+$syncGroupSchemaColumn.DataType+" VS "+$scopeCol.Datatype+")"
                            Write-Host $msg -foreground "Red"
                            [void]$errorSummary.AppendLine($msg)                    
                        }
                        else
                        {
                            $colMaxLen=$scopeCol.MaxLength
                            if($syncGroupSchemaColumn.DataType -eq 'nvarchar' -or $syncGroupSchemaColumn.DataType -eq 'nchar'){$colMaxLen=$colMaxLen/2}
                            if($scopeCol.MaxLength -eq -1 -and ($syncGroupSchemaColumn.DataType -eq 'nvarchar' -or $syncGroupSchemaColumn.DataType -eq 'nchar' -or $syncGroupSchemaColumn.DataType -eq 'varbinary' -or $syncGroupSchemaColumn.DataType -eq 'varchar' -or $syncGroupSchemaColumn.DataType -eq 'nvarchar')){$colMaxLen='max'}

                            if($syncGroupSchemaColumn.DataSize -ne $colMaxLen)
                            {
                                $ValidateTablesVSSyncDbSchemaIssuesFound = $true 
                                $msg="WARNING: " + $syncGroupSchemaTable + ".["+$syncGroupSchemaColumn.Name+"] has a different data size! ("+$syncGroupSchemaColumn.DataSize+" VS "+$scopeCol.MaxLength+")"
                                Write-Host $msg -foreground "Red"
                                [void]$errorSummary.AppendLine($msg)                    
                            }
                        }
                    }               
                }
            }        
        }
        if(!$ValidateTablesVSSyncDbSchemaIssuesFound)
        {
            Write-Host '- No issues detected for' $SyncDbScope.SyncGroupName -foreground "Green"
        }
    }
}
Catch
{
    Write-Host ValidateTablesVSSyncDbSchema exception:
    Write-Host $_.Exception.Message -ForegroundColor Red    
}
}

function ValidateTrackingRecords([String] $table, [Array] $tablePKList){
    Try{
    Write-Host "Running ValidateTrackingRecords for" $table "..." -foreground Green
    $tableNameWithoutSchema = ($table.Replace("[","").Replace("]","").Split('.'))[1]
    
    $sbQuery = New-Object -TypeName "System.Text.StringBuilder"
    $sbDeleteQuery = New-Object -TypeName "System.Text.StringBuilder"
    
    [void]$sbQuery.Append("SELECT COUNT(*) AS C FROM DataSync.")
    [void]$sbQuery.Append($tableNameWithoutSchema)
    [void]$sbQuery.Append("_dss_tracking t WITH (NOLOCK) WHERE sync_row_is_tombstone=0 AND NOT EXISTS (SELECT * FROM ")
    [void]$sbQuery.Append($table)
    [void]$sbQuery.Append(" s WITH (NOLOCK) WHERE ")
    
    [void]$sbDeleteQuery.Append("DELETE DataSync.")
    [void]$sbDeleteQuery.Append($tableNameWithoutSchema)
    [void]$sbDeleteQuery.Append("_dss_tracking FROM DataSync.")
    [void]$sbDeleteQuery.Append($tableNameWithoutSchema)
    [void]$sbDeleteQuery.Append("_dss_tracking t WHERE sync_row_is_tombstone=0 AND NOT EXISTS (SELECT * FROM ")
    [void]$sbDeleteQuery.Append($table)
    [void]$sbDeleteQuery.Append(" s WHERE ")
    
    for ($i=0; $i -lt $tablePKList.Length; $i++) {
        if($i -gt 0) 
        { 
            [void]$sbQuery.Append(" AND ")
            [void]$sbDeleteQuery.Append(" AND ")
        }
        [void]$sbQuery.Append("t."+$tablePKList[$i] + " = s."+$tablePKList[$i] )
        [void]$sbDeleteQuery.Append("t."+$tablePKList[$i] + " = s."+$tablePKList[$i] )
    }
    [void]$sbQuery.Append(")")
    [void]$sbDeleteQuery.Append(")")
    
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
        Write-Host $sbDeleteQuery.ToString() -foreground Yellow
        [void]$errorSummary.AppendLine($msg) 
        [void]$errorSummary.AppendLine($sbDeleteQuery.ToString()) 
    }
    else{
        $msg = "No issues detected in Tracking Records for Table " + $table 
        Write-Host $msg -foreground Green
    }
    
    }
    Catch
    {
        Write-Host "Error at ValidateTrackingRecords" $table -foreground "Red"
        Write-Host $_.Exception.Message -ForegroundColor Red        
    }     
}

function ValidateTrackingTable($table){  
  
    if(![string]::IsNullOrEmpty($table))
    {
        [void]$allTrackingTableList.Add($table)
    }
    
    $query = "SELECT COUNT(*) AS C FROM INFORMATION_SCHEMA.TABLES WHERE '['+TABLE_SCHEMA+'].['+ TABLE_NAME + ']' = '" + $table + "'"

    $MemberCommand.CommandText = $query
    $result = $MemberCommand.ExecuteReader()
    $datatable = new-object “System.Data.DataTable”
    $datatable.Load($result)
    $count = $datatable | select C -ExpandProperty C
    if($count -eq 1){
        Write-Host "Tracking Table " $table "exists" -foreground "Green" }
    if($count -eq 0)
    {
        $msg = "WARNING: Tracking Table " + $table + " IS MISSING!"
        Write-Host $msg -foreground Red
        [void]$errorSummary.AppendLine($msg)
    }
}

function ValidateTrigger([String] $trigger){
    
    if(![string]::IsNullOrEmpty($trigger))
    {    
        [void]$allTriggersList.Add($trigger)
    }

    $query = "
    SELECT tr.name, tr.is_disabled AS 'Disabled'
    FROM sys.triggers tr 
    INNER JOIN sys.tables t ON tr.parent_id = t.object_id 
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id 
    WHERE '['+s.name+'].['+ tr.name+']' = '" + $trigger + "'"

    $MemberCommand.CommandText = $query
    $result = $MemberCommand.ExecuteReader()
    $table = new-object “System.Data.DataTable”
    $table.Load($result)
    $count = $table.Rows.Count
    if($count -eq 1){
        if($table.Rows[0].Disabled -eq 1)
        {
            $msg = "WARNING: Trigger " + $trigger + " exists but is DISABLED!" 
            Write-Host $msg -foreground Red
            [void]$errorSummary.AppendLine($msg)
        }
        else
        {
            Write-Host "Trigger" $trigger "exists and is enabled." -foreground "Green" 
        }
    }
    if($count -eq 0)
    {
        $msg = "WARNING: Trigger " + $trigger + " IS MISSING!" 
        Write-Host $msg -foreground Red
        [void]$errorSummary.AppendLine($msg)
    }
}

function ValidateSP([String] $SP){
    
    if(![string]::IsNullOrEmpty($SP))
    { 
        [void]$allSPsList.Add($SP)
    }
    
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
        Write-Host "Procedure" $SP "exists" -foreground "Green" }
    if($count -eq 0)
    {
        $msg = "WARNING: Procedure " + $SP + " IS MISSING!"
        Write-Host $msg -foreground Red
        [void]$errorSummary.AppendLine($msg)
    }

    if($DumpMetadataobjectsForTable)
    {
        $tableNameWithoutSchema = ($DumpMetadataobjectsForTable.Replace("[","").Replace("]","").Split('.'))[1]
        if($SP.IndexOf($tableNameWithoutSchema) -ne -1)
        {
            $query = "sp_helptext '" + $SP + "'" 
            $MemberCommand.CommandText = $query
            $result = $MemberCommand.ExecuteReader()
            $datatable = new-object “System.Data.DataTable”
            $datatable.Load($result)
            if($datatable.Rows.Count -gt 0)
            {
                $SPname = $SP.Replace("[","").Replace("]","").Replace('.','_')
                $xmlResult = $datatable.Text
                $xmlResult | Out-File -filepath ('.\'+ $Server.Replace('.','_') +'_' + $Database + '_' + $SPname + '.txt')
            }
        }
    }

}

function ValidateBulkType([String] $bulkType, $columns){
    
    if(![string]::IsNullOrEmpty($bulkType))
    { 
        [void]$allBulkTypeList.Add($bulkType)
    }
    
    $query = "select tt.name 'Type',
    c.name 'ColumnName',
    t.Name 'Datatype',
    c.max_length 'MaxLength',
    c.is_nullable 'IsNullable', 
    c.column_id 'ColumnId'
    from sys.table_types tt
    inner join sys.columns c on c.object_id = tt.type_table_object_id
    inner join sys.types t ON c.user_type_id = t.user_type_id
    where '['+ SCHEMA_NAME(tt.schema_id) +'].['+ tt.name+']' ='" + $bulkType + "'" 
    
    $MemberCommand.CommandText = $query
    $result = $MemberCommand.ExecuteReader()
    $table = new-object “System.Data.DataTable”
    $table.Load($result)
    $count = $table.Rows.Count
    if($count -gt 0){
        Write-Host "Type" $bulkType "exists" -foreground "Green"
        foreach($column in $columns)
        {
            $sbCol = New-Object -TypeName "System.Text.StringBuilder"
                        
            $typeColumn = $table.Rows | Where-Object ColumnName -eq $column.name
            
            if(!$typeColumn)
            {
                $msg= "WARNING: "+ $bulkType + ".["+$column.name+"] does not exit!"
                Write-Host $msg -foreground "Red"
                [void]$errorSummary.AppendLine($msg)
                continue
            }

            [void]$sbCol.Append("- [" + $column.name + "] " + $column.param)


            if($column.type -ne $typeColumn.Datatype)
            { 
                [void]$sbCol.Append('  Type(' + $column.type +'):NOK ')
                $msg="WARNING: " + $bulkType + ".["+$column.name+"] has a different datatype! (type:"+$typeColumn.Datatype+" VS scope:"+$column.type+")"
                Write-Host $msg -foreground "Red"
                [void]$errorSummary.AppendLine($msg)                    
            } 
            else
            { 
                [void]$sbCol.Append('  Type(' + $column.type +'):OK ')
            }
            
            $colMaxLen=$typeColumn.MaxLength

            if($column.type -eq 'nvarchar' -or $column.type -eq 'nchar'){$colMaxLen=$colMaxLen/2}
            
            if($typeColumn.MaxLength -eq -1 -and ($column.type -eq 'nvarchar' -or $column.type -eq 'nchar' -or $column.type -eq 'varbinary' -or $column.type -eq 'varchar' -or $column.type -eq 'nvarchar')){$colMaxLen='max'}

            if($column.size -ne $colMaxLen)
            {
                [void]$sbCol.Append('  Size(' + $column.size +'):NOK ') 
                $msg= "WARNING: "+ $bulkType+ ".["+$column.name+"] has a different data size!(type:"+$colMaxLen+" VS scope:"+$column.size+")"
                Write-Host $msg -foreground "Red"
                [void]$errorSummary.AppendLine($msg)
            }
            else
            { 
                [void]$sbCol.Append('  Size(' + $column.size +'):OK ')
            }
                            
            if($column.null)
            {
                if($column.null -ne $typeColumn.IsNullable)
                { 
                    [void]$sbCol.Append('  Nullable(' + $column.null +'):NOK ')
                    $msg= "WARNING: " +$bulkType +".["+$column.name+"] has a different IsNullable! (type:"+$typeColumn.IsNullable+" VS scope:"+$column.null+")"
                    Write-Host $msg -foreground "Red"
                    [void]$errorSummary.AppendLine($msg) 
                } 
                else
                { 
                    [void]$sbCol.Append('  Nullable(' + $column.null +'):OK ')
                }                    
            }
            
            $sbColString = $sbCol.ToString()
            if($sbColString -match 'NOK'){ Write-Host $sbColString -ForegroundColor Red } else { Write-Host $sbColString -ForegroundColor Green }


        }
    }
    if($count -eq 0)
    {
        $msg = "WARNING: Type " + $bulkType + " IS MISSING!"
        Write-Host $msg -foreground Red
        [void]$errorSummary.AppendLine($msg)
    }
}

function DetectTrackingTableLeftovers(){
Try
{
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
                [void]$runnableScript.AppendLine($deleteStatement)
                [void]$runnableScript.AppendLine("GO")
            }
        }
    }
}
Catch
{
    Write-Host DetectTrackingTableLeftovers exception:
    Write-Host $_.Exception.Message -ForegroundColor Red    
} 
}

function DetectTriggerLeftovers(){
Try
{
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
            [void]$runnableScript.AppendLine($deleteStatement)
            [void]$runnableScript.AppendLine("GO")
        }
    }
}
Catch
{
    Write-Host DetectTriggerLeftovers exception:
    Write-Host $_.Exception.Message -ForegroundColor Red    
} 
}

function DetectProcedureLeftovers(){
Try
{
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
            [void]$runnableScript.AppendLine($deleteStatement)
            [void]$runnableScript.AppendLine("GO") 
        }
    }
}
Catch
{
    Write-Host DetectProcedureLeftovers exception:
    Write-Host $_.Exception.Message -ForegroundColor Red    
} 
}

function DetectBulkTypeLeftovers(){
Try
{
    $allBulkTypeString = "'$($allBulkTypeList -join "','")'"
    $query = "select distinct '['+ SCHEMA_NAME(tt.schema_id) +'].['+ tt.name+']' 'Type'
    from sys.table_types tt
    inner join sys.columns c on c.object_id = tt.type_table_object_id
    inner join sys.types t ON c.user_type_id = t.user_type_id
    where SCHEMA_NAME(tt.schema_id) = 'DataSync' and '['+ SCHEMA_NAME(tt.schema_id) +'].['+ tt.name+']' NOT IN (" + $allBulkTypeString + ")"
    
    $MemberCommand.CommandText = $query
    $result = $MemberCommand.ExecuteReader()
    $datatable = new-object “System.Data.DataTable”
    $datatable.Load($result)

    if(($datatable.Type).Count -eq 0)
    { 
        Write-Host "There are no Bulk Type leftovers" -foreground "Green"  
    }
    else 
    {
        foreach ($leftover in $datatable.Type) 
        {
            Write-Host "WARNING: Bulk Type" $leftover "should be a leftover." -foreground "yellow"             
            $deleteStatement = "Drop Type " + $leftover + ";"
            [void]$runnableScript.AppendLine($deleteStatement)
            [void]$runnableScript.AppendLine("GO") 
        }
    }
}
Catch
{
    Write-Host DetectBulkTypeLeftovers exception:
    Write-Host $_.Exception.Message -ForegroundColor Red    
} 
}

function ValidateFKDependencies([Array] $userTables){
Try
{
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
Catch
{
    Write-Host ValidateFKDependencies exception:
    Write-Host $_.Exception.Message -ForegroundColor Red    
}
}

function ValidateProvisionMarker{
Try
{
    $query = "select object_name(object_id) TableName, object_id, owner_scope_local_id 
from DataSync.provision_marker_dss where object_id in (
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
Catch
{
    Write-Host ValidateProvisionMarker exception:
    Write-Host $_.Exception.Message -ForegroundColor Red    
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
    Write-Host $_.Exception.Message -ForegroundColor Red    
}
}

function ValidateTableNames{
Try
{
    $query = "SELECT DISTINCT t1.name AS TableName FROM sys.tables t1 LEFT JOIN sys.tables t2 ON t1.name = t2.name AND t1.object_id <> t2.object_id WHERE (t2.schema_id) IS NOT NULL AND SCHEMA_NAME(t1.schema_id) NOT IN ('dss','TaskHosting')" 
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
    Write-Host $_.Exception.Message -ForegroundColor Red    
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
    Write-Host $_.Exception.Message -ForegroundColor Red    
}
}

function DetectProvisioningIssues{
    
    $query = "with TrackingTables as(
    select REPLACE(name,'_dss_tracking','') as TrackingTableOrigin, name TrackingTable
    from sys.tables
    where SCHEMA_NAME(schema_id) = 'DataSync'
    AND [name] not in ('schema_info_dss','scope_info_dss','scope_config_dss','provision_marker_dss')
    )
    select TrackingTable
    from TrackingTables c
    left outer join sys.tables t on c.TrackingTableOrigin = t.[name]
    where t.[name] is null"
 
    $MemberCommand.CommandText = $query
    $result = $MemberCommand.ExecuteReader()
    $datatable = new-object “System.Data.DataTable”
    $datatable.Load($result)

    foreach($extraTrackingTable in $datatable)
    {
        $msg= "WARNING: "+ $extraTrackingTable.TrackingTable + " exists but the corresponding user table does not exist! this maybe preventing provisioning/re-provisioning!"
        Write-Host $msg -foreground Red
        [void]$errorSummary.AppendLine($msg) 
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
    Write-Host $_.Exception.Message -ForegroundColor Red   
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
WHERE ud.[server] = '" + $Server +"' and ud.[database] = '" + $Database + "'
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

         $top = $datatable  | Group-Object -Property SyncGroupName | ForEach-Object {$_ | Select-Object -ExpandProperty Group | Select-Object -First 1}
         $shouldDump = $top | where { $_.OperationResult -like '*Failure*' }
         if($shouldDump -ne $null -and $DumpMetadataSchemasForSyncGroup -eq '') 
         {
            foreach($error in $shouldDump)
            {
                DumpMetadataSchemasForSyncGroup $error.SyncGroupName
            }
         }
    }
}
Catch
{
    Write-Host GetUIHistory exception:
    Write-Host $_.Exception.Message -ForegroundColor Red   
}    
}

function SendAnonymousUsageData{
    Try 
    {           
        #Despite computername and username will be used to calculate a hash string, this will keep you anonymous but allow us to identify multiple runs from the same user
        $StringBuilderHash = New-Object System.Text.StringBuilder
        [System.Security.Cryptography.HashAlgorithm]::Create("MD5").ComputeHash([System.Text.Encoding]::UTF8.GetBytes($env:computername+$env:username))|%{
        [Void]$StringBuilderHash.Append($_.ToString("x2"))
        }
        
        $body = New-Object PSObject `
            | Add-Member -PassThru NoteProperty name 'Microsoft.ApplicationInsights.Event' `
            | Add-Member -PassThru NoteProperty time $([System.dateTime]::UtcNow.ToString('o')) `
            | Add-Member -PassThru NoteProperty iKey "c8aa884b-5a60-4bec-b49e-702d69657409" `
            | Add-Member -PassThru NoteProperty tags (New-Object PSObject | Add-Member -PassThru NoteProperty 'ai.user.id' $StringBuilderHash.ToString()) `
            | Add-Member -PassThru NoteProperty data (New-Object PSObject `
                | Add-Member -PassThru NoteProperty baseType 'EventData' `
                | Add-Member -PassThru NoteProperty baseData (New-Object PSObject `
                    | Add-Member -PassThru NoteProperty ver 2 `
                    | Add-Member -PassThru NoteProperty name '5.beta3' `
                    | Add-Member -PassThru NoteProperty properties (New-Object PSObject `
                        | Add-Member -PassThru NoteProperty 'HealthChecksEnabled' $HealthChecksEnabled.ToString()`
                        | Add-Member -PassThru NoteProperty 'MonitoringMode' $MonitoringMode.ToString() `
                        | Add-Member -PassThru NoteProperty 'ExtendedValidationsEnabledForHub' $ExtendedValidationsEnabledForHub.ToString() `
                        | Add-Member -PassThru NoteProperty 'ExtendedValidationsEnabledForMember' $ExtendedValidationsEnabledForMember.ToString() )));
        
        $body = $body | ConvertTo-JSON -depth 5; 
        
        Invoke-WebRequest -Uri 'https://dc.services.visualstudio.com/v2/track' -Method 'POST' -UseBasicParsing -body $body > $null
    } 
    Catch { Write-Host $_ }
}

function ValidateSyncDB{
    Try 
    {
        $SyncDbConnection = New-Object System.Data.SqlClient.SQLConnection
        $SyncDbConnection.ConnectionString = [string]::Format("Server=tcp:{0},1433;Initial Catalog={1};Persist Security Info=False;User ID={2};Password={3};MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;", $SyncDbServer, $SyncDbDatabase, $SyncDbUser, $SyncDbPassword)
        
        Write-Host Connecting to SyncDB $SyncDbServer"/"$SyncDbDatabase
        Try
        {
            $SyncDbConnection.Open()
        }
        Catch
        {
            Write-Host $_.Exception.Message -ForegroundColor Red
            Test-NetConnection $SyncDbServer -Port 1433
            Break
        }
        
        $SyncDbCommand = New-Object System.Data.SQLClient.SQLCommand
        $SyncDbCommand.Connection = $SyncDbConnection
        
        $query = "select [name] from sys.schemas where name in ('dss','TaskHosting')" 
        $SyncDbCommand.CommandText = $query
        $result = $SyncDbCommand.ExecuteReader()
        $datatable = new-object “System.Data.DataTable”
        $datatable.Load($result)

        if(($datatable.Rows | Where-Object {$_.name -eq "dss"} | Measure).Count -gt 0) { Write-Host "dss schema exists" -foreground White }
        else 
        {
            $msg = "WARNING: dss schema IS MISSING!"
            Write-Host $msg -foreground Red
        }
        
        if(($datatable.Rows | Where-Object {$_.name -eq "TaskHosting"} | Measure).Count -gt 0) { Write-Host "TaskHosting schema exists" -foreground White }
        else 
        {
            $msg = "WARNING: TaskHosting schema IS MISSING!"
            Write-Host $msg -foreground Red
        }

        $query = "select schema_name(schema_id) as [name], count(*) as 'Count' from sys.tables
where schema_name(schema_id) = 'dss' or schema_name(schema_id) = 'TaskHosting' 
group by schema_name(schema_id)" 
        $SyncDbCommand.CommandText = $query
        $result = $SyncDbCommand.ExecuteReader()
        $datatable = new-object “System.Data.DataTable”
        $datatable.Load($result)

        $spCount = $datatable.Rows | Where-Object {$_.name -eq "dss"}
        if($spCount.Count -gt 0) { Write-Host "dss" $spCount.Count "tables found" -foreground White }
        else 
        {
            $msg = "WARNING: dss tables are MISSING!"
            Write-Host $msg -foreground Red
        }
        
        $spCount = $datatable.Rows | Where-Object {$_.name -eq "TaskHosting"}
        if($spCount.Count -gt 0) { Write-Host "TaskHosting" $spCount.Count "tables found" -foreground White }
        else 
        {
            $msg = "WARNING: TaskHosting tables are MISSING!"
            Write-Host $msg -foreground Red
        }

        $query = "select schema_name(schema_id) as [name], count(*) as 'Count' from sys.procedures
where schema_name(schema_id) = 'dss' or schema_name(schema_id) = 'TaskHosting' 
group by schema_name(schema_id)" 
        $SyncDbCommand.CommandText = $query
        $result = $SyncDbCommand.ExecuteReader()
        $datatable = new-object “System.Data.DataTable”
        $datatable.Load($result)

        $spCount = $datatable.Rows | Where-Object {$_.name -eq "dss"}
        if($spCount.Count -gt 0) { Write-Host "dss" $spCount.Count "stored procedures found" -foreground White }
        else 
        {
            $msg = "WARNING: dss stored procedures are MISSING!"
            Write-Host $msg -foreground Red
        }
        
        $spCount = $datatable.Rows | Where-Object {$_.name -eq "TaskHosting"}
        if($spCount.Count -gt 0) { Write-Host "TaskHosting" $spCount.Count "stored procedures found" -foreground White }
        else 
        {
            $msg = "WARNING: TaskHosting stored procedures are MISSING!"
            Write-Host $msg -foreground Red
        }

        $query = "select schema_name(schema_id) as [name], count(*) as 'Count'
from sys.types where is_user_defined = 1 and schema_name(schema_id) = 'dss' or schema_name(schema_id) = 'TaskHosting' 
group by schema_name(schema_id)" 
        $SyncDbCommand.CommandText = $query
        $result = $SyncDbCommand.ExecuteReader()
        $datatable = new-object “System.Data.DataTable”
        $datatable.Load($result)

        $spCount = $datatable.Rows | Where-Object {$_.name -eq "dss"}
        if($spCount.Count -gt 0) { Write-Host "dss" $spCount.Count "types found" -foreground White }
        else 
        {
            $msg = "WARNING: dss types are MISSING!"
            Write-Host $msg -foreground Red
        }
        
        $spCount = $datatable.Rows | Where-Object {$_.name -eq "TaskHosting"}
        if($spCount.Count -gt 0) { Write-Host "TaskHosting" $spCount.Count "types found" -foreground White }
        else 
        {
            $msg = "WARNING: TaskHosting types are MISSING!"
            Write-Host $msg -foreground Red
        }

        $query = "select schema_name(schema_id) as [name], count(*) as 'Count'
from sys.objects where type in ( 'FN', 'IF', 'TF' )
and schema_name(schema_id) = 'dss' or schema_name(schema_id) = 'TaskHosting'
group by schema_name(schema_id)" 
        $SyncDbCommand.CommandText = $query
        $result = $SyncDbCommand.ExecuteReader()
        $datatable = new-object “System.Data.DataTable”
        $datatable.Load($result)

        $spCount = $datatable.Rows | Where-Object {$_.name -eq "dss"}
        if($spCount.Count -gt 0) { Write-Host "dss" $spCount.Count "functions found" -foreground White }
        else 
        {
            $msg = "WARNING: dss functions are MISSING!"
            Write-Host $msg -foreground Red
        }
        
        $spCount = $datatable.Rows | Where-Object {$_.name -eq "TaskHosting"}
        if($spCount.Count -gt 0) { Write-Host "TaskHosting" $spCount.Count "functions found" -foreground White }
        else 
        {
            $msg = "WARNING: TaskHosting functions are MISSING!"
            Write-Host $msg -foreground Red
        }

        $query = "select name from sys.sysusers where name in ('##MS_SyncAccount##','DataSync_reader','DataSync_executor','DataSync_admin')" 
        $SyncDbCommand.CommandText = $query
        $result = $SyncDbCommand.ExecuteReader()
        $datatable = new-object “System.Data.DataTable”
        $datatable.Load($result)

        if(($datatable.Rows | Where-Object {$_.name -eq "##MS_SyncAccount##"} | Measure).Count -gt 0) { Write-Host "##MS_SyncAccount## exists" -foreground White }
        else 
        {
            $msg = "WARNING: ##MS_SyncAccount## IS MISSING!"
            Write-Host $msg -foreground Red
        }
        
        if(($datatable.Rows | Where-Object {$_.name -eq "DataSync_reader"} | Measure).Count -gt 0) { Write-Host "DataSync_reader exists" -foreground White }
        else 
        {
            $msg = "WARNING: DataSync_reader IS MISSING!"
            Write-Host $msg -foreground Red
        }
        
        if(($datatable.Rows | Where-Object {$_.name -eq "DataSync_executor"} | Measure).Count -gt 0) { Write-Host "DataSync_executor exists" -foreground White }
        else 
        {
            $msg = "WARNING: DataSync_executor IS MISSING!"
            Write-Host $msg -foreground Red
        }
        
        if(($datatable.Rows | Where-Object {$_.name -eq "DataSync_admin"} | Measure).Count -gt 0) { Write-Host "DataSync_admin exists" -foreground White }
        else 
        {
            $msg = "WARNING: DataSync_admin IS MISSING!"
            Write-Host $msg -foreground Red
        }

        $query = "select count(*) as 'Count' from sys.symmetric_keys where name like 'DataSyncEncryptionKey%'" 
        $SyncDbCommand.CommandText = $query
        $result = $SyncDbCommand.ExecuteReader()
        $datatable = new-object “System.Data.DataTable”
        $datatable.Load($result)

        $keyCount = $datatable.Rows
        if($keyCount.Count -gt 0) { Write-Host $keyCount.Count "DataSyncEncryptionKey found" -foreground White }
        else 
        {
            $msg = "WARNING: no DataSyncEncryptionKeys were found!"
            Write-Host $msg -foreground Red
        }

        $query = "select count(*) as 'Count' from sys.certificates where name like 'DataSyncEncryptionCertificate%'" 
        $SyncDbCommand.CommandText = $query
        $result = $SyncDbCommand.ExecuteReader()
        $datatable = new-object “System.Data.DataTable”
        $datatable.Load($result)

        $keyCount = $datatable.Rows
        if($keyCount.Count -gt 0) { Write-Host $keyCount.Count "DataSyncEncryptionCertificate found" -foreground White }
        else 
        {
            $msg = "WARNING: no DataSyncEncryptionCertificates were found!"
            Write-Host $msg -foreground Red
        }

        $SyncDbCommand.CommandText = "SELECT count(*) as C FROM [dss].[syncgroup]"
        $SyncDbMembersResult = $SyncDbCommand.ExecuteReader()
        $SyncDbMembersDataTableA = new-object “System.Data.DataTable”
        $SyncDbMembersDataTableA.Load($SyncDbMembersResult)
        Write-Host $SyncDbMembersDataTableA.C sync groups    

        $SyncDbCommand.CommandText = "SELECT count(*) as C FROM [dss].[syncgroupmember]"
        $SyncDbMembersResult = $SyncDbCommand.ExecuteReader()
        $SyncDbMembersDataTableB = new-object “System.Data.DataTable”
        $SyncDbMembersDataTableB.Load($SyncDbMembersResult)
        Write-Host $SyncDbMembersDataTableB.C sync group members     
        
        $SyncDbCommand.CommandText = "SELECT count(*) as C FROM [dss].[agent]"
        $SyncDbMembersResult = $SyncDbCommand.ExecuteReader()
        $SyncDbMembersDataTableC = new-object “System.Data.DataTable”
        $SyncDbMembersDataTableC.Load($SyncDbMembersResult)        
        Write-Host $SyncDbMembersDataTableC.C sync agents
    } 
    Catch { Write-Host $_.Exception.Message -ForegroundColor Red }
    Finally
    {        
        if($SyncDbConnection){
            Write-Host Closing connecting to SyncDb...
            $SyncDbConnection.Close()
        }        
    }
}

function DumpMetadataSchemasForSyncGroup([String] $syncGoupName){
    Try 
    {
        Write-Host Running DumpMetadataSchemasForSyncGroup
        $SyncDbConnection = New-Object System.Data.SqlClient.SQLConnection
        $SyncDbConnection.ConnectionString = [string]::Format("Server=tcp:{0},1433;Initial Catalog={1};Persist Security Info=False;User ID={2};Password={3};MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;", $SyncDbServer, $SyncDbDatabase, $SyncDbUser, $SyncDbPassword)
        
        Write-Host Connecting to SyncDB $SyncDbServer"/"$SyncDbDatabase
        Try
        {
            $SyncDbConnection.Open()
        }
        Catch
        {
            Write-Host $_.Exception.Message -ForegroundColor Red
            Test-NetConnection $SyncDbServer -Port 1433
            Break
        }
        
        $SyncDbCommand = New-Object System.Data.SQLClient.SQLCommand
        $SyncDbCommand.Connection = $SyncDbConnection
        
        $query = "SELECT [schema_description] FROM [dss].[syncgroup] WHERE [schema_description] IS NOT NULL AND [name] = '" + $syncGoupName + "'" 
        $SyncDbCommand.CommandText = $query
        $result = $SyncDbCommand.ExecuteReader()
        $datatable = new-object “System.Data.DataTable”
        $datatable.Load($result)
        if($datatable.Rows.Count -gt 0)
        {
            $xmlResult = $datatable.Rows[0].schema_description
            if($xmlResult){ $xmlResult | Out-File -filepath ('.\'+ $syncGoupName + '_schema_description.xml') }
        }

        $query = "SELECT [ocsschemadefinition] FROM [dss].[syncgroup] WHERE [ocsschemadefinition] IS NOT NULL AND [name] = '" + $syncGoupName + "'" 
        $SyncDbCommand.CommandText = $query
        $result = $SyncDbCommand.ExecuteReader()
        $datatable = new-object “System.Data.DataTable”
        $datatable.Load($result)
        if($datatable.Rows.Count -gt 0)
        {
            $xmlResult = $datatable.Rows[0].ocsschemadefinition
            if($xmlResult){ $xmlResult | Out-File -filepath ('.\'+ $syncGoupName + '_ocsschemadefinition.xml') }
        }


        $query = "SELECT ud.server as HubServer, ud.[database] as HubDatabase, [db_schema]
        FROM [dss].[syncgroup] as sg
        INNER JOIN [dss].[userdatabase] as ud on sg.hub_memberid = ud.id
        LEFT JOIN [dss].[syncgroupmember] as m on sg.id = m.syncgroupid
        WHERE [db_schema] IS NOT NULL AND sg.name = '" + $syncGoupName + "'" 
        $SyncDbCommand.CommandText = $query
        $result = $SyncDbCommand.ExecuteReader()
        $datatable = new-object “System.Data.DataTable”
        $datatable.Load($result)
        if($datatable.Rows.Count -gt 0)
        {
            $xmlResult = $datatable.Rows[0].db_schema
            if($xmlResult){ $xmlResult | Out-File -filepath ('.\'+ ($datatable.Rows[0].HubServer).Replace('.','_') +'_' + $datatable.Rows[0].HubDatabase +'_db_schema.xml') }
        }


        $query = "SELECT ud2.[server] as MemberServer ,ud2.[database] as MemberDatabase, [db_schema]
        FROM [dss].[syncgroup] as sg
        LEFT JOIN [dss].[syncgroupmember] as m on sg.id = m.syncgroupid
        LEFT JOIN [dss].[userdatabase] as ud2 on m.databaseid = ud2.id
        WHERE [db_schema] IS NOT NULL AND sg.name = '" + $syncGoupName + "'" 
        $SyncDbCommand.CommandText = $query
        $result = $SyncDbCommand.ExecuteReader()
        $datatable = new-object “System.Data.DataTable”
        $datatable.Load($result)
        if($datatable.Rows.Count -gt 0)
        {
            foreach($databse in $datatable.Rows)
            {
                $xmlResult = $databse.db_schema
                if($xmlResult){ $xmlResult | Out-File -filepath ('.\'+ ($databse.MemberServer).Replace('.','_') +'_' + $databse.MemberDatabase +'_db_schema.xml') }
            }            
        }        
    } 
    Catch { Write-Host $_.Exception.Message -ForegroundColor Red }
    Finally
    {        
        if($SyncDbConnection){
            Write-Host Closing connecting to SyncDb...
            $SyncDbConnection.Close()
        }        
    }
}

function GetIndexes($table){
Try
{ 
    $query = "sp_helpindex '" + $table + "'"

    $MemberCommand.CommandText = $query
    $result = $MemberCommand.ExecuteReader()
    $datatable = new-object “System.Data.DataTable”
    $datatable.Load($result)
    if($datatable.Rows.Count -gt 0)
    {
         $msg = "Indexes for " + $table +":"
         Write-Host $msg -foreground Green         
         $datatable | Format-Table -Wrap -AutoSize 
    }
}
Catch { Write-Host $_.Exception.Message -ForegroundColor Red}
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
        $allBulkTypeList = New-Object System.Collections.ArrayList

        $SyncDbConnection = New-Object System.Data.SqlClient.SQLConnection
        $SyncDbConnection.ConnectionString = [string]::Format("Server=tcp:{0},1433;Initial Catalog={1};Persist Security Info=False;User ID={2};Password={3};MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;", $SyncDbServer, $SyncDbDatabase, $SyncDbUser, $SyncDbPassword)
        
        Write-Host Connecting to SyncDB $SyncDbServer"/"$SyncDbDatabase
        Try
        {
            $SyncDbConnection.Open()
        }
        Catch
        {
            Write-Host $_.Exception.Message -ForegroundColor Red
            Test-NetConnection $SyncDbServer -Port 1433
            Break
        }
        
        $SyncDbCommand = New-Object System.Data.SQLClient.SQLCommand
        $SyncDbCommand.Connection = $SyncDbConnection

        Write-Host Getting scopes in SyncDB for this member database...
        
        $SyncDbCommand.CommandText = "SELECT m.[scopename]
        ,sg.name as SyncGroupName
        ,CAST(sg.schema_description as nvarchar(max)) as SchemaDescription
        ,m.[name] as MemberName
        ,m.[jobid] as JobId
        ,COUNT(mq.[MessageId]) as Messages
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
        GROUP BY m.[scopename],sg.name,CAST(sg.schema_description as nvarchar(max)),m.[name],m.[memberstate],m.[hubstate],m.[jobid],enum1.Name,enum2.Name,enum3.Name"
        $SyncDbMembersResult = $SyncDbCommand.ExecuteReader()
        $SyncDbMembersDataTable = new-object “System.Data.DataTable”
        $SyncDbMembersDataTable.Load($SyncDbMembersResult)
        
        Write-Host $SyncDbMembersDataTable.Rows.Count members found in this sync metadata database
        $SyncDbMembersDataTable.Rows | Sort-Object -Property scopename | Select scopename, SyncGroupName, MemberName, SyncDirection, State, HubState, JobId, Messages | Format-Table -Wrap -AutoSize
        $scopesList = $SyncDbMembersDataTable.Rows | Select -ExpandProperty scopename
        
        $shouldMonitor = $SyncDbMembersDataTable.Rows | where { `            
            $_.State.Equals('Provisioning')                     `
            -or $_.State.Equals('SyncInProgress')               `
            -or $_.State.Equals('DeProvisioning')               `
            -or $_.State.Equals('DeProvisioned')                `
            -or $_.State.Equals('Reprovisioning')               `
            -or $_.State.Equals('SyncCancelling')               `
            -or $_.HubState.Equals('Provisioning')              `
            -or $_.HubState.Equals('DeProvisioning')            `
            -or $_.HubState.Equals('DeProvisioned')             `
            -or $_.HubState.Equals('Reprovisioning')            
        }
        if($shouldMonitor -and $MonitoringMode -eq 'AUTO')
        {
            $MonitoringMode = 'ENABLED'
        }

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

        Write-Host
        GetUIHistory        
        Write-Host

        $MemberConnection = New-Object System.Data.SqlClient.SQLConnection
        $MemberConnection.ConnectionString = [string]::Format("Server={0};Initial Catalog={1};Persist Security Info=False;User ID={2};Password={3};MultipleActiveResultSets=False;Connection Timeout=30;", $Server, $Database, $MbrUser, $MbrPassword)
        
        Write-Host
        Write-Host Connecting to Hub/Member $Server"/"$Database
        Try
        {
            $MemberConnection.Open()
        }
        Catch
        {
            Write-Host $_.Exception.Message -ForegroundColor Red
            Test-NetConnection $Server -Port 1433
            Break
        }    

        $MemberCommand = New-Object System.Data.SQLClient.SQLCommand
        $MemberCommand.Connection = $MemberConnection

        Try
        {
            Write-Host
            Write-Host Database version and configuration: 
            $MemberCommand.CommandText = "SELECT compatibility_level AS [CompatLevel], collation_name AS [Collation], snapshot_isolation_state_desc AS [Snapshot], @@VERSION AS [Version] FROM sys.databases WHERE name = DB_NAME();"
            $MemberResult = $MemberCommand.ExecuteReader()
            $MemberVersion = new-object “System.Data.DataTable”
            $MemberVersion.Load($MemberResult)                         
            $MemberVersion.Rows | Format-Table -Wrap -AutoSize
            Write-Host
        }
        Catch
        {
            Write-Host $_.Exception.Message -ForegroundColor Red
        }
        
        ### Database Validations ###
        ValidateCircularReferences
        ValidateTableNames
        ValidateObjectNames
        DetectComputedColumns
        DetectProvisioningIssues

        ValidateTablesVSSyncDbSchema $SyncDbMembersDataTable
        Write-Host
        Write-Host Getting scopes in this Hub/Member database...
        
        Try
        {
            $MemberCommand.CommandText = "SELECT [sync_scope_name], [scope_local_id], [scope_config_id],[config_data],[scope_status], CAST([schema_major_version] AS varchar) + '.' + CAST([schema_minor_version] AS varchar) as [Version] FROM [DataSync].[scope_config_dss] AS sc LEFT OUTER JOIN [DataSync].[scope_info_dss] AS si ON si.scope_config_id = sc.config_id LEFT JOIN [DataSync].[schema_info_dss] ON 1=1"
            $MemberResult = $MemberCommand.ExecuteReader()
            $MemberScopes = new-object “System.Data.DataTable”
            $MemberScopes.Load($MemberResult)
             
            Write-Host $MemberScopes.Rows.Count scopes found in Hub/Member
            $MemberScopes.Rows | Select sync_scope_name, scope_config_id, scope_status, scope_local_id, Version | Sort-Object -Property sync_scope_name | Format-Table -Wrap -AutoSize
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
                [void]$runnableScript.AppendLine($deleteStatement)
                [void]$runnableScript.AppendLine("GO")
                    
                $deleteStatement = "DELETE FROM [DataSync].[scope_info_dss] WHERE [scope_local_id] = '"+ $scope.scope_local_id + "'" 
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
                ValidateTablesVSLocalSchema ($xmlcontent.SqlSyncProviderScopeConfiguration.Adapter | Select -ExpandProperty GlobalName)
                ShowRowCount ($xmlcontent.SqlSyncProviderScopeConfiguration.Adapter | Select -ExpandProperty GlobalName)

                foreach($table in $xmlcontent.SqlSyncProviderScopeConfiguration.Adapter)
                {        
                    #Tracking Tables
                    ValidateTrackingTable($table.TrackingTable)
                    
                    ##Triggers   
                    ValidateTrigger($table.InsTrig)
                    ValidateTrigger($table.UpdTrig)
                    ValidateTrigger($table.DelTrig)
                    
                    ## Procedures
                    if($table.SelChngProc){ ValidateSP($table.SelChngProc) }
                    if($table.SelRowProc){ ValidateSP($table.SelRowProc) }
                    if($table.InsProc){ ValidateSP($table.InsProc) }
                    if($table.UpdProc){ ValidateSP($table.UpdProc) }
                    if($table.DelProc){ ValidateSP($table.DelProc) }
                    if($table.InsMetaProc){ ValidateSP($table.InsMetaProc) }
                    if($table.UpdMetaProc){ ValidateSP($table.UpdMetaProc) }
                    if($table.DelMetaProc){ ValidateSP($table.DelMetaProc) }
                    if($table.BulkInsProc){ ValidateSP($table.BulkInsProc) }
                    if($table.BulkUpdProc){ ValidateSP($table.BulkUpdProc) }
                    if($table.BulkDelProc){ ValidateSP($table.BulkDelProc) }

                    ## BulkType
                    if($table.BulkTableType){ ValidateBulkType $table.BulkTableType $table.Col }

                    ## Indexes
                    GetIndexes $table.Name
                }

                #Constraints
                ValidateFKDependencies ($xmlcontent.SqlSyncProviderScopeConfiguration.Adapter | Select -ExpandProperty GlobalName)        
                }
            }
        }
        Catch
        {
            Write-Host $_.Exception.Message -ForegroundColor Red
        } 
        
        ### Detect Leftovers ###
        DetectTrackingTableLeftovers
        DetectTriggerLeftovers
        DetectProcedureLeftovers
        DetectBulkTypeLeftovers
        
        ### Validations ###
        ValidateProvisionMarker

        if($runnableScript.Length -gt 0)
        {
            $dumpScript = New-Object -TypeName 'System.Text.StringBuilder'
            [void]$dumpScript.AppendLine(" --*****************************************************************************************************************")  
            [void]$dumpScript.AppendLine(" --LEFTOVERS CLEANUP SCRIPT : START")
            [void]$dumpScript.AppendLine(" --ONLY applicable when this database is not being used by any other sync group in other regions and/or subscription")
            [void]$dumpScript.AppendLine(" --AND Data Sync Health Checker was able to access the right Sync Metadata Database")    
            [void]$dumpScript.AppendLine(" --*****************************************************************************************************************")  
            [void]$dumpScript.AppendLine($runnableScript.ToString())                                                                                                                           
            [void]$dumpScript.AppendLine(" --*****************************************************************************************************************")  
            [void]$dumpScript.AppendLine(" --LEFTOVERS CLEANUP SCRIPT : END")
            [void]$dumpScript.AppendLine(" --*****************************************************************************************************************")              
            ($dumpScript.ToString()) | Out-File -filepath ('.\' + $Server + '_' + $Database +'_leftovers.sql')
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

    Write-Host ****************************** -ForegroundColor Green
    Write-Host             MONITORING 
    Write-Host ****************************** -ForegroundColor Green
    
    $monitorUntil = (Get-Date).AddMinutes($MonitoringDurationInMinutes)
        
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
            Write-Host $_.Exception.Message -ForegroundColor Red
            Test-NetConnection $HubServer -Port 1433
            Break
        }  
    
    $MemberConnection = New-Object System.Data.SqlClient.SQLConnection
    $MemberConnection.ConnectionString = [string]::Format("Server={0};Initial Catalog={1};Persist Security Info=False;User ID={2};Password={3};MultipleActiveResultSets=False;Connection Timeout=30;", $MemberServer, $MemberDatabase, $MemberUser, $MemberPassword)
    $MemberCommand = New-Object System.Data.SQLClient.SQLCommand
    $MemberCommand.Connection = $MemberConnection
    
    Write-Host Connecting to Member $MemberServer"/"$MemberDatabase
    Try
    {
            $MemberConnection.Open()
        }
    Catch
    {
            Write-Host $_.Exception.Message -ForegroundColor Red
            Test-NetConnection $MemberServer -Port 1433
            Break
        } 
    
    $HubCommand.CommandText = "SELECT GETUTCDATE() as now"
    $result = $HubCommand.ExecuteReader()
    $datatable = new-object “System.Data.DataTable”
    $datatable.Load($result)
    $lasttime = $datatable.Rows[0].now
    
    while((Get-Date) -le $monitorUntil){
            
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
            else
            {
                Write-Host "- No new records from Hub Monitor (SPs)" -ForegroundColor Green
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
            else
            {
                Write-Host "- No new records from Member Monitor (SPs)" -ForegroundColor Green
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
            else
            {
                Write-Host "- No new records from Hub Monitor (running)" -ForegroundColor Green
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
            else
            {
                Write-Host "- No new records from Member Monitor (running)" -ForegroundColor Green
            }
            
            $lasttime = $lasttime.AddSeconds($MonitoringIntervalInSeconds)
            Write-Host "Waiting..." $MonitoringIntervalInSeconds "seconds..." -ForegroundColor Green
            Start-Sleep -s $MonitoringIntervalInSeconds
        }
    Write-Host
    Write-Host "Monitoring finished" -ForegroundColor Green
}

Try 
{

cls
$tempDir = $env:TEMP + '\DataSyncHealthChecker\' + [System.DateTime]::Now.ToString('yyyyMMddTHHmmss')
New-Item $tempDir -ItemType directory | Out-Null
Set-Location -Path $tempDir

Try 
{
    $file = '.\_SyncDB_Log.txt'
    Start-Transcript -Path $file
    
    Write-Host ************************************************************ -ForegroundColor Green
    Write-Host "        Data Sync Health Checker v5.beta2 Results"              -ForegroundColor Green
    Write-Host ************************************************************ -ForegroundColor Green
    Write-Host
    Write-Host Configuration: -ForegroundColor Green
    Write-Host PowerShell $PSVersionTable.PSVersion
    Write-Host HealthChecksEnabled $HealthChecksEnabled
    Write-Host MonitoringMode $MonitoringMode
    Write-Host MonitoringIntervalInSeconds $MonitoringIntervalInSeconds
    Write-Host SendAnonymousUsageData $SendAnonymousUsageData
    Write-Host ExtendedValidationsEnabledForHub $ExtendedValidationsEnabledForHub
    Write-Host ExtendedValidationsEnabledForMember $ExtendedValidationsEnabledForMember
    Write-Host ExtendedValidationsTableFilter $ExtendedValidationsTableFilter
    Write-Host ExtendedValidationsCommandTimeout $ExtendedValidationsCommandTimeout 
    
    if($SendAnonymousUsageData){ SendAnonymousUsageData }
    
    #SyncDB
    if($SyncDbServer -ne '' -and $SyncDbDatabase -ne '')
    {
        Write-Host
        Write-Host ***************** Validating Sync Metadata Database ********************** -ForegroundColor Green
        Write-Host 
        ValidateSyncDB
        if($DumpMetadataSchemasForSyncGroup -ne '')
        {
            DumpMetadataSchemasForSyncGroup $DumpMetadataSchemasForSyncGroup
        }   
    }
}
Finally 
{
    Try{ Stop-Transcript | Out-Null } Catch [System.InvalidOperationException]{}
    $lineNumber = (Select-String -Path $file -Pattern 'Transcript started').LineNumber
    (Get-Content $file | Select-Object -Skip $lineNumber) | Set-Content $file    
}

#Hub
$Server = $HubServer
$Database = $HubDatabase 
$MbrUser = $HubUser
$MbrPassword = $HubPassword
$ExtendedValidationsEnabled = $ExtendedValidationsEnabledForHub 
if($Server -ne '' -and $Database -ne '')
{
    Try 
    {
        $file = '.\_Hub_Log.txt'
        Start-Transcript -Path $file
        Write-Host
        Write-Host ***************** Validating Hub ********************** -ForegroundColor Green
        Write-Host 
        ValidateDSSMember
    }
    Finally 
    {
        Try{ Stop-Transcript | Out-Null } Catch [System.InvalidOperationException]{}
        $lineNumber = (Select-String -Path $file -Pattern 'Transcript started').LineNumber
        (Get-Content $file | Select-Object -Skip $lineNumber) | Set-Content $file        
    }
}

#Member
$Server = $MemberServer
$Database = $MemberDatabase 
$MbrUser = $MemberUser
$MbrPassword = $MemberPassword
$ExtendedValidationsEnabled = $ExtendedValidationsEnabledForMember
if($Server -ne '' -and $Database -ne '')
{
    Try 
    {
        $file = '.\_Member_Log.txt'
        Start-Transcript -Path $file
        Write-Host
        Write-Host ***************** Validating Member ********************** -ForegroundColor Green
        Write-Host
        ValidateDSSMember
    }
    Finally 
    {
        Try{ Stop-Transcript | Out-Null } Catch [System.InvalidOperationException]{}
        $lineNumber = (Select-String -Path $file -Pattern 'Transcript started').LineNumber
        (Get-Content $file | Select-Object -Skip $lineNumber) | Set-Content $file
    }
}

#Monitor
if($MonitoringMode -eq 'ENABLED')
{
    Try 
    {
        $file = '.\_Monitoring_Log.txt'
        Start-Transcript -Path $file
        Monitor
    }
    Finally 
    {
        Try{ Stop-Transcript | Out-Null } Catch [System.InvalidOperationException]{}
        $lineNumber = (Select-String -Path $file -Pattern 'Transcript started').LineNumber
        (Get-Content $file | Select-Object -Skip $lineNumber) | Set-Content $file
    }
}

}
Finally 
{
    ii $tempDir
}