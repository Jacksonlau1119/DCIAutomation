DECLARE @tableName VARCHAR(100)
DECLARE @suffix VARCHAR(50) = 'PIESData72_bronze'  -- Replace with the desired suffix

DECLARE @sql NVARCHAR(MAX) = ''

DECLARE tableCursor CURSOR FOR
SELECT TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME LIKE '%' + @suffix

OPEN tableCursor

FETCH NEXT FROM tableCursor INTO @tableName

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql = @sql + 'SELECT * FROM ' + @tableName + ' WHERE upload_time = (SELECT MAX(upload_time) FROM ' + @tableName + ') UNION ALL '

    FETCH NEXT FROM tableCursor INTO @tableName
END

CLOSE tableCursor
DEALLOCATE tableCursor

-- Remove the trailing 'UNION ALL' from the generated SQL
SET @sql = LEFT(@sql, LEN(@sql) - LEN(' UNION ALL '))

-- Create a new table with the concatenated data
SET @sql = 'SELECT * INTO ' + @suffix + 'FROM (' + @sql + ') AS ConcatenatedData'

-- Execute the dynamic SQL statement
EXEC sp_executesql @sql
