################ Queries ###################

grant_permissions = """
                    Use master
                    DECLARE @dbname VARCHAR(50)   
                    DECLARE @statement NVARCHAR(max)

                    DECLARE db_cursor CURSOR 
                    LOCAL FAST_FORWARD
                    FOR  
                    SELECT name
                    FROM MASTER.dbo.sysdatabases
                    WHERE name NOT IN ('master','model','msdb','tempdb','distribution')  
                    OPEN db_cursor  
                    FETCH NEXT FROM db_cursor INTO @dbname  
                    WHILE @@FETCH_STATUS = 0  
                    BEGIN  

                    SELECT @statement = 'use '+@dbname +';'+' EXEC sp_addrolemember N''db_owner'', 
                    automation_user;EXEC sp_addrolemember N''db_owner'', automation_user'

                    exec sp_executesql @statement

                    FETCH NEXT FROM db_cursor INTO @dbname  
                    END  
                    CLOSE db_cursor  
                    DEALLOCATE db_cursor
                    ;
                    """




db_list = []
get_databases = """
SELECT name
FROM MASTER.dbo.sysdatabases
WHERE name NOT IN ('master','model','msdb','tempdb','distribution','DWQueue','DWConfiguration','DWDiagnostics')  
"""

meta_query = """
    With primary_keys as
    (
    Select TOP 10000000 
     KU.table_name as TABLENAME
    ,column_name as PRIMARYKEYCOLUMN
    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC 
    INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
    ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' 
    AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME 
    ORDER BY 
         KU.TABLE_NAME
        ,KU.ORDINAL_POSITION
 
    
    ),
    
    
    
    filters as 
    (
    Select TOP 10000000
    case  
    	when is_nullable = 'NO' and (DATA_TYPE = 'datetime' or DATA_TYPE = 'timestamp') then 1 else 0
    end
    has_timestamp,
	case  
    	when trim(upper(COLUMN_NAME)) = '<##KEYCOLUMN##>' then 1 
    end
    has_id,
    null
    as filter_statement,

    Null
    as initial_load_filter,
    * 
    from [INFORMATION_SCHEMA].[COLUMNS]
    --where is_nullable = 'NO'
    order by Column_Name Asc
    ),

    --
    filter_conditions as (
    SELECT 

    TABLE_CATALOG CAT,
    		TABLE_SCHEMA  SCH,
    		TABLE_NAME    TBL,
    		case when sum(has_timestamp)>=1 then 1 else 0 end has_timestamp,
			case when sum(has_id)>=1 then 1 else 0 end has_id,
    		max(COLUMN_NAME) as COL,
      'None'
      AS Filter_Statements,
     'None'
      as initial_load_filter,
      '' as start_date
      FROM filters

     GROUP BY	TABLE_CATALOG,
    			TABLE_SCHEMA,
    			TABLE_NAME
    ),

    subject_areas as 
    (
    Select
    TABLE_CATALOG									CAT,
    TABLE_SCHEMA									SCH,
    TABLE_NAME										TBL,
    'NS_VALIDATION'									as subject_area
    from   [INFORMATION_SCHEMA].[COLUMNS]
    group by TABLE_CATALOG,									
    TABLE_SCHEMA	,								
    TABLE_NAME
    ),


    main as (
    Select TABLE_CATALOG																				as DATABASE_NAME,
    TABLE_SCHEMA																						as TABLE_SCHEMA,
    TABLE_NAME																							as TABLE_NAME,
    COLUMN_NAME																							as 'source.name',
    replace(upper(replace([COLUMN_NAME],'value','raw_value')),' ','_')									as 'sink.name',
    case 
    	when PATINDEX('%[^_0-9A-Za-z]%',COLUMN_NAME)>1 then 'True' 
    end																									as Uncompatible_Column,
    sub.subject_area																					as TARGET_SUBJECT_AREA,
     fltr.Filter_Statements																				as filter_statement,
    '{"DATABASE_NAME":"'+TABLE_CATALOG+'",'+'"TABLE_SCHMEA":"'+TABLE_SCHEMA+'",'+'"TABLE_NAME":"'+TABLE_NAME+'",'+'"TARGET_SUBJECT_AREA":"'+sub.subject_area+'",'+'"FILTER_STATEMENT":"'+
    ''
    +'"}'																								as JSON_OBJECT,
    CURRENT_TIMESTAMP																					as CREATED_ON,
    CAST( GETDATE() AS Date )																			as EFFECTIVE_DATE,
    CURRENT_TIMESTAMP LAST_MODIFIED,
    case when upper(TABLE_NAME) like '%NOT IN USE%' then 0
    when upper(TABLE_NAME) like '%ADF_CONTROL%' then 0
    else 1
    end																									as ACTIVE,
    ''																									as COMMENT
    from  [INFORMATION_SCHEMA].[COLUMNS]													            as Sch
    left outer join filter_conditions																	as fltr 
    	on Sch.TABLE_CATALOG	= fltr.CAT
    	and Sch.TABLE_SCHEMA	= fltr.SCH
    	and Sch.TABLE_NAME		= fltr.TBL
    	and Sch.COLUMN_NAME		= fltr.COL
    left join subject_areas																		        as sub 
    	on Sch.TABLE_CATALOG	= sub.CAT
    	and Sch.TABLE_SCHEMA	= sub.SCH
    	and Sch.TABLE_NAME		= sub.TBL					
    ),


    mapping as (
    Select DATABASE_NAME, 
    table_schema, 
    table_name,
    Target_Subject_Area,
    '{"type": "TabularTranslator", "mappings":['+STRING_AGG(cast('{"source":{"name":"'+[source.name]+'"},"sink":{"name":"'+[sink.name]+'"}}' as NVARCHAR(MAX)),',')+']}'
     column_mapping,
     sum(1) column_counts
    from main 
    group by DATABASE_NAME, TAble_schema, table_name,Target_Subject_Area
    ),

    row_counts as 
    (
    Select a.*,
    column_mapping,
    column_counts
    from 
    (
    SELECT
    SCHEMA_NAME(A.schema_id) as TABLE_SCHEMA ,
    A.Name AS TABLE_NAME

    , SUM(B.rows) AS RecordCount  
    FROM sys.objects A  
    INNER JOIN sys.partitions B ON A.object_id = B.object_id

    WHERE A.type = 'U'  
    GROUP BY A.schema_id, A.Name 
    )a 
    inner join mapping on a.TABLE_NAME = mapping.TABLE_NAME

    ),
    table_creation as 
    (
    SELECT
         name as tc_table_name, object_id, create_date as tc_create_date, modify_date as tc_modify_date
    FROM
         sys.tables
    )

    Select 
    tc.tc_create_date,
    tc.tc_modify_date,
    rc.*,
    format(rc.RecordCount,'N0','en-us')num_row_count,
    fc.has_timestamp as enforced_timestamp,
	fc.has_id		 as has_id,
    CAST(getdate() AS date)								as current_dt,
    fc.start_date,
    case 
    when rc.RecordCount <= 1000000 then 1
    else 1 end as daily_partitions,
    case 
    when rc.RecordCount <= 1000000 then Null
    else fc.Filter_Statements							
    end													as dynamic_filter,
    case when rc.RecordCount <= 1000000 then Null
    else  fc.initial_load_filter 
    end													as initial_load_filter
    from row_counts rc
    left join filter_conditions fc on
    		 fc.SCH = rc.TABLE_SCHEMA
    		and fc.TBL = rc.TABLE_NAME
    left join table_creation tc on tc.tc_table_name  = rc.TABLE_NAME
    where 
    -- fc.has_id = 1 and 
    upper(TABLE_NAME) not like '%TRACKING%'
    and upper(TABLE_NAME) not like '%LOG%'
    and TABLE_NAME <> 'Terminal'
    and TABLE_NAME <> 'Terminal_Source'
    and TABLE_NAME <> 'scope_info'
    and TABLE_NAME <> 'Group'
    and TABLE_NAME <> 'UserProfile'
    and TABLE_NAME <> 'webpages_Roles'
    and TABLE_NAME <> 'UserToken'
    and TABLE_NAME <> 'Vessel'
    and TABLE_NAME <> 'CompatibleRecord_Source'
    and has_id = 1 
    order by  rc.RecordCount desc
    """

meta_query_legacy = """
    With primary_keys as
    (
    Select TOP 10000000 
     KU.table_name as TABLENAME
    ,column_name as PRIMARYKEYCOLUMN
    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC 
    INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
    ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' 
    AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME 
    ORDER BY 
         KU.TABLE_NAME
        ,KU.ORDINAL_POSITION


    ),



    filters as 
    (
    Select TOP 10000000
    case  
    	when is_nullable = 'NO' and (DATA_TYPE = 'datetime' or DATA_TYPE = 'timestamp') then 1 else 0
    end
    has_timestamp,
	case  
    	when (upper(COLUMN_NAME)) = 'DOCUMENTID' then 1 
    end
    has_id,
    null as filter_statement,

    Null
    as initial_load_filter,
    * 
    from [INFORMATION_SCHEMA].[COLUMNS]
    --where is_nullable = 'NO'
    order by Column_Name Asc
    ),

    --
    filter_conditions as (
    SELECT 

    TABLE_CATALOG CAT,
    		TABLE_SCHEMA  SCH,
    		TABLE_NAME    TBL,
    		case when sum(has_timestamp)>=1 then 1 else 0 end has_timestamp,
			case when sum(has_id)>=1 then 1 else 0 end has_id,
    		max(COLUMN_NAME) as COL,
      'None'
      AS Filter_Statements,
     'None'
      as initial_load_filter,
      '' as start_date
      FROM filters

     GROUP BY	TABLE_CATALOG,
    			TABLE_SCHEMA,
    			TABLE_NAME
    ),

    subject_areas as 
    (
    Select
    TABLE_CATALOG									CAT,
    TABLE_SCHEMA									SCH,
    TABLE_NAME										TBL,
    'NS_VALIDATION'									as subject_area
    from   [INFORMATION_SCHEMA].[COLUMNS]
    group by TABLE_CATALOG,									
    TABLE_SCHEMA	,								
    TABLE_NAME
    ),


    main as (
    Select TABLE_CATALOG																				as DATABASE_NAME,
    TABLE_SCHEMA																						as TABLE_SCHEMA,
    TABLE_NAME																							as TABLE_NAME,
    COLUMN_NAME																							as 'source.name',
    replace(upper(replace([COLUMN_NAME],'value','raw_value')),' ','_')									as 'sink.name',
    case 
    	when PATINDEX('%[^_0-9A-Za-z]%',COLUMN_NAME)>1 then 'True' 
    end																									as Uncompatible_Column,
    sub.subject_area																					as TARGET_SUBJECT_AREA,
     fltr.Filter_Statements																				as filter_statement,
    '{"DATABASE_NAME":"'+TABLE_CATALOG+'",'+'"TABLE_SCHMEA":"'+TABLE_SCHEMA+'",'+'"TABLE_NAME":"'+TABLE_NAME+'",'+'"TARGET_SUBJECT_AREA":"'+sub.subject_area+'",'+'"FILTER_STATEMENT":"'+
    ''
    +'"}'																								as JSON_OBJECT,
    CURRENT_TIMESTAMP																					as CREATED_ON,
    CAST( GETDATE() AS Date )																			as EFFECTIVE_DATE,
    CURRENT_TIMESTAMP LAST_MODIFIED,
    case when upper(TABLE_NAME) like '%NOT IN USE%' then 0
    when upper(TABLE_NAME) like '%ADF_CONTROL%' then 0
    else 1
    end																									as ACTIVE,
    ''																									as COMMENT
    from  [INFORMATION_SCHEMA].[COLUMNS]													            as Sch
    left outer join filter_conditions																	as fltr 
    	on Sch.TABLE_CATALOG	= fltr.CAT
    	and Sch.TABLE_SCHEMA	= fltr.SCH
    	and Sch.TABLE_NAME		= fltr.TBL
    	and Sch.COLUMN_NAME		= fltr.COL
    left join subject_areas																		        as sub 
    	on Sch.TABLE_CATALOG	= sub.CAT
    	and Sch.TABLE_SCHEMA	= sub.SCH
    	and Sch.TABLE_NAME		= sub.TBL					
    ),


    mapping as (
    Select DATABASE_NAME, 
    table_schema, 
    table_name,
    Target_Subject_Area,
    '{"type": "TabularTranslator", "mappings":['+max(cast('{"source":{"name":"'+[source.name]+'"},"sink":{"name":"'+[sink.name]+'"}}' as NVARCHAR(MAX)))+']}'
     column_mapping,
     sum(1) column_counts
    from main 
    group by DATABASE_NAME, TAble_schema, table_name,Target_Subject_Area
    ),

    row_counts as 
    (
    Select a.*,
    column_mapping,
    column_counts
    from 
    (
    SELECT
    SCHEMA_NAME(A.schema_id) as TABLE_SCHEMA ,
    A.Name AS TABLE_NAME

    , SUM(B.rows) AS RecordCount  
    FROM sys.objects A  
    INNER JOIN sys.partitions B ON A.object_id = B.object_id

    WHERE A.type = 'U'  
    GROUP BY A.schema_id, A.Name 
    )a 
    inner join mapping on a.TABLE_NAME = mapping.TABLE_NAME

    ),
    table_creation as 
    (
    SELECT
         name as tc_table_name, object_id, create_date as tc_create_date, modify_date as tc_modify_date
    FROM
         sys.tables
    )

    Select 
    tc.tc_create_date,
    tc.tc_modify_date,
    rc.*,
    format(rc.RecordCount,'N0','en-us')num_row_count,
    fc.has_timestamp as enforced_timestamp,
	fc.has_id		 as has_id,
    CAST(getdate() AS date)								as current_dt,
    fc.start_date,
    case 
    when rc.RecordCount <= 1000000 then 1
    else 1 end as daily_partitions,
    case 
    when rc.RecordCount <= 1000000 then Null
    else fc.Filter_Statements							
    end													as dynamic_filter,
    case when rc.RecordCount <= 1000000 then Null
    else  fc.initial_load_filter 
    end													as initial_load_filter
    from row_counts rc
    left join filter_conditions fc on
    		 fc.SCH = rc.TABLE_SCHEMA
    		and fc.TBL = rc.TABLE_NAME
    left join table_creation tc on tc.tc_table_name  = rc.TABLE_NAME
    where 
    -- fc.has_id = 1 and 
    upper(TABLE_NAME) not like '%TRACKING%'
    and upper(TABLE_NAME) not like '%ROLE%'
    and upper(TABLE_NAME) not like '%LOG%'
    and TABLE_NAME <> 'Terminal'
    and TABLE_NAME <> 'Terminal_Source'
    and TABLE_NAME <> 'scope_info'
    and TABLE_NAME <> 'Group'
    and TABLE_NAME <> 'UserProfile'
    and TABLE_NAME <> 'webpages_Roles'
    and TABLE_NAME <> 'UserToken'
    and TABLE_NAME <> 'Vessel'
    and TABLE_NAME <> 'CompatibleRecord_Source'
    and TABLE_NAME <> 'schema_info'
    order by  rc.RecordCount desc
    """