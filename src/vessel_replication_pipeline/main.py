from Database_Connector import ODBC_Connector
from Queries import *
import pandas  as pd
from tqdm import tqdm
import json
import sys
######################### Pandas Settings ################################
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
tqdm.pandas()
###########################################################################

if len(sys.argv)    > 1:
    backup_path     = sys.argv[1]
    server          = sys.argv[2]
    database        = sys.argv[3]
    username        = sys.argv[4]
    password        = sys.argv[5]
    driver          = sys.argv[6]
    database_path   = sys.argv[7]
    keycolumn       = sys.argv[8]
else:


    read_config = open('C:\Devops_Pipeline\Vessel-Replication-Pipeline\src\Config\Config.json')
    config_data = json.load(read_config)
    #print(json.dumps(config_data['Replicate_to_Remote']))
    backup_path         = f"{config_data['Sync_to_Local']['backup_path']}".replace('/', '\\')
    server              = f"{config_data['Sync_to_Local']['server']}".replace('/', '\\')
    database            = f"{config_data['Sync_to_Local']['database']}"
    username            = f"{config_data['Sync_to_Local']['username']}"
    password            = f"{config_data['Sync_to_Local']['password']}"
    driver              = f"{config_data['Sync_to_Local']['driver']}"
    database_path       = f"{config_data['Sync_to_Local']['database_path']}"
    keycolumn           = f"{config_data['Sync_to_Local']['keycolumn']}"


print(ODBC_Connector(backup_path=backup_path,server=server,database=database,username=username,password=password,driver=driver).validate_parms)
df_sql_1 = ODBC_Connector(backup_path=backup_path,server=server,database=database,username=username,password=password,driver=driver).execute_sql_pandas(sql_statement=get_databases)

for i,r in df_sql_1.iterrows():
    vessel_database = r['name']
    print('Querying catalog for database : ',vessel_database )
    df_sql_vessel = ODBC_Connector(backup_path=backup_path,server=server,database=vessel_database,username=username,password=password,driver=driver).execute_sql_pandas(sql_statement=meta_query.replace('<##KEYCOLUMN##>',keycolumn))
    df_sql_vessel['Database_Name'] = vessel_database
    db_list.append(df_sql_vessel)
final_df = pd.concat(db_list)
final_df['delta_query'] = final_df.progress_apply(lambda x:
    f"""
    select 
    count(1) missing_records from {x['Database_Name']}.dbo.{x['TABLE_NAME']}
    where {keycolumn} not in (Select {keycolumn} from {database}.dbo.{x['TABLE_NAME']})
     ;
    """, axis=1)
final_df['insert_query'] = final_df.progress_apply(lambda x:
    f"""

    INSERT INTO {database}.dbo.{x['TABLE_NAME']}
    Select * from {x['Database_Name']}.dbo.{x['TABLE_NAME']}
    where {keycolumn} not in (Select {keycolumn} from {database}.dbo.{x['TABLE_NAME']})

    ;
    """, axis=1)

final_df['missing_records'] = final_df.progress_apply(lambda x: ODBC_Connector(backup_path=backup_path,server=server,database=x["Database_Name"],username=username,password=password,driver=driver).execute_sql_pandas(x['delta_query'])['missing_records'].to_string(index=False),axis =1)
insert_missing_records      = final_df.progress_apply(lambda x: ODBC_Connector(backup_path=backup_path,server=server,database=x["Database_Name"],username=username,password=password,driver=driver).execute_sql_cursor(x['insert_query']),axis =1)
final_df['Database_Name']   = final_df['Database_Name'].replace("Teekay", "2_Teekay").replace("Teekay_Web", "1_Teekay_Web")
report_df                   = pd.pivot_table(final_df,values='missing_records',index=['TABLE_NAME'],columns='Database_Name',aggfunc={'missing_records': sum})
print(report_df)
report_df.to_csv(f'C:\Devops_Pipeline\Vessel-Replication-Pipeline\src\Reports\Shore_{database}_vs_Vessel_Delta_Pivot_Report_{keycolumn}.csv')
