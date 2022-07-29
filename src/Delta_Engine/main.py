from src.vessel_replication_pipeline.Database_Connector import ODBC_Connector
from src.vessel_replication_pipeline.Queries import *
import pandas  as pd
from tqdm import tqdm
import hashlib
import json
import sys
######################### Pandas Settings ################################
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
tqdm.pandas()
###########################################################################

if len(sys.argv)    > 1:
    source_backup_path     = sys.argv[1]
    source_server          = sys.argv[2]
    source_database        = sys.argv[3]
    source_username        = sys.argv[4]
    source_password        = sys.argv[5]
    source_driver          = sys.argv[6]

    target_backup_path     = sys.argv[7]
    target_server          = sys.argv[8]
    target_database        = sys.argv[9]
    target_username        = sys.argv[10]
    target_password        = sys.argv[11]
    target_driver          = sys.argv[12]
    target_database_path   = sys.argv[13]
    report_path            = sys.argv[14]
    keycolumn              = sys.argv[15]
    

else:


    read_config = open('C:\Devops_Pipeline\Vessel-Replication-Pipeline\src\Config\Config.json')
    config_data = json.load(read_config)
    print(json.dumps(config_data['Replicate_to_Remote']))


    source_path             = f"{config_data['Replicate_to_Remote']['source_path']}".replace('/','\\')
    source_server           = f"{config_data['Replicate_to_Remote']['source_server']}".replace('/','\\')
    source_database         = f"{config_data['Replicate_to_Remote']['source_database']}"
    source_username         = f"{config_data['Replicate_to_Remote']['source_username']}"
    source_password         = f"{config_data['Replicate_to_Remote']['source_password']}"
    source_driver           = f"{config_data['Replicate_to_Remote']['source_driver']}"
    source_database_path    = f"{config_data['Replicate_to_Remote']['source_database_path']}".replace('/','\\')

    target_path             = f"{config_data['Replicate_to_Remote']['target_path']}".replace('/','\\')
    target_server           = f"{config_data['Replicate_to_Remote']['target_server']}".replace('/','\\')
    target_database         = f"{config_data['Replicate_to_Remote']['target_database']}"
    target_username         = f"{config_data['Replicate_to_Remote']['target_username']}"
    target_password         = f"{config_data['Replicate_to_Remote']['target_password']}"
    target_driver           = f"{config_data['Replicate_to_Remote']['target_driver']}"
    target_database_path    = f"{config_data['Replicate_to_Remote']['target_database_path']}".replace('/','\\')

    report_path             = f"{config_data['Replicate_to_Remote']['report_path']}"
    keycolumn               = f"{config_data['Replicate_to_Remote']['keycolumn']}"

###########################################
#####     SQL_SERVER Connections      #####
###########################################
conn_local_sql_server =ODBC_Connector(backup_path=source_database_path,server=source_server,database=source_database,username=source_username,password=source_password,driver=source_driver)
remote_sql_server_shore_db =ODBC_Connector(backup_path=target_database_path,server=target_server,database=target_database,username=target_username,password=target_password,driver=target_driver)


################ Queries ###################
for i,r in remote_sql_server_shore_db.execute_sql_pandas(sql_statement=get_databases).iterrows():
    vessel_database = r['name']
    #print('Querying catalog for database : ',vessel_database )
    df_sql_shore = remote_sql_server_shore_db.execute_sql_pandas(sql_statement=meta_query_legacy.replace('<##KEYCOLUMN##>',keycolumn))
    df_sql_shore['Database_Name'] = vessel_database
    db_list.append(df_sql_shore)
meta_df = pd.concat(db_list)


################ Compare ###################
## Get data from local db and insert into remote ##
for i,r in meta_df.drop_duplicates(subset=['TABLE_NAME']).iterrows():
    table_name = r['TABLE_NAME']
    print('Getting Data from :' ,table_name)
    get_data_query = f'Select * from {target_database}.dbo.{table_name}'

    # execute query on local and remote db #
    remote_results = remote_sql_server_shore_db.execute_sql_pandas(get_data_query)
    remote_columns = [c for c in remote_results.columns]
    remote_results['md5'] = remote_results.apply(lambda x:  hashlib.md5(''.join(map(str, x.values)).encode('utf-8')).hexdigest(), axis =1 )


    local_results = conn_local_sql_server.execute_sql_pandas(get_data_query)
    local_results = local_results[[*remote_columns]]
    local_results['md5'] = local_results.apply(lambda x: hashlib.md5(''.join(map(str, x.values)).encode('utf-8')).hexdigest(), axis=1)


    # find delta records #
    if 'id' in local_results.columns:

        delta_results = local_results[~local_results['md5'].isin(remote_results['md5']) & ~local_results['id'].isin(remote_results['id'])]
    elif 'Id' in local_results.columns:

        delta_results = local_results[~local_results['md5'].isin(remote_results['md5']) & ~local_results['Id'].isin(remote_results['Id'])]
    elif 'ID' in local_results.columns:

        delta_results = local_results[~local_results['md5'].isin(remote_results['md5']) & ~local_results['ID'].isin(remote_results['ID'])]
    elif  'document_id' in local_results.columns:

        delta_results = local_results[~local_results['md5'].isin(remote_results['md5']) & ~local_results['document_id'].isin(remote_results['document_id'])]
    elif  'DocumentId' in local_results.columns:

        delta_results = local_results[~local_results['md5'].isin(remote_results['md5']) & ~local_results['DocumentId'].isin(remote_results['DocumentId'])]

    else:

        delta_results = local_results[~local_results['md5'].isin(remote_results['md5'])]

    delta_results= delta_results[[*remote_columns]]

    ####
    #delta_results.to_csv(f'{report_path}_{table_name}_Delta_Report.csv')
    try:
        print('writting data to:', table_name)
        rows_inserted = delta_results.count()[0]
        delta_results.to_sql(
        name=table_name,
        con=remote_sql_server_shore_db.sql_alchemy_connection,
        if_exists="append",
        index=False)
        print('finished inserting records to:', table_name, f'\n inserted a total of {rows_inserted}')
    except Exception as e:
        print('unable to write results to remote table, error is : ',e)
    print('#######')
"""
"""
