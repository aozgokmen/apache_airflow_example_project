from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from google.cloud import storage
from google.cloud import bigquery
from slack_messages import throw_fail_message
from datetime import datetime
import pandas as pd
import json


########################################
#  CONNECTIONS
########################################
pg_hook = PostgresHook(postgres_conn_id='')
conn = pg_hook.get_conn()

bq_hook = BigQueryHook(gcp_conn_id='', use_legacy_sql=False)
bq_client = bigquery.Client(project=bq_hook._get_field(""), credentials=bq_hook.get_credentials())

gcs_hook = GoogleCloudStorageHook(gcp_conn_id='')
gcs_client = storage.Client(project=gcs_hook._get_field(""), credentials=gcs_hook.get_credentials())



########################################
#  PUBLIC FUNCTIONS
########################################
# CHECK IF ANY FILE EXISTS
def get_file_count():
   bucket_name = ''
   bucket = gcs_client.bucket(bucket_name)
   blobs = list(bucket.list_blobs(prefix=''))
   return len(blobs)

# Delete if bucket exists
def delete_bucket():
    bucket = gcs_client.get_bucket('')
    blobs = bucket.list_blobs(prefix='')
    for blob in blobs:
        blob.delete()
              
                
        
    
########################################
#   POSTGRES TO GOOGLE CLOUD STORAGE
########################################
def postgres_to_gcs():
    cursor=conn.cursor()
    query_job=bq_client.query("""SQL""")   
    table_last_id=[i[0] for i in query_job]
    var_table_last_id = (str(table_last_id).replace('[','')).replace(']','')    

    part_query = f"""
                    WITH RECURSIVE cte_partitions as (
                    SELECT  min(id) as min_id, min(id)+99999 as max_id, 1 as part_number 
                    from 
                    where id>{var_table_last_id}
                    UNION ALL
                    SELECT cte.min_id+100000, cte.max_id+100000, cte.part_number+1 
                    FROM cte_partitions as cte
                    WHERE cte.max_id <= (
                                            select max(id)
                                            from 
                                            where id>{var_table_last_id}
                                            )
                    ) 
                    select min_id, max_id, part_number
                    from cte_partitions;
                 """
    
    cursor.execute(part_query)
    conn.commit()
    partition_data = cursor.fetchall()
    df_part_list = pd.DataFrame(partition_data, columns=[i[0] for i in cursor.description])

    for i in range(0, len(df_part_list)):
        data_query=f"""
                    SELECT id, listing_id, url, source_url, created_by, deleted_by, 
                           to_char(created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as created_at, 
                           to_char(deleted_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as deleted_at, 
                           phash_mean, relative_path
                    FROM TABLE
                    WHERE id between {df_part_list.iloc[i].min_id} and {df_part_list.iloc[i].max_id} ;
                  """
        cursor.execute(data_query)
        data = cursor.fetchall()

        bucket = gcs_client.bucket('')
        blob = bucket.blob(f'_{df_part_list.iloc[i].part_number}.json')           
        blob.upload_from_string('\n'.join(json.dumps(dict(zip([column[0] for column in cursor.description], row))) for row in data), 'application/json')
        print(f'Part {df_part_list.iloc[i].part_number} completed!')

        conn.commit()
    cursor.close()
    conn.close()        



########################################
#   GOOGLE CLOUD STORAGE TO DWH
########################################
def gcs_to_dwh():
 if get_file_count()>0:
    schema = [
                bigquery.SchemaField("", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("", bigquery.enums.SqlTypeNames.INTEGER),  
                bigquery.SchemaField("", bigquery.enums.SqlTypeNames.TIMESTAMP),
                bigquery.SchemaField("", bigquery.enums.SqlTypeNames.TIMESTAMP),
                bigquery.SchemaField("", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("", bigquery.enums.SqlTypeNames.BOOLEAN)                       
             ]  
        
    table_ref = bq_client.dataset('').table('') # Define the table reference
    table = bigquery.Table(table_ref, schema=schema)
    
    # Load the data from a JSON file
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", create_disposition='CREATE_NEVER')
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

    uri = ""
    job = bq_client.load_table_from_uri(uri, table, job_config=job_config)  # Make an API request        
    job.result() # Wait for the job to complete
    print(f'Loaded {job.output_rows} rows into {table.path}') # Print the number of rows inserted

 else:
    print("There is no data, this step skipped")
    return True
 

 ########################################
#   EXTRACT DELETED RECORDS
########################################
def extract_deleted(dataset, table):
    cursor=conn.cursor()
    query = """
              SQL
            """
    cursor.execute(query)
    data = cursor.fetchall()
    conn.commit()    
    df_list = pd.DataFrame(data,columns=[i[0] for i in cursor.description])
    df = pd.DataFrame(df_list)

    table_id = f"{dataset}.{table}"
    
    job_config = bigquery.LoadJobConfig(
        schema=[
        bigquery.SchemaField("", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("", bigquery.enums.SqlTypeNames.TIMESTAMP)
        ],
        write_disposition="WRITE_TRUNCATE",
        create_disposition='CREATE_IF_NEEDED'
    )
    job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()




########################################
#   DELETE SOFT DELETES
########################################
def delete_soft_deletes():
    merge_query = """
           SQL 
      """
    job = bq_client.query(merge_query)
    job.result() # Wait for the job to complete
    print(f'Data deleted from dwh') # Print the number of rows deleted'



    
########################################################################################################################
#   DAG Creation
########################################################################################################################    
default_args = {'owner': 'airflow', 'depends_on_past': False, 'on_failure_callback': throw_fail_message}

with DAG(
    default_args=default_args,
    dag_id='',
    tags=['ETL', 'postgrestobq'],
    description='',
    start_date=datetime(2023, 3, 1),
    schedule_interval='10 2 * * *',
    catchup=False,
) as dag:
    
    task1 = PythonOperator(
        task_id='delete_bucket',
        python_callable=delete_bucket,
        provide_context=True)
    
    task2 = PythonOperator(
         task_id='postgres_to_gcs',
         python_callable=postgres_to_gcs,
         provide_context=True)
    
    task3 = PythonOperator(
         task_id='gcs_to_dwh',
         python_callable=gcs_to_dwh,
         provide_context=True)
    
    task4 = PythonOperator(
         task_id='extract_deleted',
         op_kwargs={''},
         python_callable=extract_deleted,
         provide_context=True)    
    
    task5 = PythonOperator(
         task_id='delete_soft_deletes',
         python_callable=delete_soft_deletes,
         provide_context=True)
      
    task1 >> task2 >> task3 >> task4 >> task5
