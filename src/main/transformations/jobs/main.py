########### Get s3 Client ################
import os
import shutil
import sys
from typing import final

import numpy as np
import pandas as pd
from datetime import datetime
from jmespath.compat import string_type
from pyspark.sql.functions import lit, concat_ws, expr
#from pyspark.sql.connect.functions import concat_ws, lit
from pyspark.sql.types import StructType, StructField, IntegerType, DataType, FloatType, StringType

from resources.dev import config
from resources.dev.config import bucket_name
from src.main.download.aws_file_download import S3FileDownloader
from src.main.move.move_files import move_s3_to_s3
from src.main.read.aws_read import S3Reader
from src.main.read.database_read import DatabaseReader
from src.main.transformations.jobs.dimension_tables_join import dimesions_table_join
from src.main.upload.upload_to_s3 import UploadToS3
from src.main.utility.encrypt_decrypt import decrypt
from src.main.utility.logging_config import logger
from src.main.utility.my_sql_session import get_mysql_connection
from src.main.utility.s3_client_object import S3ClientProvider
from src.main.utility.spark_session import spark_session
from src.main.write.parquet_writer import ParquetWriter
from src.test.sales_data_upload_s3 import s3_directory

# Decrypt AWS credentials and get S3 client
aws_access_key = config.aws_access_key
aws_secret_key = config.aws_secret_key

s3_client_provider = S3ClientProvider(decrypt(aws_access_key), decrypt(aws_secret_key))
s3_client = s3_client_provider.get_client()
# Use s3_client for your s3 operations
response = s3_client.list_buckets()
print(response)
logger.info('List of Buckets: %s', response['Buckets'])

# Check if local directory has already a file
csv_files = [file for file in os.listdir(config.local_directory) if file.endswith('.csv')]
connection = get_mysql_connection()
cursor = connection.cursor()

total_csv_files = []
if csv_files:
    for file in csv_files:
        total_csv_files.append(file)

    statement = f"""
    SELECT DISTINCT file_name FROM 
    pradeep.product_staging_table 
    WHERE file_name IN ({str(total_csv_files)[1:-1]}) AND status = 'A'
    """

    logger.info('Dynamically created statement: %s', statement)

    # Execute the statement
    cursor.execute(statement)
    data = cursor.fetchall()
    if data:
        logger.info('Your last run was failed; please check')
    else:
        logger.info('No Record Match...')
else:
    logger.info('Last run was successful!')

try:
    s3_reader = S3Reader()
    # Folder path should come from config
    folder_path = config.s3_source_directory
    s3_absolute_file_path = s3_reader.list_files(s3_client, config.bucket_name, folder_path=folder_path)
    logger.info('Absolute path on s3 bucket for csv file: %s', s3_absolute_file_path)
    if not s3_absolute_file_path:
        logger.info('No files available at %s', folder_path)
        raise Exception('No Data available to process')
except Exception as e:
    logger.error('Exited with error: %s', e)
    raise e
############################################################################################
# Retrieve bucket and directory configuration
bucket_name = config.bucket_name
local_directory = config.local_directory
prefix = f's3://{bucket_name}/'
file_paths = [url[len(prefix):] for url in s3_absolute_file_path]
logger.info('File path available on s3 under %s bucket with folder names: %s', bucket_name, file_paths)

try:
    downloader = S3FileDownloader(s3_client, bucket_name, local_directory)
    downloader.download_files(file_paths)
except Exception as e:
    logger.error('File download error: %s', e)
    sys.exit()

# Get a list of all files in the local directory
all_files = os.listdir(local_directory)
logger.info('List of files present in local directory after download: %s', all_files)

# Filter files with ".csv" in their names and create absolute paths
csv_files = []
error_files = []
if all_files:
    for files in all_files:
        abs_path = os.path.abspath(os.path.join(local_directory, files))
        if files.endswith('.csv'):
            csv_files.append(abs_path)
        else:
            error_files.append(abs_path)
    if not csv_files:
        logger.error('No CSV data available to process the request')
        raise Exception('No CSV data available to process the request')
else:
    logger.error('There is no data to process')
    raise Exception('There is no data to process')

# Logging CSV files to be processed
logger.info('************* Listing the File **************')
logger.info('List of CSV files that need to be processed: %s', csv_files)

# Create Spark session
logger.info('************************* Creating Spark session ************************')
spark = spark_session()
logger.info('************************ Spark session created ************************')

#check the required column in the schema of csv files
#if not required columns keep it in a list or error_files
#else union all the data into on dataframe

logger.info('**************** checking Schema for data loaded in s3 **************************')
correct_files = []
for data in csv_files:
    data_schema = spark.read.format('csv')\
        .option('header','true')\
        .load(data).columns
    logger.info(f'Schema for the {data} is {data_schema}')
    logger.info(f'Mandatory columns schema is {config.mandatory_columns}')
    missing_columns = set(config.mandatory_columns) - set(data_schema)
    logger.info(f'missing columns are set {missing_columns}')

    if missing_columns:
        error_files.append(data)
    else:
        logger.info(f'No missing column for the {data}')
        correct_files.append(data)
logger.info(f'********************List of correct files ***************{correct_files}')
logger.info(f'*********************List of error files **************{error_files}')
logger.info('************ Moving Error data to error directory if any *************')

#Move the data to error directory on local
error_folder_local_path = config.error_folder_path_local
if error_files:
    for file_path in error_files:
        if os.path.exists(file_path):
            file_name = os.path.basename(file_path)
            destination_path = os.path.join(error_folder_local_path, file_name)
            shutil.move(file_path, destination_path)
            logger.info(f'Moved {file_name} from s3 file path to {destination_path}')
            source_prefix = config.s3_source_directory
            destination_prefix = config.s3_error_directory
            #message = move_s3_to_s3((s3_client,config.bucket_name,source_prefix,destination_prefix,file_name))
            message = move_s3_to_s3(s3_client, config.bucket_name, source_prefix, destination_prefix, file_name)
            logger.info(f'{message}')
        else:
            logger.info(f'{file_path} does not exits.')
else:
    logger.info('********************** There is no error files available at our dataset ********')


#Additional columns needs to be taken care of
#Determine extra columns
#Before running the process
#Stage table needs to be updated with status as Active(A or inactive(I))
logger.info(f'********* Updating the product_staging_table that we have started the process *************')
insert_statements = []
db_name = config.database_name
current_date = datetime.now()
formatted_date = current_date.strftime('%y-%m-%d %H:%M:%S')
if correct_files:
    for file in correct_files:
        filename = os.path.basename(file)
        statements = f"INSERT INTO {db_name}.{config.product_staging_table}"\
                     f"(file_name, file_location,created_date, status)" \
                     f"VALUE ('{filename}', '{filename}', '{formatted_date}', 'A') "
        insert_statements.append(statements)
    logger.info(f"Insert statement created for staging table --- {insert_statements}")
    logger.info("****************Connecting with my SQL server *************************")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info("**************** My SQL server connected successfully **********************")
    for statement in insert_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.info('**************** There is no files to process *********************')
    raise Exception('********************* No Data available with correct file *******************')


logger.info('******************* Staging table updated successfully ***********************')
logger.info('******************** Fixing extra column coming form source *******************')

schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField('product_name', StringType(),True),
    StructField('sales_date',DataType(),True),
    StructField('sales_person_id',IntegerType(),True),
    StructField('price',FloatType(),True),
    StructField('quantity',IntegerType(),True),
    StructField('total_cost',FloatType(),True),
    StructField('additional_column', StringType(),True)
    
])

#Connectiong with DatabaseReader
database_client = DatabaseReader(config.url, config.properties)
logger.info('*************** creating empty dataframe ********************')
final_df_to_process = database_client.create_dataframe(spark, 'empty_df_create_table')
#final_df_to_process = spark.createDataFrame([], schema = schema)
# Create a new column with concatenated values of extra columns
for data in correct_files:
    data_df = spark.read.format('csv') \
        .option('header', 'true') \
        .option('inferSchema', 'true') \
        .load(data)
    data_schema = data_df.columns
    extra_columns = list(set(data_schema) - set(config.mandatory_columns))
    logger.info(f'Extra columns present at source is {extra_columns}')
    if extra_columns:
        data_df = data_df.withColumn('additional_column', concat_ws(' ', *extra_columns))\
            .select("customer_id", "store_id","product_name","sales_date","sales_person_id","price","quantity","total_cost","additional_column")
        logger.info(f'processed {data} and added additional_column')
    else:
        data_df = data_df.withColumn("additional_column", lit(None))\
            .select("customer_id", "store_id","product_name","sales_date","sales_person_id","price","quantity","total_cost","additional_column")

    final_df_to_process = final_df_to_process.union(data_df)
# final_df_to_process = data_df
logger.info('************** Final Dataframe from source which will be going to processing *************')
final_df_to_process.show()

#Enrich the data from all dimension table
#also create a datamart for sales_team and their incentive, address and all
#another datamart for customer who bought how much each day of month
#there should b a store_id segregation
#read the data from parquet and generate a csv file
#sales_persion_total_billing_done_for_each_month, total_incentive


#connceting with DatabaseReader
database_client = DatabaseReader(config.url, config.properties)
#crating df for all tables
#customer table
logger.info("*********************** Loading customer table into customer_table_df ****************")
customer_table_df = database_client.create_dataframe(spark,config.customer_table_name)
#proecut table
logger.info("********************** Loading product table into product_table_df *********************")
product_table_df = database_client.create_dataframe(spark, config.product_table)

#product_staging_table table
logger.info("******************* Loging staging table into product_stating_table *****************")
product_staging_table_df = database_client.create_dataframe(spark, config.product_staging_table)

#sales_team table
sales_team_table_df = database_client.create_dataframe(spark, config.sales_team_table)

#store table
logger.info("*************** Loading store table into store_table_df ******************")
store_table_df = database_client.create_dataframe(spark, config.store_table)

s3_customer_store_sales_df_join = dimesions_table_join(final_df_to_process,customer_table_df,store_table_df,sales_team_table_df)

#Final enriched data
logger.info("********************** Final Enriched Data **********************")
s3_customer_store_sales_df_join.show()


#Write the customer data into customer data mart in parquet format
#file will be written to local first
#move the raw data to s3 bucket for reporting tool
#write reporting data into mysql table also
logger.info("*********************** write the data into customer data mart ****************")
final_customer_data_mart_df = s3_customer_store_sales_df_join\
    .select("ct.customer_id",
            "ct.first_name","ct.last_name","ct.address",
            "ct.pincode","phone_number","sales_date","total_cost")

logger.info("****************** Final Data for customer Data Mart *****************************")

final_customer_data_mart_df.show()
parquet_writer = ParquetWriter("overwrite","parquet")
parquet_writer.dataframe_writer(final_customer_data_mart_df, config.customer_data_mart_local_file)

logger.info(f"******** Customer data written to local disk at {config.customer_data_mart_local_file} *********")

#Move data on s3 bucket for customer_data_mart
logger.info(f"**************** Data Movement form local to s3 for customer data mart ****************")
s3_uploader = UploadToS3(s3_client)
s3_directory= config.s3_customer_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory, config.bucket_name, config.customer_data_mart_local_file)
logger.info(f"{message}")

#sales_team Data Mart
logger.info("*************** write the data into sales team Data Mart **************")
final_sales_team_data_mart_df = s3_customer_store_sales_df_join.select("store_id","sales_person_id","sales_person_first_name","sales_person_last_name","store_manager_name","manager_id","is_manager","sales_person_address","sales_person_pincode","sales_date","total_cost",expr("SUBSTRING(sales_date,1,7) as sales_month"))

logger.info("************** Final Data for sales team Data Mart *****************************")
final_sales_team_data_mart_df.show()
############# This code is perfect #######################################
##########################################################################################################################################
parquet_writer.dataframe_writer(final_sales_team_data_mart_df, config.sales_team_data_mart_local_file)

logger.info(f"************************ sales team data written to local disk at {config.sales_team_data_mart_local_file}")


#Move data on s3 bucket for sales_data_mart
s3_directory = config.s3_sales_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory, config.bucket_name, config.sales_team_data_mart_local_file)
logger.info(f"{message}")

#Also writing the data into partitions
# final_sales_team_data_mart_df.write.format("parquet")\
#     .option("header","true")\
#     .mode("overwrite")\
#     .partitionBy("sales_month","store_id")\
#     .option("path",config.sales_team_data_mart_partitioned_local_file)\
#     .save()


















