import os

key = "youtube_project"
iv = "youtube_encyptyo"
salt = "youtube_AesEncryption"

#AWS Access And Secret key (AWS Cloud Details)
aws_access_key = "aws_access_key"
aws_secret_key = "aws_secret_key"
bucket_name = "data-engineer-data-bucket-91"
s3_customer_datamart_directory = "customer_data_mart"
s3_sales_datamart_directory = "sales_data_mart"
s3_source_directory = "sales_data/"
s3_error_directory = "sales_data_error/"
s3_processed_directory = "sales_data_processed/"


#Database credential
# MySQL database connection properties
database_name = "database_name"
url = f"jdbc:mysql://localhost:3306/{database_name}"
properties = {
    "user": "root",
    "password": "database password",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Table name
customer_table_name = "customer"
product_staging_table = "product_staging_table"
product_table = "product"
sales_team_table = "sales_team"
store_table = "store"

#Data Mart details
customer_data_mart_table = "customers_data_mart"
sales_team_data_mart_table = "sales_team_data_mart"

# Required columns
mandatory_columns = ["customer_id","store_id","product_name","sales_date","sales_person_id","price","quantity","total_cost"]


# File Download location C:\Users\prade\Desktop\spark_data\file_from_s3
local_directory = "C:\\Users\\prade\\Desktop\\spark_data\\file_from_s3"
customer_data_mart_local_file = "C:\\Users\\prade\\Desktop\\spark_data\\customer_data_mart"
sales_team_data_mart_local_file = "C:\\Users\\prade\\Desktop\\spark_data\\sales_team_data_mart"
sales_team_data_mart_partitioned_local_file = "C:\\Users\\prade\\Desktop\\spark_data\\sales_partition_data"
error_folder_path_local = "C:\\Users\\prade\\Desktop\\spark_data\\error_files"
