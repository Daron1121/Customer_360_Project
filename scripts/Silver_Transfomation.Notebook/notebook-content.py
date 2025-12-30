# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5e79b1d8-2d53-4cd0-bf3d-d8c1436fb62d",
# META       "default_lakehouse_name": "lh_cust_360_ds",
# META       "default_lakehouse_workspace_id": "75fb39ee-6fa7-4fbb-863d-67b3d2e6bc1c",
# META       "known_lakehouses": [
# META         {
# META           "id": "5e79b1d8-2d53-4cd0-bf3d-d8c1436fb62d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import col, explode, lower, trim

sources = [
    {
        'path': 'Files/bronze/CRM_API_Data.json',
        'destination': 'silver.CRM_Cust',
        'format_file': 'json',
        'type_of_file': 'CRM'
    },
    {
        'path': 'Files/bronze/Marketing_API_Data.json',
        'destination': 'silver.Marketing_Cust',
        'format_file': 'json',
        'type_of_file': 'Marketing'
    },
    {
        'path': 'Files/bronze/ERP_CSV_Data.csv',
        'destination': 'silver.ERP_Cust',
        'format_file': 'csv',
        'type_of_file': 'ERP'
    },
    {
        'path': 'Files/bronze/Support_JSON_ticket_data.json',
        'destination': 'silver.Support_Cust',
        'format_file': 'json',
        'type_of_file': 'Support_JSON'
    }
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def CRM_Transformation(df):
    df_users = df.select(explode("users").alias("user"))

    df_clean = df_users.select(
        col('user.id').alias('User_Id'),
        lower(trim(col('user.firstName'))).alias('First_Name'),
        lower(trim(col('user.lastName'))).alias('Last_Name'),
        lower(trim(col('user.email'))).alias('Email'),
        col('user.birthDate').alias('Birth_Date'),
        lower(trim(col('user.gender'))).alias('Gender'),
        lower(trim(col('user.phone'))).alias('Phone_Number'),
        col('user.address.address').alias("Street"),
        col('user.address.city').alias("City"),
        col('user.address.postalCode').alias("Postal_Code"),
        col('user.address.country').alias("Country")
    )
    return df_clean

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def Marketing_Transformation(df):
    df_post = df.select(explode("posts").alias("post"))
    df_clean = df_post.select(
        col('post.id').alias('Post_Id'),
        col('post.userId').alias('User_Id'),
        col('post.views').alias('Views'),
        col('post.reactions.likes').alias('Likes'),
        col('post.reactions.dislikes').alias('Dislikes') 
    )
    return df_clean

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def ERP_Transformation(df):
    df_clean = df.select(
        col('cust_number').alias('Id'),
        col('full_name').alias('Full_Name'),
        col('street').alias('Street'),
        col('city').alias('City'),
        col('postal_code').alias('Postal_Code'),
        col('lat_lng').alias('Lat_Lng'),
        col('tax_id').alias('Tax_id')
    )
    return df_clean

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def Support_JSON_Transformation(df):
    df_clean = df.select(
        col('ticket_id').alias('Ticket_Id'),
        col('email').alias('Email'),
        col('category').alias('Category'),
        col('status').alias('Status'),
        col('created_at').alias('Created_At'),
        col('priority').alias('Priority')
    )
    return df_clean


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

TRANSFORMATIONS = {
    "CRM": CRM_Transformation,
    "Marketing": Marketing_Transformation,
    "ERP": ERP_Transformation,
    "Support_JSON": Support_JSON_Transformation
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read_bronze(source):
    reader = spark.read.format(source['format_file'])
    if source['format_file'] == 'csv':
        reader = reader.option('header', 'true')
    return reader.load(source['path'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for src in sources:
    df = read_bronze(src)
    transform_fn  = TRANSFORMATIONS[src["type_of_file"]]
    df_clean = transform_fn(df)
    df_clean.write.mode('overwrite').format('delta').saveAsTable(src['destination'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
