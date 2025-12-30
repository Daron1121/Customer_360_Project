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

from pyspark.sql.functions import col, split, coalesce, trim, lower, sum, count, when, lit


crm = spark.table("silver.crm_cust")
erp = spark.table("silver.erp_cust")
marketing = spark.table("silver.marketing_cust")
support = spark.table("silver.support_cust")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Customer_info

# CELL ********************

erp_prepared = erp.select(
    col("Id").alias("id"),
    split(col("Full_Name"), " ").getItem(0).alias("erp_first_name"),
    split(col("Full_Name"), " ").getItem(1).alias("erp_last_name"),
    col("Street").alias("erp_street"),
    col("City").alias("erp_city"),
    col("Tax_id").alias("tax_id")
)

crm_cust = crm.alias("crm")
erp_cust = erp_prepared.alias("erp")

customers = crm_cust.join(erp_cust, crm_cust["user_id"] == erp_cust["id"], "left")

customer_info = customers.select(
    col("crm.user_id").alias("user_id"),
    lower(trim(col("crm.email"))).alias("email"),
    lower(trim(coalesce(col("erp.erp_first_name"), col("crm.first_name")))).alias("first_name"),
    lower(trim(coalesce(col("erp.erp_last_name"), col("crm.last_name")))).alias("last_name"),
    col("crm.birth_date").alias("birth_date"),
    trim(col("crm.gender")).alias("gender"),
    lower(trim(coalesce(col("erp.erp_street"), col("crm.street")))).alias("street"),
    lower(trim(coalesce(col("erp.erp_city"), col("crm.city")))).alias("city"),
    lower(trim(col("crm.country"))).alias("country")
)

customer_info.write.format("delta").mode("overwrite").saveAsTable("gold.customer_info")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Marketing aggregations

# CELL ********************

marketing_gold = marketing.groupBy("user_id").agg(
    count("*").alias("total_posts"),
    sum("views").alias("total_views"),
    sum("likes").alias("total_likes"),
    sum("dislikes").alias("total_dislikes")
)
marketing_gold.write.format("delta").mode("overwrite").saveAsTable("gold.customer_marketing_metrics")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Support Aggregation

# CELL ********************

support_gold = support.groupBy("email").agg(
    count("*").alias("ticket_count"),
    sum(when(col("status") == "open", 1).otherwise(0)).alias("open_tickets"),
    sum(when(col("priority") == "high", 1).otherwise(0)).alias("high_priority_tickets")
)
support_gold.write.format("delta").mode("overwrite").saveAsTable("gold.customer_ticket_metrics")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
