# Customer_360_Project
Building customer 360 profile on Microsoft Fabric using data from different sources.

## General Information
The purpose of project is to connect data from 4 different sources, into a single Customer profile 

## Data Sources
Data comes from 4 different sources :
CRM API -> It is REST type api from dummyjson/users, it works as a source-of-truth data for whole structure of project.
ERP CSV -> It is single file in CSV format that have complementary data about users, for project purpose ERP addresses are generated sequentially to preserve person-level consistency across address attributes. tax_id is a synthetic identifier generated for demo purposes and does not represent a real-world tax number.
Marketing API -> It is social media data having information about user posts & comments, in project purposes only tags, reactions, and views will be used for later use, rest will stay in bronze layer
Support JSON -> It is single file in JSON format that contains some ticket's data, in project it is treated as a some one-time event data that will not grow. Connects to CRM data using emails.

## Data ingestion / Bronze Layer
Data is ingested into fabric using data pipeline with notebook & copy data activities.
To avoid code duplication, API ingestion is implemented as a parameterized notebook reused across multiple REST sources, orchestrated by a single Fabric pipeline.
