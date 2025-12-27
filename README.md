# Customer_360_Project
Building customer 360 profile on Microsoft Fabric using data from different sources.

## General Information
The purpose of project is to connect data from 4 different sources, into a single Customer profile. Data comes from dummyjson and Mackaroo for project purpose. 

## Data Sources
### Data comes from 4 different sources :
- **CRM API** -> It is REST type api from dummyjson/users, it works as a source-of-truth data for whole structure of project. It's ingested using notebook
- **ERP CSV** -> It is single file in CSV format that have complementary data about users, for project purpose ERP addresses are generated sequentially to preserve person-level consistency across address attributes. tax_id is a synthetic identifier generated for demo purposes and does not represent a real-world tax number. It based in Ms Fabric (lakehouse act as a sink for data)
- **Marketing API** -> It is REST type api dummyjson/posts, contain social media data having information about user posts & comments, in project purposes only tags, reactions, and views will be used for later use, rest will stay in bronze layer. It's ingested using notebook.
- **Support JSON** -> It is single file in JSON format that contains some ticket's data, in project it is treated as a some one-time event data that will not grow. Connects to CRM data using emails. It is based automatically in Ms Fabric from start

## Data ingestion / Bronze Layer
Data is ingested into fabric using data pipeline with notebook & copy data activities. 
To avoid code duplication, API ingestion is implemented as a parameterized notebook reused across multiple REST sources, orchestrated by a single Fabric pipeline.

## Data transformation / Silver Layer
Dats is cleaned from files using 1 single notebook for every file.
Data is cleaned from technical side.

## Data enrichements / Gold Layer
The Gold layer represents the business-ready, analytics-optimized data model built on top of curated Silver datasets.
Its primary goal is to provide a single, unified view of the customer (Customer 360) by resolving identities across multiple source systems and enriching customer profiles with aggregated behavioral insights.
Customer profile attributes are sourced from the system of record best suited for each domain, with CRM providing contact identity and ERP providing formal address information.

