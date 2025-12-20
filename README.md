# Customer_360_Project
Building customer 360 profile on Microsoft Fabric using data from different sources.

The purpose of project is to connect data from 4 different sources, into a single Customer profile 

### Data Sources
Data comes from 4 different sources :
CRM API -> It is REST type api from dummyjson/users, it works as a source-of-truth data for whole structure of project.
ERP CSV -> It is single file that have complementary data about users, for project purpose ERP addresses are generated sequentially to preserve person-level consistency across address attributes. tax_id is a synthetic identifier generated for demo purposes and does not represent a real-world tax number.
Marketing API ->
Support JSON ->
