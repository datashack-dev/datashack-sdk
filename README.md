# Datashack 



## Pre-requisites

- install node
- install python
- install terraform and cdktf
  - https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli
  - npm install --global cdktf-cli@latest

## Getting Started

run in your terminal 

```
pip install datashack
git clone .../datashack/exmples
datashack plan ./datashack-examples/py/my_app/models
```



## What datashack do

### Streaming data platform

Spark's streaming pipelines enable real-time data analysis, empowering businesses to monitor their operations, identify trends, and make informed decisions. Datashack's streaming data platform provides a seamless solution for processing, storage, and analytics, with Spark's streaming capabilities creating trailing tables while still keeping the schema and evolving it. Datashack also provides storage on S3, with tables and databases in Glue Catalog for Athena analytics, which can be integrated with BI tools. Additionally, Datashack's platform allows events to be sent to machine learning models or data warehouses.

### Datashack capabilities

1. Datashack allows you to create a fully operational data platform with just a few lines of code. The platform includes infrastructure for:
    1. Storing data in S3 as Parquet/Delta/Iceberg, with partitions and other relevant configurations.
    2. Preparing Spark streaming jobs.
    3. Managing data using the Glue catalog, including tables and databases.
    4. Leveraging big data features such as:
        1. Compaction using Delta optimize/Iceberg compact.
        2. CRUD operations using Delta optimize/Iceberg compact.
        3. Schema evolutions using Delta/Iceberg.
    5. Ensuring proper catalog governance using Lake Formation*.
    6. Saving costs by setting a retention policy on your data platform and utilizing intelligent tiering to move data to archives or cold storage.
2. You can send data directly from your code to the platform for processing, storage, and analytics. You can combine events from various organizational tools, such as Salesforce or Google Analytics, and integrate them into your data platform.
    1. Ingestion:
        1. Send events to Datashack from your organization's environment.
        2. Datashack can run ingestion from different organizational tools such as Google Analytics, Shopify, and more.
    2. Processing: This is done with streaming capabilities (Apache Spark) and creates trailing tables while still keeping the schema, and evolving the schema.
        1. Build your bronze, silver, and gold data models with Spark processing.
        2. Process your data with various transformations such as joins, aggregations, PII masking, and more.
    3. Analytics & querying:
        1. Athena: Tables and databases are instantly queryable with AWS Athena.
        2. Warehouse: You can set the data to be written to a warehouse or synced with an S3 table, supporting various warehouses such as Redshift, RDS(SQL), and DynamoDB.
3. Business Tools and Integrations:
    1. Business Intelligence (BI) tools that can integrate with AWS Athena, AWS S3, and AWS Glue Table include Amazon QuickSight, Tableau, Power BI, QlikView, and Looker. These tools provide data visualization and analysis capabilities, enabling businesses to extract insights and make informed decisions based on their data.
    2. Machine Learning (ML) tools such as QWAK and SageMaker are powerful tools for your data science needs. With Datashack, you no longer need ML Ops and data engineers for your data, as you can connect your Data Scientists directly to your data.
    3. API and Exporting tools - connect your customers directly to Datashack's powerful data exporting tool.