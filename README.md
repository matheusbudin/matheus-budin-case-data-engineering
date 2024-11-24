# matheus-budin-case-data-engineering
-----
# Data Engineering Brewery Data Pipeline

## 1. Case Objective and Project Overview
- This case requested an end-to-end solution that covers fetching data from an API: <https://www.openbrewerydb.org/>. The goal is to persist and transform the data based on the **Medallion Architecture**. 

  - **Landing Zone**: Contains raw data fetched from the API in JSON format.  
  - **Bronze Layer**: Stores the data without transformations but in a more performant format called `delta`.  
  - **Silver Layer**: Includes data transformations and quality checks, such as deduplication and postal code format verification.  
  - **Gold Layer**: Aggregates the data for analysis of KPIs, helping business units make informed decisions.

## 2. Architecture
The following architecture, based on Azure Cloud and the Databricks platform, was created:

![Architecture](https://github.com/matheusbudin/matheus-budin-case-data-engineering/blob/main/images/brewery-data-architecture.png)

### 2.1 Tech Stack used:
- Python **requests** library to fetch data from the API using databricks notebooks, it gives us more flexibility (like selecting how many itens we want to fetch) and it is easier to maintain by looking directly into the code.

- Microsoft Azure Cloud: all the resources were used inside azure cloud: resource group, active directory, key vault, data lake gen2, data factory, databricks platform.

- Data factory: used mainly to call the databricks notebooks. I also have an example of api fetch directly from `Copy` activity in the [data factory folder](https://github.com/matheusbudin/matheus-budin-case-data-engineering/tree/main/datafactory)
