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
- **Python** **requests** library: to fetch data from the API using databricks notebooks, it gives us more flexibility (like selecting how many itens we want to fetch) and it is easier to maintain by looking directly into the code.

- **Microsoft Azure Cloud:** all the resources were used inside azure cloud: resource group, active directory, key vault, data lake gen2, data factory, databricks platform.

- **Data factory:** used mainly to call the databricks notebooks. I also have an example of api fetch directly from `Copy activity` in the [data factory folder](https://github.com/matheusbudin/matheus-budin-case-data-engaineering/tree/main/datafactory)

- **Databricks:** where our ETL pipeline core is located. I provided the 3 versions of code:
1) **.py** code that is managed by databricks + github versioning control. 
2) **.ipynb** that are like the jupyter notebooks so its easier to consult the code directly in github. 
3) **.dbc** files if you want to import into your own databricks workspace and test it.

- **Databricks SQL Editor:**: Used to perform AD-HOC querys that translates into some business KPI's.

- **Power BI**: To build a dashboard with some business KPIs to help business desicion making.

- **Git Hub**: for databricks code and azure data factory pipeline and resources versioning.
--------
## 3. App and services configuration

This end to end pipeline was build inside Microsoft azure cloud. With that being said, if you want to perform a similar solution, you will need the following resources:

- Azure premium subscrition (you can use the free 100 USD if its your first account);
- After that, you will need to create a resource group that is responsible to group all of your apps and resources in one place/region
- Followed by creating a storage account, make sure to check in the option "data lake gen 2" when creating it.
- Inside your storage account, create your a container with some aditional Paths/Folders: `landing-zone`, `bronze`, `silver`. `gold`.

- Create a azure data factory app, its pretty straighforward just next it until its deployed.

- Finally create a databricks resource inside azure, with the premium tier (tip: use the `trial` version that comes with 15 days of free premium DBUs).

### 3.1 Azure data factory settng up

If you are a root user you will have a Owner/admin role automatically set up for you, otherwise you will need to give, through **IAM**, the **Owner** role, to fully manage all the resources inside the pipeline, as well as having the hability to assign role **RBAC** - `Role Based Access Control`.

1. The first step is to link your git service to azure data factory this will help the versioning for pipeline, linked services and triggers.

2. **setting up the linked services**:
  - **HTTP**: this linked service will perform a api fetching (if you do not want to use the notebook plus python's requests library approach)
    - **input the base URL** : api end point URL ->  `https://api.openbrewerydb.org/breweries`
    - test the connection.

3. **Azure data lake gen2**:
  - click on the "browser" icon to point to your storage account container.
  - test the connection.

4. **Azure key vault**:
  - **Authentication method**: its recommended to select the `azure key vault` to ensure security with your access token. **Important**: this access token needs to be generated in your databricks workspace and then store into azure key vault as a **azure key vault secret**
  - **set the run time version and cluster ID**: for learning purposes and cost savings i recomend the `STARDARD_F4` cluster version in a single node mode. This is the least expensive cluster with **0.5 DBU per hour** (which basically is 0.5 USD per hour of usage)

### 3.2 Databricks setting up:

- The first step to configure databricks in Azure cloud enviroment is to create either mount points and/or the unity catalog that points to a external location. In this case, since the databricks already deploys an automatic unity catalog configured. I decided to go with the mount point method, for learning purposes and because it stills widely used in the industry (with the legacy Hive metastore)
  - The fully step by step configuration walk through is described in the databricks notebook: [mounting point configuration databricks notebook](https://github.com/matheusbudin/matheus-budin-case-data-engineering/blob/main/databricks-notebooks/databricks-notebooks-ipynb-version/config_dbfs_mount_point.ipynb)

  - Assign the **storage storage accout contributor** to databricks (this is already covered in the notebook above walkthrough - this is just an very important reminder)

- **cluster configuration**: As said in section 3.1 item 4. I recomend that you choose a run time like 11.2 that has the cluster `STARDARD_F4` (4 cores + 8gb RAM) in a single node.
  - Do not forget to input enviroment variables inside when provisioning the cluster, **DO NOT LET YOUR CREDENTIALS INSIDE YOUR CODE** those credentials are crutial to build your mount point. To access them you need to perform the following example code:
```python
import os

storage_account_name = os.getenv("STORAGE_ACCOUNT_NAME")
container_name = "api-pipeline"
mount_point = "files"
client_id = os.getenv("CLIENT_ID")
tenant_id = os.getenv("TENANT_ID")
client_secret = os.getenv("CLIENT_SECRET_VALUE")

```

- **SQL EDITOR FOR AD-HOC QUERYS**: This resource is built in databricks workspace, all you have to do is start a `sql warehouse cluster` to perform ad-hoc querys. 
- **Be carefull** the default `sql warehouse cluster` that is deployed automatically costs `40 DBUs per hour`! it is recommended to configure the least expensive version (`2 DBUs per house`) by provisioning your own `sql warehouse cluster`.

### 3.3 Azure Key Vault:

- **databricks access token**: AS described earlier, for saety reasons, it is recommended to store your databricks developer token inside a azure key vault **secret**.
- **enviroment variables**: it is recommended to store your enviroment variables as **Keys** as well.

### 3.4 Power BI Desktop:

- If you have a `Microsoft Corporate Account` you can use the Power Bi version inside **Microsoft Fabric**.
- But since i didnt have one corporate account, i used the power bi desktop.
  - In this case to connect to your database, so you can have access to your tables, it is required a **JDBC/ODBC** connection. and you will need to go on your cluster configuration and copy the information represented by the print screen bellow (**it is `censored in red` because it is also a sensitive information).

![Databricks <-> Power Bi connection](https://github.com/matheusbudin/matheus-budin-case-data-engineering/blob/main/images/databricks_powerBi_connection.png)

## 4. Coding explanations:
- **disclaimer**: all the databricks contains fully commented codes, this section is just to point the most important transformations inside the code.

### 4.1 Fetching data using Python and Requests Library:

- The following code extracted from the notebook -> [1_brewery_api_fetch_to_bronze.ipynb](https://github.com/matheusbudin/matheus-budin-case-data-engineering/blob/main/databricks-notebooks/databricks-notebooks-ipynb-version/1_brewery_api_fetch_to_bronze.ipynb)  shows the flexibility to fetch the amount of itens per api calling, just change the parameter: `per_page` that represents `itens per page` that is passed to fetch the data from API.

```python
# Initialize variables
# 'total_pages' means = set the ammount of list chuncks to be fetched
api_url = "https://api.openbrewerydb.org/v1/breweries"
per_page = 50
total_pages = 10 
all_breweries_data = []

# Fetch data from the API in chunks of 50 items
for page in range(1, total_pages + 1):
    params = {'per_page': per_page, 'page': page}
    response = requests.get(api_url, params=params)
    
    # Check for successful response
    if response.status_code == 200:
        breweries_data = response.json()
        all_breweries_data.extend(breweries_data)
    else:
        print(f"Failed to fetch data from page {page}:", response.status_code)
        break
```
- this notebook also have a deduplication, that ensures non duplicated `ID's`.

### 4.2 Data cleaning and transformation (Silver Layer):
The notebook :
[2_etl_silver.ipynb](https://github.com/matheusbudin/matheus-budin-case-data-engineering/blob/main/databricks-notebooks/databricks-notebooks-ipynb-version/2_etl_silver.ipynb)

It Has some transformations to secure data quality:

- droping rows with `NULL` **ID's**:
```python
# drops the rows with missing id
df_clean = df_clean.na.drop(subset=["id"])
```

- Hashing sensitive information: I masked the `phone` data to show a way of protection for sensitive data (e.g: name, last name, address, etc)

```python
# masking sensitive data (like phone, user cpf, user uuid and so on)
def hash_sensitive_data(df: DataFrame, column_name: str) -> DataFrame:
    return df.withColumn(column_name, sha2(col(column_name), 256))

df_clean = hash_sensitive_data(df_clean, "phone")
display(df_clean)

```
- Postal code formats filter: This code used USA postal codes as an example, it can be replicated to more postal codes patterns:

```python
#drops the rows that has the postal code out of the 'xxxxx-xxxx' format - testing for USA as an example
usa_postal_code_pattern = "^[0-9]{5}(-[0-9]{4})?$|^[0-9]{5}-?$"  # USA format: xxxxx or xxxxx-xxxx

# Filter rows with invalid postal codes for the United States
df_invalid_postal = df_clean.filter(
    (col("country") == "united states") & ~col("postal_code").rlike(usa_postal_code_pattern)
)
```

- The code checks for duplicated IDs once again, checks if there are any rows that does not pass the postal-code pattern verification:

```python

#drops the duplicated rows
df_clean = df_clean.dropDuplicates(["id"])

# Validate 'postal_code' format
if df_invalid_postal.count() > 0:
    print("Warning: Invalid postal codes found.")

#filter and test for both phone and website rows NOT NULL
df_clean = df_clean.filter(~(col("phone").isNull() & col("website_url").isNull()))
```
- then i decided o drop rows that does not have a contact information like **NULL** values for **phone**:

```python
df_no_contact_information = df_clean.filter((col("phone").isNull() & col("website_url").isNull()))
                                            
if df_no_contact_information.count() > 0:
    print(f"We have {df_no_contact_information.count()} registries that does not have a contact information")
```

- Finally, i performed a data quality check that will help the **pipeline MONITORING** , that stops the pipeline and raises an error that will be catched by Azure data factory:

```python
# Log data quality issues
data_quality_issues = df_duplicates.count() + df_invalid_postal.count() + df_no_contact_information.count()
if data_quality_issues > 0:
    raise Exception("We should not carry data with these quality checks not approved")
else:
    fl_quality = 1 #setting a quality check flag in case we need in the future
    print("Our data is good to proceed :)")
```
### 4.3 Data Aggregation - Gold Layer:
[3_etl_gold.ipynb](https://github.com/matheusbudin/matheus-budin-case-data-engineering/blob/main/databricks-notebooks/databricks-notebooks-ipynb-version/3_etl_gold.ipynb)
- This notebook aggregates the information per `name, brewery_type, state, country, city`, this aggregation can be treated like aN **OBT - One Big Table** that is widely used on the **Data Vault 2.0 and Data Vault 2.1 data modeling** this means that i can provide any business unit with only the business information that they need (this is sometimes made in `views format` but usually materialized in tables as well).

```python
# Aggregate the number of breweries per type and state
df_gold = df_silver.groupBy("name", "brewery_type", "state", "country", "city") \
                   .agg(count("id").alias("brewery_count")) \
                   .withColumn("ts_loadtimestamp_gold", current_timestamp())
```
- the ```ts_loadtimestamp_gold``` is created for incremental data methodology and data traceability.
