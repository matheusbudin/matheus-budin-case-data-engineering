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