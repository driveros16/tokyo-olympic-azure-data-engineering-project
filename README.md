# Olympics in Tokyo - 2021 
**Azure Cloud**

Tokyo-olympic-data-engineering-project

## Description
***
### Datapipe line to ingesting, loading and transforming the data using various azure services.

## Datapipe line - Architectural Diagram 

![image](https://github.com/driveros16/tokyo-olympic-azure-data-engineering-project/assets/112787924/c69d4d73-c160-434c-9b9f-54f9c652ec1a)


Create storage account

Create container with name ```tokyolympicdata```

Inside ```tokyolympicdata``` create two folders

+ raw-data - stored the data as it is in raw form coming from git
+  transformed-data - stored the transformed data using pyspark in databricks

## The movement of data
***
External source from ```Git``` > Azure cloud storage inside ```raw-data``` folder > Transformations in ```databricks``` using PySpark > Azure cloud storage inside ```transformed-data``` folder > Azure Synapse Analytics

## Services used
***
_Data Source_ - Data kept in git ```folder name``` **data** https://github.com/driveros16/tokyo-olympic-azure-data-engineering-project/tree/main/data
+ athletes.csv
+ coaches.csv
+ entriesgender.csv
+ medals.csv
+ teams.csv

#### Data_ingestion  
_Data factory_ - Here we will create data pipelines to ingest data from ```git folder``` to the ```raw-folder``` using **azure_data_factory**

![image](https://github.com/driveros16/tokyo-olympic-azure-data-engineering-project/assets/112787924/8963bb4f-409b-42f3-aeec-c99bbb1f5d4a)


### For transformation 
_Databricks_ - ```Tokyo_Olympic_Transformation.ipynb```

After data transfomation using PySpark store the data back to ```transformed-data``` folder inside the storage container

Now the data is in transformed state inside the **data lake gen 2**

### Getting insights 
_Azure Synapse Analytics_ - is a limitless analytics service that brings together enterprise data warehousing and Big Data analytics privilege us with services like using ADF, runing SQL queries, Python/PySpark code using either serverless or dedicated resources
+ Creating database
+ Creating tables for all the .csv files inside ```transformed-data``` folder
+ Run SQL commands to get the insights from the data even pyhton codes can be executed
![image](https://github.com/driveros16/tokyo-olympic-azure-data-engineering-project/assets/112787924/b534736d-45bb-4be6-9d51-2fd02168b4ef)





