# Durable Functions & CosmosDB ETL
This sample illustrates the use of Azure Durable Functions in combination with CosmosDB to create an ETL process (Extract - Load - Transform). The sample takes advantage of CosmosDB data replication to provide High Availability deploying the functions to two different regions. 

IMPORTANT NOTE: The code in this repository is not production-ready. It serves only to demonstrate the main points via minimal working (minmal exception handling, monitoring, etc.) Refer to the official documentation and samples for more information. 


#CosmosDB config
* Multi-region (i.e. West Europe, North Europe) and single-write

#Run locally
* Open the project folder with Visual Studio Code (ensure you have the Azure Function Extension installed). You can then, debug, run directly.
* Other option is by using the Azure functions "func" command directly in the folder.

#Deploy
* Deploy the Azure functions to two different regions (i.e West Europe, North Europe). 
* Create the configuration settings (refer to sample.settings.json) and set the correct connection strings for storage and CosmosDB. 
* Set the PreferredLocations config setting value of according to the region of the deployed functions.
