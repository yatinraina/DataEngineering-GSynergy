# DataEngineering-GSynergy
Challenge Hunting - Retail Data Aggregation

There are 4 files which represents : 
1. DFD.drawio.png -  This shows the DataFlow Diagram between several layers.
2. ER_Diagram.drawio.png - Proposed Entitiy Relationship Diagram wrt column names.
3. NB_Total_Sales.py - The Pyspark Notebook which is using the transformation.
4. PL_Total_Sales.zip - This will the template of the pipelines which can be imported to synapse(Azure Orchestration tool) envrionment only.


**Explaination of Solution :**
1. Ingestion (bronze layer) - The data was available to as files and is directly uploaded to Azure Data lake Storage Gen2 with the partition folder as '/test/2025-03-25/*'. In case the data is incrementally loaded then partition folder will be created with the date of ingestion.
2. Transformation (Silver layer) - Pyspark notebook is used for the transformation.
   'transactions' fact table and 'clnd' dimention table is loaded in the data frame and any NULLS and duplicates are revomed (if any) using the user defined function 'remove_duplicates_and_null()'.
   Inner Joined is performed between transactions'transactions' fact table and 'clnd' dimention table using fscldt_id as foriegn key.
   The data is then aggregated as sum of 'sales_units','sales_dollars','discount_dollars' grouping by 'pos_site_id','sku_id', 'price_substate_id', 'type','fsclwk_id'.
3. Consumption layer : The final data is writen into dedicated SQL Pool or sql data wareshouse and is staged between the process in the ADLS.

**Note :** 
1.The final aggregated data is only available for the date '2018-02-04' and above. If the data is required before this date then a new 'clnd' table would be required or the dates can be manully generated.
2. The code can use be for incremental loads. The path of the incremental data should be in 'yyyy-MM-dd' folder inside storage and the pipline needs to be schedule at most on the same day.



**How to Test :**
1. Import the file into snapse.
2. Edit the notebook and write the location for the files where you have uploaded the data.
3. In the last cell change the path the staging folder and for writing the data please mention the sql pool name.
Please use the change the auth mechanism if the notebook fails in your environmnet.
   
