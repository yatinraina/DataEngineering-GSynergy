#!/usr/bin/env python
# coding: utf-8

# ## NB_Total_Sales
# 
# 
# 

# In[20]:


import com.microsoft.spark.sqlanalytics
from com.microsoft.spark.sqlanalytics.Constants import Constants

#date = '2025-03-25'

#fact_averagecosts_path = 'abfss://o2comsprodbronzestgfs@o2comsprodbronzestg.dfs.core.windows.net/test/2025-03-25/fact.averagecosts.dlm.gz'
fact_transactions_path = 'abfss://o2comsprodbronzestgfs@o2comsprodbronzestg.dfs.core.windows.net/test/'+Date+'/fact.transactions.dlm.gz'
dim_clnd_path = 'abfss://o2comsprodbronzestgfs@o2comsprodbronzestg.dfs.core.windows.net/test/'+Date+'/hier.clnd.dlm.gz'
#dim_hldy_path = 'abfss://o2comsprodbronzestgfs@o2comsprodbronzestg.dfs.core.windows.net/test/2025-03-25/hier.hldy.dlm.gz'
#dim_invloc_path = 'abfss://o2comsprodbronzestgfs@o2comsprodbronzestg.dfs.core.windows.net/test/2025-03-25/hier.invloc.dlm.gz'
#dim_possite_path = 'abfss://o2comsprodbronzestgfs@o2comsprodbronzestg.dfs.core.windows.net/test/2025-03-25/hier.possite.dlm.gz'
#dim_pricestate_path = 'abfss://o2comsprodbronzestgfs@o2comsprodbronzestg.dfs.core.windows.net/test/2025-03-25/hier.pricestate.dlm.gz'
#dim_prod_path = 'abfss://o2comsprodbronzestgfs@o2comsprodbronzestg.dfs.core.windows.net/test/2025-03-25/hier.prod.dlm.gz'
#dim_rtlloc_path = 'abfss://o2comsprodbronzestgfs@o2comsprodbronzestg.dfs.core.windows.net/test/2025-03-25/hier.rtlloc.dlm.gz'



# In[10]:


def remove_duplicates_and_null(path) :

    df = spark.read.format('csv')\
         .option('header','true')\
         .options(delimiter='|',inferschema='True')\
         .load(path)

    df.count()
    df.printSchema()

    df = df.groupBy(df.columns).count().filter('count=1').drop('count')

    for i in df.columns :
        df = df.filter(f'{i} IS NOT NULL')

    df.count()

    return df

def get_duplicates_and_null(path) :

    df = spark.read.format('csv')\
         .option('header','true')\
         .options(delimiter='|',inferschema='True')\
         .load(path)

    df.count()

    df = df.groupBy(df.columns).count().filter('count>1').drop('count')

    for i in df.columns :
        df = df.filter(f'{i} IS NULL')

    df.count()

    return df


# In[11]:


df_transactions = remove_duplicates_and_null(fact_transactions_path)
df_clnd = remove_duplicates_and_null(dim_clnd_path)


# In[12]:


df_joined = df_transactions.join(df_clnd,df_transactions.fscldt_id == df_clnd.fscldt_id,'inner')\
                .drop(df_clnd.fscldt_id)


df_joined_agg = df_joined.groupBy('pos_site_id','sku_id', 'price_substate_id', 'type','fsclwk_id')\
                .sum('sales_units','sales_dollars','discount_dollars')

display(df_joined_agg)


# In[16]:


df_joined_agg.write.mode('append')\
    .option(Constants.TEMP_FOLDER, "abfss://jajmdpo2copsprbronzefs@o2comsprodbronzestg.dfs.core.windows.net/Staging")\
    .synapsesql('o2comsprodddsqlpl01.dbo.mview_weekly_sales')

