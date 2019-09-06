# Databricks notebook source
import pyspark
# from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
# from pyspark.sql import Row
# from pyspark import SparkContext
# from pyspark import SparkConf
# from pyspark.sql import SQLContext

# COMMAND ----------

spark = SparkSession \
    .builder \
    .appName("TPC-DS for Cosmos DB Spark") \
    .config('spark.driver.memory', '1g')  \
    .config('spark.executor.memory', '14g') \
    .config('spark.executor.cores', '4') \
    .getOrCreate()



# COMMAND ----------

file_format = "parquet"
storage_account_name = "cosmoshadoopdata"
storage_account_key = "KnjIj1g7v/qQMFOWhdAr9zQgsjNaUe1S6FaWY8C6Bb350sb7W1uwYwKjjDV2qvwde3ZkVEDLNXO00U1EEAJPJw=="
spark.conf.set("spark.hadoop.fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
spark.conf.set("fs.azure.account.key."+storage_account_name+".blob.core.windows.net",storage_account_key)


# COMMAND ----------

#Load all tables forquerying
call_center = spark.read.parquet("wasbs://cosmoshadoopdata01@cosmoshadoopdata.blob.core.windows.net/hive/warehouse/external/ds_call_centerxp")
catalog_returns = spark.read.parquet("wasbs://cosmoshadoopdata01@cosmoshadoopdata.blob.core.windows.net/hive/warehouse/external/ds_catalog_returnsxp") 
customer = spark.read.parquet("wasbs://cosmoshadoopdata01@cosmoshadoopdata.blob.core.windows.net/hive/warehouse/external/ds_customerxp") 
customer_demographics = spark.read.parquet("wasbs://cosmoshadoopdata01@cosmoshadoopdata.blob.core.windows.net/hive/warehouse/external/ds_customer_demographicsxp") 
dbgen_version = spark.read.parquet("wasbs://cosmoshadoopdata01@cosmoshadoopdata.blob.core.windows.net/hive/warehouse/external/ds_dbgen_versionxp") 
income_band = spark.read.parquet("wasbs://cosmoshadoopdata01@cosmoshadoopdata.blob.core.windows.net/hive/warehouse/external/ds_income_bandxp") 
item = spark.read.parquet("wasbs://cosmoshadoopdata01@cosmoshadoopdata.blob.core.windows.net/hive/warehouse/external/ds_itemxp") 
reason = spark.read.parquet("wasbs://cosmoshadoopdata01@cosmoshadoopdata.blob.core.windows.net/hive/warehouse/external/ds_reasonxp") 
store = spark.read.parquet("wasbs://cosmoshadoopdata01@cosmoshadoopdata.blob.core.windows.net/hive/warehouse/external/ds_storexp") 
store_sales = spark.read.parquet("wasbs://cosmoshadoopdata01@cosmoshadoopdata.blob.core.windows.net/hive/warehouse/external/ds_store_salesxp") 
warehouse = spark.read.parquet("wasbs://cosmoshadoopdata01@cosmoshadoopdata.blob.core.windows.net/hive/warehouse/external/ds_warehousexp")
web_returns = spark.read.parquet("wasbs://cosmoshadoopdata01@cosmoshadoopdata.blob.core.windows.net/hive/warehouse/external/ds_web_returnsxp") 
web_site = spark.read.parquet("wasbs://cosmoshadoopdata01@cosmoshadoopdata.blob.core.windows.net/hive/warehouse/external/ds_web_sitexp")
catalog_page = spark.read.parquet("wasbs://cosmoshadoopdata01@cosmoshadoopdata.blob.core.windows.net/hive/warehouse/external/ds_catalog_pagexp")
catalog_sales = spark.read.parquet("wasbs://cosmoshadoopdata01@cosmoshadoopdata.blob.core.windows.net/hive/warehouse/external/ds_catalog_salesxp")
customer_address = spark.read.parquet("wasbs://cosmoshadoopdata01@cosmoshadoopdata.blob.core.windows.net/hive/warehouse/external/ds_customer_addressxp")
date_dim = spark.read.parquet("wasbs://cosmoshadoopdata01@cosmoshadoopdata.blob.core.windows.net/hive/warehouse/external/ds_date_dimxp")
household_demographics = spark.read.parquet("wasbs://cosmoshadoopdata01@cosmoshadoopdata.blob.core.windows.net/hive/warehouse/external/ds_household_demographicsxp")
inventory = spark.read.parquet("wasbs://cosmoshadoopdata01@cosmoshadoopdata.blob.core.windows.net/hive/warehouse/external/ds_inventoryxp")
promotion = spark.read.parquet("wasbs://cosmoshadoopdata01@cosmoshadoopdata.blob.core.windows.net/hive/warehouse/external/ds_promotionxp")
ship_mode = spark.read.parquet("wasbs://cosmoshadoopdata01@cosmoshadoopdata.blob.core.windows.net/hive/warehouse/external/ds_ship_modexp")
store_returns = spark.read.parquet("wasbs://cosmoshadoopdata01@cosmoshadoopdata.blob.core.windows.net/hive/warehouse/external/ds_store_returnsxp")
time_dim = spark.read.parquet("wasbs://cosmoshadoopdata01@cosmoshadoopdata.blob.core.windows.net/hive/warehouse/external/ds_time_dimxp")
web_page = spark.read.parquet("wasbs://cosmoshadoopdata01@cosmoshadoopdata.blob.core.windows.net/hive/warehouse/external/ds_web_pagexp")
web_sales = spark.read.parquet("wasbs://cosmoshadoopdata01@cosmoshadoopdata.blob.core.windows.net/hive/warehouse/external/ds_web_salesxp")

#cache all tables
call_center.cache
catalog_returns.cache
customer.cache
customer_demographics.cache
dbgen_version.cache
income_band.cache
item.cache
reason.cache
store.cache
store_sales.cache
warehouse.cache
web_returns.cache
web_site.cache
catalog_page.cache
catalog_sales.cache
customer_address.cache
date_dim.cache
household_demographics.cache
inventory.cache
promotion.cache
ship_mode.cache
store_returns.cache
time_dim.cache
web_page.cache
web_sales.cache


#Register all tables in Spark SQL
call_center.createOrReplaceTempView("call_center")
catalog_returns.createOrReplaceTempView("catalog_returns")
customer.createOrReplaceTempView("customer")
customer_demographics.createOrReplaceTempView("customer_demographics")
dbgen_version.createOrReplaceTempView("dbgen_version")
income_band.createOrReplaceTempView("income_band")
item.createOrReplaceTempView("item")
reason.createOrReplaceTempView("reason")
store.createOrReplaceTempView("store")
store_sales.createOrReplaceTempView("store_sales")
warehouse.createOrReplaceTempView("warehouse")
web_returns.createOrReplaceTempView("web_returns")
web_site.createOrReplaceTempView("web_site")
catalog_page.createOrReplaceTempView("catalog_page")
catalog_sales.createOrReplaceTempView("catalog_sales")
customer_address.createOrReplaceTempView("customer_address")
date_dim.createOrReplaceTempView("date_dim")
household_demographics.createOrReplaceTempView("household_demographics")
inventory.createOrReplaceTempView("inventory")
promotion.createOrReplaceTempView("promotion")
ship_mode.createOrReplaceTempView("ship_mode")
store_returns.createOrReplaceTempView("store_returns")
time_dim.createOrReplaceTempView("time_dim")
web_page.createOrReplaceTempView("web_page")
web_sales.createOrReplaceTempView("web_sales")

from datetime import datetime

# COMMAND ----------
q1start = datetime.now()

query1 = spark.sql("WITH customer_total_return AS \
   (SELECT sr_customer_sk AS ctr_customer_sk, sr_store_sk AS ctr_store_sk, \
           sum(sr_return_amt) AS ctr_total_return \
    FROM store_returns, date_dim \
    WHERE sr_returned_date_sk = d_date_sk AND d_year = 2000 \
    GROUP BY sr_customer_sk, sr_store_sk) \
 SELECT c_customer_id \
   FROM customer_total_return ctr1, store, customer \
   WHERE ctr1.ctr_total_return > \
    (SELECT avg(ctr_total_return)*1.2 \
      FROM customer_total_return ctr2 \
       WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk) \
   AND s_store_sk = ctr1.ctr_store_sk \
   AND s_state = 'TN' \
   AND ctr1.ctr_customer_sk = c_customer_sk \
   ORDER BY c_customer_id LIMIT 100").show()

q1end = datetime.now()
# COMMAND ----------
q2start = datetime.now()
query2 = spark.sql("WITH wscs as  \
 (SELECT sold_date_sk, sales_price  \
  FROM (SELECT ws_sold_date_sk sold_date_sk, ws_ext_sales_price sales_price  \
        FROM web_sales  \
        UNION ALL  \
       SELECT cs_sold_date_sk sold_date_sk, cs_ext_sales_price sales_price  \
        FROM catalog_sales) x),  \
 wswscs AS  \
 (SELECT d_week_seq,  \
        sum(case when (d_day_name='Sunday') then sales_price else null end) sun_sales,  \
        sum(case when (d_day_name='Monday') then sales_price else null end) mon_sales,  \
        sum(case when (d_day_name='Tuesday') then sales_price else  null end) tue_sales,  \
        sum(case when (d_day_name='Wednesday') then sales_price else null end) wed_sales,  \
        sum(case when (d_day_name='Thursday') then sales_price else null end) thu_sales,  \
        sum(case when (d_day_name='Friday') then sales_price else null end) fri_sales,  \
        sum(case when (d_day_name='Saturday') then sales_price else null end) sat_sales  \
 FROM wscs, date_dim  \
 WHERE d_date_sk = sold_date_sk  \
 GROUP BY d_week_seq)  \
 SELECT d_week_seq1  \
       ,round(sun_sales1/sun_sales2,2)  \
       ,round(mon_sales1/mon_sales2,2)  \
       ,round(tue_sales1/tue_sales2,2)  \
       ,round(wed_sales1/wed_sales2,2)  \
       ,round(thu_sales1/thu_sales2,2)  \
       ,round(fri_sales1/fri_sales2,2)  \
       ,round(sat_sales1/sat_sales2,2)  \
 FROM  \
 (SELECT wswscs.d_week_seq d_week_seq1  \
        ,sun_sales sun_sales1  \
        ,mon_sales mon_sales1  \
        ,tue_sales tue_sales1  \
        ,wed_sales wed_sales1  \
        ,thu_sales thu_sales1  \
        ,fri_sales fri_sales1  \
        ,sat_sales sat_sales1  \
  FROM wswscs,date_dim  \
  WHERE date_dim.d_week_seq = wswscs.d_week_seq AND d_year = 2001) y,  \
 (SELECT wswscs.d_week_seq d_week_seq2  \
        ,sun_sales sun_sales2  \
        ,mon_sales mon_sales2  \
        ,tue_sales tue_sales2  \
        ,wed_sales wed_sales2  \
        ,thu_sales thu_sales2  \
        ,fri_sales fri_sales2  \
        ,sat_sales sat_sales2  \
  FROM wswscs, date_dim  \
  WHERE date_dim.d_week_seq = wswscs.d_week_seq AND d_year = 2001 + 1) z  \
 WHERE d_week_seq1=d_week_seq2-53  \
 ORDER BY d_week_seq1").show()

q2end = datetime.now()
# COMMAND ----------
q3start = datetime.now()
query3 = spark.sql("SELECT dt.d_year, item.i_brand_id brand_id, item.i_brand brand,SUM(ss_ext_sales_price) sum_agg  \
 FROM  date_dim dt, store_sales, item  \
 WHERE dt.d_date_sk = store_sales.ss_sold_date_sk  \
   AND store_sales.ss_item_sk = item.i_item_sk  \
   AND item.i_manufact_id = 128  \
   AND dt.d_moy=11  \
 GROUP BY dt.d_year, item.i_brand, item.i_brand_id  \
 ORDER BY dt.d_year, sum_agg desc, brand_id  \
 LIMIT 100").show()

q3end = datetime.now()
# COMMAND ----------
q4start = datetime.now()
query4 = spark.sql("WITH year_total AS (  \
 SELECT c_customer_id customer_id,  \
        c_first_name customer_first_name,  \
        c_last_name customer_last_name,  \
        c_preferred_cust_flag customer_preferred_cust_flag,  \
        c_birth_country customer_birth_country,  \
        c_login customer_login,  \
        c_email_address customer_email_address,  \
        d_year dyear,  \
        sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2) year_total,  \
        's' sale_type  \
 FROM customer, store_sales, date_dim  \
 WHERE c_customer_sk = ss_customer_sk AND ss_sold_date_sk = d_date_sk  \
 GROUP BY c_customer_id,  \
          c_first_name,  \
          c_last_name,  \
          c_preferred_cust_flag,  \
          c_birth_country,  \
          c_login,  \
          c_email_address,  \
          d_year  \
 UNION ALL  \
 SELECT c_customer_id customer_id,  \
        c_first_name customer_first_name,  \
        c_last_name customer_last_name,  \
        c_preferred_cust_flag customer_preferred_cust_flag,  \
        c_birth_country customer_birth_country,  \
        c_login customer_login,  \
        c_email_address customer_email_address,  \
        d_year dyear,  \
        sum((((cs_ext_list_price-cs_ext_wholesale_cost-cs_ext_discount_amt)+cs_ext_sales_price)/2) ) year_total,  \
        'c' sale_type  \
 FROM customer, catalog_sales, date_dim  \
 WHERE c_customer_sk = cs_bill_customer_sk AND cs_sold_date_sk = d_date_sk  \
 GROUP BY c_customer_id,  \
          c_first_name,  \
          c_last_name,  \
          c_preferred_cust_flag,  \
          c_birth_country,  \
          c_login,  \
          c_email_address,  \
          d_year  \
 UNION ALL  \
 SELECT c_customer_id customer_id  \
       ,c_first_name customer_first_name  \
       ,c_last_name customer_last_name  \
       ,c_preferred_cust_flag customer_preferred_cust_flag  \
       ,c_birth_country customer_birth_country  \
       ,c_login customer_login  \
       ,c_email_address customer_email_address  \
       ,d_year dyear  \
       ,sum((((ws_ext_list_price-ws_ext_wholesale_cost-ws_ext_discount_amt)+ws_ext_sales_price)/2) ) year_total  \
       ,'w' sale_type  \
 FROM customer, web_sales, date_dim  \
 WHERE c_customer_sk = ws_bill_customer_sk AND ws_sold_date_sk = d_date_sk  \
 GROUP BY c_customer_id,  \
          c_first_name,  \
          c_last_name,  \
          c_preferred_cust_flag,  \
          c_birth_country,  \
          c_login,  \
          c_email_address,  \
          d_year)  \
 SELECT  \
   t_s_secyear.customer_id,  \
   t_s_secyear.customer_first_name,  \
   t_s_secyear.customer_last_name,  \
   t_s_secyear.customer_preferred_cust_flag  \
 FROM year_total t_s_firstyear, year_total t_s_secyear, year_total t_c_firstyear,  \
      year_total t_c_secyear, year_total t_w_firstyear, year_total t_w_secyear  \
 WHERE t_s_secyear.customer_id = t_s_firstyear.customer_id  \
   and t_s_firstyear.customer_id = t_c_secyear.customer_id  \
   and t_s_firstyear.customer_id = t_c_firstyear.customer_id  \
   and t_s_firstyear.customer_id = t_w_firstyear.customer_id  \
   and t_s_firstyear.customer_id = t_w_secyear.customer_id  \
   and t_s_firstyear.sale_type = 's'  \
   and t_c_firstyear.sale_type = 'c'  \
   and t_w_firstyear.sale_type = 'w'  \
   and t_s_secyear.sale_type = 's'  \
   and t_c_secyear.sale_type = 'c'  \
   and t_w_secyear.sale_type = 'w'  \
   and t_s_firstyear.dyear = 2001  \
   and t_s_secyear.dyear = 2001+1  \
   and t_c_firstyear.dyear = 2001  \
   and t_c_secyear.dyear = 2001+1  \
   and t_w_firstyear.dyear = 2001  \
   and t_w_secyear.dyear = 2001+1  \
   and t_s_firstyear.year_total > 0  \
   and t_c_firstyear.year_total > 0  \
   and t_w_firstyear.year_total > 0  \
   and case when t_c_firstyear.year_total > 0 then t_c_secyear.year_total / t_c_firstyear.year_total else null end  \
           > case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else null end  \
   and case when t_c_firstyear.year_total > 0 then t_c_secyear.year_total / t_c_firstyear.year_total else null end  \
           > case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total else null end  \
 ORDER BY  \
   t_s_secyear.customer_id,  \
   t_s_secyear.customer_first_name,  \
   t_s_secyear.customer_last_name,  \
   t_s_secyear.customer_preferred_cust_flag  \
 LIMIT 100").show()

q4end = datetime.now()
# COMMAND ----------
q5start = datetime.now()
query5 = spark.sql("WITH ssr AS  \
  (SELECT s_store_id,  \
          sum(sales_price) as sales,  \
          sum(profit) as profit,  \
          sum(return_amt) as returns,  \
          sum(net_loss) as profit_loss  \
  FROM  \
    (SELECT ss_store_sk as store_sk,  \
            ss_sold_date_sk  as date_sk,  \
            ss_ext_sales_price as sales_price,  \
            ss_net_profit as profit,  \
            cast(0 as decimal(7,2)) as return_amt,  \
            cast(0 as decimal(7,2)) as net_loss  \
    FROM store_sales  \
    UNION ALL  \
    SELECT sr_store_sk as store_sk,  \
           sr_returned_date_sk as date_sk,  \
           cast(0 as decimal(7,2)) as sales_price,  \
           cast(0 as decimal(7,2)) as profit,  \
           sr_return_amt as return_amt,  \
           sr_net_loss as net_loss  \
    FROM store_returns)  \
    salesreturns, date_dim, store  \
  WHERE date_sk = d_date_sk  \
       and d_date between cast('2000-08-23' as date)  \
                  and ((cast('2000-08-23' as date) + interval '14' day))  \
       and store_sk = s_store_sk  \
 GROUP BY s_store_id),  \
 csr AS  \
 (SELECT cp_catalog_page_id,  \
         sum(sales_price) as sales,  \
         sum(profit) as profit,  \
         sum(return_amt) as returns,  \
         sum(net_loss) as profit_loss  \
 FROM  \
   (SELECT cs_catalog_page_sk as page_sk,  \
           cs_sold_date_sk  as date_sk,  \
           cs_ext_sales_price as sales_price,  \
           cs_net_profit as profit,  \
           cast(0 as decimal(7,2)) as return_amt,  \
           cast(0 as decimal(7,2)) as net_loss  \
    FROM catalog_sales  \
    UNION ALL  \
    SELECT cr_catalog_page_sk as page_sk,  \
           cr_returned_date_sk as date_sk,  \
           cast(0 as decimal(7,2)) as sales_price,  \
           cast(0 as decimal(7,2)) as profit,  \
           cr_return_amount as return_amt,  \
           cr_net_loss as net_loss  \
    from catalog_returns  \
   ) salesreturns, date_dim, catalog_page  \
 WHERE date_sk = d_date_sk  \
       and d_date between cast('2000-08-23' as date)  \
                  and ((cast('2000-08-23' as date) + interval '14' day))  \
       and page_sk = cp_catalog_page_sk  \
 GROUP BY cp_catalog_page_id)  \
 ,  \
 wsr AS  \
 (SELECT web_site_id,  \
         sum(sales_price) as sales,  \
         sum(profit) as profit,  \
         sum(return_amt) as returns,  \
         sum(net_loss) as profit_loss  \
 from  \
  (select  ws_web_site_sk as wsr_web_site_sk,  \
            ws_sold_date_sk  as date_sk,  \
            ws_ext_sales_price as sales_price,  \
            ws_net_profit as profit,  \
            cast(0 as decimal(7,2)) as return_amt,  \
            cast(0 as decimal(7,2)) as net_loss  \
    from web_sales  \
    union all  \
    select ws_web_site_sk as wsr_web_site_sk,  \
           wr_returned_date_sk as date_sk,  \
           cast(0 as decimal(7,2)) as sales_price,  \
           cast(0 as decimal(7,2)) as profit,  \
           wr_return_amt as return_amt,  \
           wr_net_loss as net_loss  \
    FROM web_returns LEFT  OUTER JOIN web_sales on  \
         ( wr_item_sk = ws_item_sk  \
           and wr_order_number = ws_order_number)  \
   ) salesreturns, date_dim, web_site  \
 WHERE date_sk = d_date_sk  \
       and d_date between cast('2000-08-23' as date)  \
                  and ((cast('2000-08-23' as date) + interval '14' day))  \
       and wsr_web_site_sk = web_site_sk  \
 GROUP BY web_site_id)  \
 SELECT channel,  \
        id,  \
        sum(sales) as sales,  \
        sum(returns) as returns,  \
        sum(profit) as profit  \
 from  \
 (select 'store channel' as channel,  \
         concat('store', s_store_id) as id,  \
         sales,  \
         returns,  \
        (profit - profit_loss) as profit  \
 FROM ssr  \
 UNION ALL  \
 select 'catalog channel' as channel,  \
        concat('catalog_page', cp_catalog_page_id) as id,  \
        sales,  \
        returns,  \
        (profit - profit_loss) as profit  \
 FROM  csr  \
 UNION ALL  \
 SELECT 'web channel' as channel,  \
        concat('web_site', web_site_id) as id,  \
        sales,  \
        returns,  \
        (profit - profit_loss) as profit  \
 FROM wsr  \
 ) x  \
 GROUP BY ROLLUP (channel, id)  \
 ORDER BY channel, id  \
 LIMIT 100").show()

q5end = datetime.now()
# COMMAND ----------

print(q1end-q1start)
print(q2end-q2start)
print(q3end-q3start)
print(q4end-q4start)
print(q5end-q5start)

