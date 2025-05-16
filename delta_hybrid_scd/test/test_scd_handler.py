# Databricks notebook source
#%pip install delta-hybrid-scd

# COMMAND ----------

#%restart_python

# COMMAND ----------

import sys
sys.path.append('/Workspace/Users/prateek.mishra@thoughtworks.com/scd_poetry')

# COMMAND ----------

import importlib
import delta_hybrid_scd.scd_handler as scd_handler
importlib.reload(scd_handler)

# COMMAND ----------

from delta_hybrid_scd import scd_handler
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.testing.utils import assertDataFrameEqual

import unittest as F 
import pyspark.testing
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# COMMAND ----------

catalog_name = "hive_metastore"
schema_name = "test_scd_lib"

spark.conf.set("SCD.CATALOG", catalog_name)
spark.conf.set("SCD.SCHEMA", schema_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS ${SCD.CATALOG}.${SCD.SCHEMA}.account_scd2;
# MAGIC DROP TABLE IF EXISTS ${SCD.CATALOG}.${SCD.SCHEMA}.account_src;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE ${SCD.CATALOG}.${SCD.SCHEMA}.account_scd2 (
# MAGIC     account_key BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 10),
# MAGIC     id INT,
# MAGIC     stock_name STRING,
# MAGIC     units INT,
# MAGIC     platform STRING,
# MAGIC     scd_key STRING,
# MAGIC     upd_key STRING,
# MAGIC     record_status STRING,
# MAGIC     effective_from TIMESTAMP,
# MAGIC     effective_to TIMESTAMP,
# MAGIC     dw_inserted_at TIMESTAMP,
# MAGIC     dw_updated_at TIMESTAMP
# MAGIC )
# MAGIC USING delta
# MAGIC TBLPROPERTIES (
# MAGIC 'delta.autoOptimize.optimizeWrite'='true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE ${SCD.CATALOG}.${SCD.SCHEMA}.account_src (
# MAGIC     id LONG,
# MAGIC     stock_name STRING,
# MAGIC     units LONG,
# MAGIC     platform STRING,
# MAGIC     reg_ts TIMESTAMP,
# MAGIC     last_modify_ts TIMESTAMP
# MAGIC )
# MAGIC USING delta
# MAGIC TBLPROPERTIES (
# MAGIC 'delta.autoOptimize.optimizeWrite'='true'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC - On day 1, load 3 new account_id's into the table

# COMMAND ----------

# DBTITLE 1,Day 1 run
def load_all_accounts_on_day_1():

    incremental_data = [
        (1, "Google", 0, "Kite", datetime(2015, 12, 25, 10, 5, 30), datetime(2025, 5, 10, 10, 5, 20)),
        (1, "BTC", 0, "Binance", datetime(2016, 12, 25, 11, 5, 30), datetime(2025, 5, 11, 10, 5, 20)),
        (3, "ETH", 20, "Binance", datetime(2016, 12, 26, 12, 7, 35), datetime(2025, 5, 11, 10, 5, 20))
    ]
    
    schema = ["id", "stock_name", "units", "platform", "reg_ts", "last_modify_ts"]
    df = spark.createDataFrame(incremental_data, schema)
    df.write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.account_src")
    
    target_table = f"{catalog_name}.{schema_name}.account_scd2"
    pk_col = ["id", "stock_name"]
    skey_col = ["units"]
    select_col_list = ["id", "stock_name", "units", "platform"]
    effective_from_col = "last_modify_ts"
    initial_eff_date_col = "reg_ts"
    
    scd_handler.apply_scd(df, skey_col, pk_col, target_table, select_col_list, effective_from_col, initial_eff_date_col)

    logger.info("Completed: load_all_accounts_on_day_1")

# COMMAND ----------

def test_all_accounts_are_correctly_loaded_on_day1():
    table_count = spark.sql(f"select count(distinct id, stock_name) from {catalog_name}.{schema_name}.account_scd2 where effective_to is null and record_status = 'A' ").collect()[0][0]

    assert table_count == 3
    logger.info("Success: test_all_accounts_are_correctly_loaded_on_day1")

# COMMAND ----------

def test_effective_from_is_correctly_loaded():
    effective_from_df = spark.sql(f"select effective_from from {catalog_name}.{schema_name}.account_scd2 where effective_to is null and record_status = 'A' order by id, stock_name")
    reg_ts_df = spark.sql(f"select cast(reg_ts as timestamp) as effective_from from {catalog_name}.{schema_name}.account_src order by id, stock_name")

    assert effective_from_df.collect() == reg_ts_df.collect()
    logger.info("Success: test_effective_from_is_correctly_loaded")

# COMMAND ----------

effective_from_df = spark.sql(f"select effective_from from {catalog_name}.{schema_name}.account_scd2 where effective_to is null and record_status = 'A' order by id, stock_name")
reg_ts_df = spark.sql(f"select cast(reg_ts as timestamp) as effective_from from {catalog_name}.{schema_name}.account_src order by id, stock_name")

# COMMAND ----------

# MAGIC %md
# MAGIC - On day 2, update units for Google and btc, this should create 2 new records for these sources
# MAGIC - Pass a duplicate data for eth, this should not update the table

# COMMAND ----------

def run_day2_incremental_load_with_updated_and_duplicate_data():

    spark.sql(f"drop table if exists {catalog_name}.{schema_name}.account_scd2_day1")
    spark.sql(f"create table {catalog_name}.{schema_name}.account_scd2_day1 as select * from {catalog_name}.{schema_name}.account_scd2")

    incremental_data = [
        (1, "Google", 100, "Kite", datetime(2015, 12, 25, 10, 5, 30), datetime(2025, 5, 12, 10, 5, 20)),
        (1, "BTC", 171, "Binance", datetime(2016, 12, 25, 11, 5, 30), datetime(2025, 5, 12, 10, 5, 20)),
        (3, "ETH", 20, "Binance", datetime(2016, 12, 26, 12, 7, 35), datetime(2025, 5, 11, 10, 5, 20))
    ]

    schema = ["id", "stock_name", "units", "platform", "reg_ts", "last_modify_ts"]
    df = spark.createDataFrame(incremental_data, schema)
    df.write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.account_src")

    target_table = f"{catalog_name}.{schema_name}.account_scd2"

    pk_col = ["id", "stock_name"]
    skey_col = ["units"]
    select_col_list = ["id", "stock_name", "units", "platform"]
    effective_from_col = "last_modify_ts"
    initial_eff_date_col = "reg_ts"

    scd_handler.apply_scd(df, skey_col, pk_col, target_table, select_col_list, effective_from_col, initial_eff_date_col)

    logger.info("Completed: run_day2_incremental_load_with_updated_and_duplicate_data")

# COMMAND ----------

def test_old_records_are_marked_with_status_as_I():
    inactive_rec_count = spark.sql(f"select count(*) from {catalog_name}.{schema_name}.account_scd2 where record_status = 'I' and effective_to is not null").collect()[0][0]

    assert inactive_rec_count == 2
    logger.info("Success: test_old_records_are_marked_with_status_as_I")

# COMMAND ----------

def test_duplicate_data_is_not_updated_on_incremental_run():
    old_data = spark.sql(f"select * from {catalog_name}.{schema_name}.account_scd2_day1 where id = 3 and stock_name = 'ETH' ")
    new_data = spark.sql(f"select * from {catalog_name}.{schema_name}.account_scd2 where id = 3 and stock_name = 'ETH' ")

    assert old_data.collect() == new_data.collect()
    logger.info("Success: test_duplicate_data_is_not_updated_on_incremental_run")

# COMMAND ----------

def test_effective_from_should_be_changed_on_incremental_run():
    old_google_date = spark.sql(f"select effective_from from {catalog_name}.{schema_name}.account_scd2_day1 where id = 1 and stock_name = 'Google' and effective_to is null and record_status = 'A' ")
    new_google_date = spark.sql(f"select effective_from from {catalog_name}.{schema_name}.account_scd2 where id = 1 and stock_name = 'Google'  and effective_to is null and record_status = 'A' ")

    old_btc_date = spark.sql(f"select effective_from from {catalog_name}.{schema_name}.account_scd2_day1 where id = 1 and stock_name = 'BTC' and effective_to is null and record_status = 'A' ")
    new_btc_date = spark.sql(f"select effective_from from {catalog_name}.{schema_name}.account_scd2 where id = 1 and stock_name = 'BTC'  and effective_to is null and record_status = 'A' ")

    assert  new_google_date.collect()[0][0] > old_google_date.collect()[0][0]
    assert  new_btc_date.collect()[0][0] > old_btc_date.collect()[0][0]
    logger.info("Success: test_effective_from_should_be_changed_on_incremental_run")

# COMMAND ----------

def test_units_should_be_changed_on_incremental_run():
    old_google_data = spark.sql(f"select units from {catalog_name}.{schema_name}.account_scd2_day1 where id = 1 and stock_name = 'Google' and effective_to is null and record_status = 'A' ")
    new_google_data = spark.sql(f"select units from {catalog_name}.{schema_name}.account_scd2 where id = 1 and stock_name = 'Google'  and effective_to is null and record_status = 'A' ")

    old_btc_data = spark.sql(f"select units from {catalog_name}.{schema_name}.account_scd2_day1 where id = 1 and stock_name = 'BTC' and effective_to is null and record_status = 'A' ")
    new_btc_data = spark.sql(f"select units from {catalog_name}.{schema_name}.account_scd2 where id = 1 and stock_name = 'BTC'  and effective_to is null and record_status = 'A' ")

    assert old_google_data.collect() != new_google_data.collect()
    assert old_btc_data.collect() != new_btc_data.collect()
    logger.info("Success: test_units_should_be_changed_on_incremental_run")

# COMMAND ----------

def test_effective_to_of_old_data_should_match_effective_from_of_new_data():
    old_effective_to_date = spark.sql(f"select distinct(effective_to) from {catalog_name}.{schema_name}.account_scd2 where id = 1 and record_status = 'I' ")
    new_effective_from_date = spark.sql(f"select distinct(effective_from) from {catalog_name}.{schema_name}.account_scd2 where id = 1 and record_status = 'A' ")

    assert old_effective_to_date.collect() == new_effective_from_date.collect()
    logger.info("Success: test_effective_to_of_old_data_should_match_effective_from_of_new_data")

# COMMAND ----------

# MAGIC %md
# MAGIC - On day 3, change platform for Google, this should trigger an update as this is a non-scd column. New row will not be created.
# MAGIC - Change units and platform for btc, this should add a new row as scd-column is also updated along with other column.

# COMMAND ----------

def run_day3_incremental_load_with_updated_data():

    spark.sql(f"drop table if exists {catalog_name}.{schema_name}.account_scd2_day2")
    spark.sql(f"create table {catalog_name}.{schema_name}.account_scd2_day2 as select * from {catalog_name}.{schema_name}.account_scd2")

    incremental_data = [
        (1, "Google", 100, "CoinSwitch", datetime(2015, 12, 25, 10, 5, 30), datetime(2025, 5, 13, 10, 5, 20)),
        (1, "BTC", 200, "CoinSwitch", datetime(2016, 12, 25, 11, 5, 30), datetime(2025, 5, 13, 10, 5, 20))
    ]

    schema = ["id", "stock_name", "units", "platform", "reg_ts", "last_modify_ts"]
    df = spark.createDataFrame(incremental_data, schema)
    df.write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.account_src")

    target_table = f"{catalog_name}.{schema_name}.account_scd2"

    pk_col = ["id", "stock_name"]
    skey_col = ["units"]
    select_col_list = ["id", "stock_name", "units", "platform"]
    effective_from_col = "last_modify_ts"
    initial_eff_date_col = "reg_ts"

    scd_handler.apply_scd(df, skey_col, pk_col, target_table, select_col_list, effective_from_col, initial_eff_date_col)

    logger.info("Completed: run_day3_incremental_load_with_updated_data")

# COMMAND ----------

def test_google_records_are_updated_for_change_in_non_scd_column():

    old_google_platform = spark.sql(f"select platform from {catalog_name}.{schema_name}.account_scd2_day2 where id = 1 and stock_name = 'Google' and effective_to is null and record_status = 'A' ")
    new_google_platform = spark.sql(f"select platform from {catalog_name}.{schema_name}.account_scd2 where id = 1 and stock_name = 'Google'  and effective_to is null and record_status = 'A' ")

    old_google_dw_updated = spark.sql(f"select dw_updated_at from {catalog_name}.{schema_name}.account_scd2_day2 where id = 1 and stock_name = 'Google' and effective_to is null and record_status = 'A' ")
    new_google_dw_updated = spark.sql(f"select dw_updated_at from {catalog_name}.{schema_name}.account_scd2 where id = 1 and stock_name = 'Google'  and effective_to is null and record_status = 'A' ")

    #count should rembtcn 2 as non-scd column has been updated
    google_rec_count = spark.sql(f"select count(*) from {catalog_name}.{schema_name}.account_scd2 where id = 1 and stock_name = 'Google' ").collect()[0][0]

    assert old_google_platform.collect() != new_google_platform.collect()
    assert old_google_dw_updated.collect() != new_google_dw_updated.collect()
    assert google_rec_count == 2

    logger.info("Success: test_google_records_are_updated_for_change_in_non_scd_column")

# COMMAND ----------

def test_btc_records_are_updated_for_change_in_scd_and_non_scd_column():

    old_btc_units = spark.sql(f"select units from {catalog_name}.{schema_name}.account_scd2_day2 where id = 1 and stock_name = 'BTC' and effective_to is null and record_status = 'A' ")
    new_btc_units = spark.sql(f"select units from {catalog_name}.{schema_name}.account_scd2 where id = 1 and stock_name = 'BTC' and effective_to is null and record_status = 'A' ")

    old_btc_platform = spark.sql(f"select platform from {catalog_name}.{schema_name}.account_scd2_day2 where id = 1 and stock_name = 'BTC' and effective_to is null and record_status = 'A' ")
    new_btc_platform = spark.sql(f"select platform from {catalog_name}.{schema_name}.account_scd2 where id = 1 and stock_name = 'BTC' and effective_to is null and record_status = 'A' ")

    #Count should be incremented to 3 as new row is now added
    btc_rec_count = spark.sql(f"select count(*) from {catalog_name}.{schema_name}.account_scd2 where id = 1 and stock_name = 'BTC' ").collect()[0][0]

    assert old_btc_units.collect() != new_btc_units.collect()
    assert old_btc_platform.collect() != new_btc_platform.collect()
    assert btc_rec_count == 3

    logger.info("Success: test_btc_records_are_updated_for_change_in_scd_and_non_scd_column")

# COMMAND ----------

load_all_accounts_on_day_1()
test_all_accounts_are_correctly_loaded_on_day1()
test_effective_from_is_correctly_loaded()

run_day2_incremental_load_with_updated_and_duplicate_data()
test_old_records_are_marked_with_status_as_I()
test_duplicate_data_is_not_updated_on_incremental_run()
test_effective_from_should_be_changed_on_incremental_run()
test_units_should_be_changed_on_incremental_run()
test_effective_to_of_old_data_should_match_effective_from_of_new_data()

run_day3_incremental_load_with_updated_data()
test_google_records_are_updated_for_change_in_non_scd_column()
test_btc_records_are_updated_for_change_in_scd_and_non_scd_column()