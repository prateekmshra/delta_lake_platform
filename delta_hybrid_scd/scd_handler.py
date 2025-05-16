from databricks.sdk.runtime import *
from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime

def delta_merge(df, pk_col, delta_table, is_active_data = False):
    """
        Args:
            df: dataframe containing the rows to be inserted
            pk_col: list of primary key columns in the table
            delta_table: absolute name of the table where the data will be stored
            is_active_data: boolean value denoting whether the df is an active data df or not
        
        When is_active_data = False:
            - This implies the df has inactive records
            - Fetch the most recent entry from the table based on the primary key and the effective_to column
            - Update the record with the new values from the df if the merge condition is satisfied

        When is_active_data = True:
            - This implies the df has active records
            - Fetch the most recent entry from the table based on the primary key and the effective_to column
            - compare scd_key of the df with the scd_key in the table:
                - if scd_key is same:
                    - compare upd_key of the df with the upd_key in the table:
                        - if upd_key is different then:
                            - update the record with the new values from the df (SCD 1)
                        - if upd_key is also same then:
                            - do nothing as this is a duplicate record
            - if scd_key is different from the scd_key in the table then:
                - insert the new record into the table (SCD 2)
    """

    merge_condition = " AND ".join([f"target.{col} = updates.{col}" for col in pk_col]) + " AND target.effective_to is null AND target.record_status = 'A'" + (" AND target.scd_key = updates.scd_key" if is_active_data else "")

    update_condition = "target.upd_key != updates.upd_key" if is_active_data else None

    when_matched_update_column_dictionary = {
        f"{column}": f"updates.{column}"
        for column in (set(df.columns) - {"dw_inserted_at", "effective_from"})
    }

    when_not_matched_insert_column_dictionary = {
        f"{column}": f"updates.{column}"
        for column in df.columns
    }

    delta_table.alias("target").merge(
        df.alias("updates"),
        merge_condition
    ).whenMatchedUpdate(
        condition = update_condition, set=when_matched_update_column_dictionary
    ).whenNotMatchedInsert(values=when_not_matched_insert_column_dictionary).execute()
    last_operation_df = delta_table.history(1)
    display(last_operation_df.select("operationMetrics"))


def apply_scd(df, scd_key_col, pk_col, target_table, select_col_list = None, effective_from_col = None, initial_eff_date = None):
    """
    Args:
        df: dataframe containing the rows to be inserted
        scd_key_col: list of columns on which the hash value(skey) will be generated to capture SCD-2 changes
        pk_col: list of primary key columns in the table
        target_table: absolute name of the SCD table where the data will be stored, eg: development.gold_dm.client_dim
        select_col_list: list of columns to be selected from the dataframe
        effective_from_col: date/timestamp column denoting when a particular record became effective, eg: last_modify_ts
        initial_eff_date: date/timestamp column denoting the initial effective date of the record, eg: registration_date
    """

    target_df = (
        spark.sql(f"select * from {target_table}")
        .withColumn("effective_to",coalesce(col("effective_to"), current_timestamp()))
        .withColumn("row_number", row_number().over(Window.partitionBy(*pk_col).orderBy(col("dw_inserted_at").desc(), col("effective_to").desc())))
        .filter(col("row_number") == 1)
    )

    delta_table = DeltaTable.forName(spark, target_table)

    system_scd_cols = ["record_status", "effective_from", "effective_to", "dw_inserted_at", "dw_updated_at", "scd_key", "upd_key"]
    select_cols_list = [col for col in select_col_list if col not in system_scd_cols] if select_col_list else df.columns

    updates_hashkey_col = [col for col in select_cols_list if col not in scd_key_col and col not in system_scd_cols]
    scd_key_col.extend([col for col in pk_col if col not in scd_key_col])

    current_ts = lit(datetime.now())
    effective_from_ts = col(effective_from_col).cast("timestamp") if effective_from_col else current_ts
    initial_effective_from_ts = col(initial_eff_date).cast("timestamp") if initial_eff_date else effective_from_ts

    """
    - scd_key is created using scd_key_col columns, i.e., any change in these columns will mark old records as inactive and new records as active
    - upd_key is created using updates_hashkey_col columns, i.e., any change in these columns will directly be updated into the table (change will not be captured as a new row)
    """
    # Mark all records in the transformed dataframe as A (Active) and set the effective_to to Null

    active_data_df = (
        df.withColumn("record_status", lit("A"))
        .withColumn("effective_from", coalesce(effective_from_ts, current_ts)) # reason, move to line #61
        .withColumn("initial_effective_from", coalesce(initial_effective_from_ts, current_ts))
        .withColumn("effective_to", lit(None))
        .withColumn("dw_inserted_at", current_ts)
        .withColumn("dw_updated_at", current_ts)
        .withColumn("scd_key", sha2(concat_ws("", *[col(c) for c in scd_key_col]), 256))
        .withColumn("upd_key", sha2(concat_ws("", *[col(c) for c in updates_hashkey_col]), 256))
        .selectExpr(*select_cols_list, *system_scd_cols, "initial_effective_from")
    )

    # Check if the output table is empty, if yes, insert all the records into the table
    """
    Get modified records from the table by filtering on change in skey (denoting a change in any/all of the scd columns)
    """
    processed_data = (
        active_data_df.alias("active_df").join(
            target_df.alias("target_df"),
            on = pk_col,
            how = "inner"
        )
    )

    active_processed_data = processed_data.selectExpr("active_df.*")

    inactive_processed_data_to_update = (
        processed_data.filter("active_df.scd_key != target_df.scd_key")
        .selectExpr("target_df.*", "active_df.effective_from as active_eff_from")
    )

    active_unprocessed_data = (
        active_data_df.alias("active_df").join(
            target_df.alias("target_df"),
            on = pk_col,
            how = "left_anti"
        )
        .selectExpr("active_df.*")
        .withColumn("effective_from", col("initial_effective_from"))
    )

    active_union_data_df = (
        active_processed_data
        .unionByName(active_unprocessed_data)
    ).drop("initial_effective_from")
    
    # Mark the changed records as I (Inactive) and set the effective_to to current timestamp
    inactive_data_to_update = (
        inactive_processed_data_to_update.selectExpr(*select_cols_list, "active_eff_from")
        .withColumn("record_status", lit("I"))
        .withColumn("effective_to", col("active_eff_from"))
        .withColumn("dw_inserted_at", current_ts)
        .withColumn("dw_updated_at", current_ts)
        .drop("active_eff_from")
    )

    if inactive_data_to_update.isEmpty() == False:
        delta_merge(inactive_data_to_update, pk_col, delta_table)
    """
    For all the data marked as Active in the dataframe:
        1. Check if the record exists in the target table
            a. If yes
                - update the exisiting row if any of the non-scd columns have changed (change in upd_key only).
                - insert a new row if any of the scd columns have changed (change in skey).
            b. If no, this is a new record and needs to be directly inserted.
    """

    if active_union_data_df.isEmpty() == False:
        delta_merge(active_union_data_df, pk_col, delta_table, is_active_data = True)
