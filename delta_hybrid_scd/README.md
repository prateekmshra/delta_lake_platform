# Hybrid SCD1 and SCD2 Implementation

This package enables the implementation of a hybrid Slowly Changing Dimension (SCD) Type 1 and Type 2 based on changes in column values.

## Prerequisites

- **Databricks Environment**: Ensure you have access to a Databricks workspace.
- **Delta Table**: The target Delta table must include the following mandatory columns:
  - `record_status`: Indicates whether the record is active or inactive.
  - `effective_from`: Specifies when the record became active in the source system.
  - `effective_to`: Specifies when the record became inactive in the source system.
  - `dw_inserted_at`: Indicates when the record was inserted into the system.
  - `dw_updated_at`: Indicates when the record was last updated in the system.
  - `scd_key`: Hash key column created on columns on which SCD Type 2 changes are tracked.
  - `upd_key`: Hash key column on which SCD Type 1 changes are tracked.

## Setup

To set up the project, follow these steps:

```sh
pip install delta-hybrid-scd
```

```python
from delta_hybrid_scd import scd_handler
```

## Implementation Use Case

Scenario available in `delta_hybrid_scd/test/test_scd_handler`:

We have source data with the following columns:

<img src="delta_hybrid_scd/img/source_data.png" alt="Source Data" width="900"/>

Upon the initial run, we get the following:
Here, `effective_from` is created using `reg_ts` (passed from the `initial_eff_date_col` parameter).

<img src="delta_hybrid_scd/img/first_run.png" alt="Day 1 Run" width="900"/>

On the incremental run, we update values for BTC and Google. As this is an incremental run,
`effective_from` is now taken from `last_modify_ts` (passed from the `effective_from_col` parameter).

<img src="delta_hybrid_scd/img/incremental_run_1.png" alt="Incremental Run" width="900"/>

On the second incremental run, there is a change in the `platform` value for Google. As this is a non-SCD2 column, the column is directly updated (SCD1).

<img src="delta_hybrid_scd/img/incremental_run_2.png" alt="Second Incremental Run" width="900"/>