# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook Intro 
# MAGIC
# MAGIC > **Version  1.00**  \
# MAGIC Developer : Emiliano Barco\
# MAGIC Second Release Date: Dec,2023 \
# MAGIC Changes: Initial release
# MAGIC
# MAGIC > **Version  2.0**  \
# MAGIC Developer : Hart Zhou\
# MAGIC Second Release Date: Oct,2024 \
# MAGIC Changes: Add addtional filter from BRD
# MAGIC
# MAGIC  > **Version  2.1**  \
# MAGIC Developer : Rafeed Sultaan
# MAGIC
# MAGIC Release Date: TBD \
# MAGIC Changes:<br>
# MAGIC 1) Refactored and optimised the notebook (replaced selfjoins and distinct)
# MAGIC 2) Added 3 days buffer to the Load Window Days
# MAGIC 3) Added Lookback Functionality
# MAGIC 4) Using a combination of 3 metrics for qualified transactions
# MAGIC 5) Added Generic DB Writes
# MAGIC
# MAGIC Detail: Rule 20:  
# MAGIC Confluence Page:[Rule 20 ](https://thestar.atlassian.net/wiki/spaces/DNA/pages/1995440327/Rule+20+-+Cancel+Credit)
# MAGIC PS. It is an '_All Properties' Notebook. But never named to _All_Properties because of GIT Version History
# MAGIC
# MAGIC
# MAGIC
# MAGIC | Key Parameter | Details |
# MAGIC | ----------- | ----------- |
# MAGIC | DateProcessing | ADF passed UTC time or manually input for historical reload |
# MAGIC | RuleID |  Constant field = 20 |
# MAGIC | RunTestMode | 1) 'no' -> Default mode for Production and UAT runs.<br> 2) 'prod_data_test' -> Test on prod_data_test, with target odl write <br> 3) 'unit_test_only' -> Unit Testing Mode, without target odl write <br> 4) 'data_test' -> Black Box Testing Mode (Legacy) | 
# MAGIC  ---
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Begins
# MAGIC
# MAGIC This process generates ALERTS based on Cancel Credit along with other types of transactions transactions for a given 7 days window based on a start-date.
# MAGIC
# MAGIC if CurrentProcessDate = 2023-06-20 the start-date will be 2023-06-19 (CurrentProcessDate -1 day). 
# MAGIC From this date (2023-06-19) it is required to generate a 7 days window per day for 7 days starting from 2023-06-19 till 2023-06-13.
# MAGIC Note: We will need 14 days of data to generate a 7 days 7-days-window as 2023-06-13 will need also another 6 days of data previous to this date to generate the 7-days-window.
# MAGIC
# MAGIC Example:
# MAGIC -------
# MAGIC Generate 7 days 7-days-window from 2023-06-19 to 2023-06-13\
# MAGIC Date 2023-06-19 --> generate a 7 days window from 2023-06-13 to 2023-06-19\
# MAGIC Date 2023-06-18 --> generate a 7 days window from 2023-06-12 to 2023-06-18\
# MAGIC Date 2023-06-17 --> generate a 7 days window from 2023-06-11 to 2023-06-17\
# MAGIC ...\
# MAGIC Date 2023-06-13 --> generate a 7 days window from 2023-06-07 to 2023-06-13

# COMMAND ----------

# MAGIC %run /Auth_Notebooks/AuthenticationClass

# COMMAND ----------

# MAGIC %run /AML/utils/aml_utils

# COMMAND ----------

# MAGIC %run /AML/metrics/aml_metrics

# COMMAND ----------

# MAGIC %md # Configurations

# COMMAND ----------

# MAGIC %md ## Src Table Config

# COMMAND ----------

# Source Table Configurations
cfg_table_list = [
   {
        'TYPE': 'TRANSACTIONAL',
        'NAME': 'devtrans',
        'SOURCE': 'ACCESS',
        'PRIMARY_KEY': ['TRANSID'],
        'SOURCE_COLUMN': [
            {'COLUMN_NAME': 'TRANSID',      'DATA_TYPE': 'BIGINT'},
            {'COLUMN_NAME': 'PTNID',        'DATA_TYPE': 'BIGINT'},
            {'COLUMN_NAME': 'DTTYPID',      'DATA_TYPE': 'BIGINT'},
            {'COLUMN_NAME': 'CREDITAMT',    'DATA_TYPE': 'DECIMAL(14,2)'},
            {'COLUMN_NAME': 'DEBITAMT',     'DATA_TYPE': 'DECIMAL(14,2)'},
            {'COLUMN_NAME': 'DEVID',        'DATA_TYPE': 'BIGINT'},
            {'COLUMN_NAME': 'COMPEXPIRE',   'DATA_TYPE': 'TIMESTAMP'},
            {'COLUMN_NAME': 'PAPERREFNUM',  'DATA_TYPE': 'STRING'},
            {'COLUMN_NAME': 'CHECKNUM',     'DATA_TYPE': 'STRING'},
            {'COLUMN_NAME': 'LASTMODDATE',  'DATA_TYPE': 'TIMESTAMP'}
        ]
    },
    {
        'TYPE': 'REFERENCE',
        'NAME': 'devtranstype',
        'SOURCE': 'ACCESS',
        'PRIMARY_KEY': ['DTTYPID'],
        'SOURCE_COLUMN': [
            {'COLUMN_NAME': 'DTTYPID',      'DATA_TYPE': 'BIGINT'},
            {'COLUMN_NAME': 'DTTYPNAME',    'DATA_TYPE': 'STRING'},
            {'COLUMN_NAME': 'LASTMODDATE',  'DATA_TYPE': 'TIMESTAMP'}
        ]
    }
  ]

# COMMAND ----------

# MAGIC %md ## Notebook Parameters and Configuration

# COMMAND ----------

# WIDGETS
dbutils.widgets.dropdown("ENV", "", ["", "UAT", "PROD"])
dbutils.widgets.text("EnvironmentSchema", "dbo")
dbutils.widgets.text("JobRunID", "")
dbutils.widgets.text("RuleID", "20") 
dbutils.widgets.text("CurrentProcessDate", "")
dbutils.widgets.dropdown("RunTestMode", "no", ['no','prod_data_test','data_test','unit_test_only'])
dbutils.widgets.dropdown("isLookback", "no", ['no', 'yes'])

#PARAMETERS
param_ENV = dbutils.widgets.get("ENV")
environment_schema = dbutils.widgets.get("EnvironmentSchema")
jobrunid = dbutils.widgets.get("JobRunID") # populated by ADF
ruleid = dbutils.widgets.get("RuleID") # populated by ADF
p_current_process_date = dbutils.widgets.get("CurrentProcessDate")
param_isLookback = dbutils.widgets.get("isLookback")
param_run_tests_mode = dbutils.widgets.get("RunTestMode")

if param_run_tests_mode != 'unit_test_only':
  AUTH = AuthenticationClass(param_ENV)
  cfg_ENV = AUTH.env_config # All env variables

  Storage_ACCESS_Account_Name = cfg_ENV['Storage_ACCESS_Account_Name']
  Storage_ACCESS_Container_Name = cfg_ENV['Storage_ACCESS_Container_Name']
  aml_sql_ConString = AUTH.get_aml_connection_string()
  
  spark.conf.set(f"fs.azure.sas.{Storage_ACCESS_Container_Name}.{Storage_ACCESS_Account_Name}.blob.core.windows.net", AUTH.get_access_sas('r'))

  #######
  #Default Path for UAT and Prods run. param_run_tests_mode -> 'no'
  blobBaseUrl = f"wasbs://{Storage_ACCESS_Container_Name}@{Storage_ACCESS_Account_Name}.blob.core.windows.net/internal/"
  srcname = 'SYNKROS ALL PROPERTIES' # name of the source, Brisbane, Gold Coast, Sydney for Synkros,
  table_mdm_current_customer = "mdm.slvr_mdm_current_customer"
  source_data_schema = "silver_cleansed" # Usage of these tables/views in detiremined by the table config - currently used by reference tables
  dt_current_process = datetime.strptime(p_current_process_date, "%Y-%m-%d") # Convert input param currentProcessDate to datetime object 

  if param_run_tests_mode == 'prod_data_test': #This mode is enable testing and validation on production datasets within UAT env.
    blobBaseUrl = "/mnt/amlremediation/AMLProdDataImport/"
    source_data_schema = "amlremediation" # Usage of these tables/views in detiremined by the table config - currently used by reference tables
  elif param_run_tests_mode == 'data_test': #This mode is to control test Key source & target for making scenarios in 'Unit Test' runs.
    blobBaseUrlKeyTables = '/mnt/access/internal/test/aml_rule20_testcases/'
    source_data_schema = 'amlremediation'
    environment_schema=helpertableschema


  # Config DB for Generic DB Writes
  cfg_db = {
    'table_dimrule': f'{environment_schema}.dim_AMLTransactionMonitoringRule',
    'table_dimcase': f'{environment_schema}.dim_AMLTransactionMonitoringCase',
    'table_dimcaseattribute': f'{environment_schema}.dim_AMLTransactionMonitoringCaseAttribute',
    'table_factcasecreation': f'{environment_schema}.fact_AMLTransactionMonitoringCaseCreation',
    'table_factcasedetail': f'{environment_schema}.fact_AMLTransactionMonitoringCaseDetail',
    'isLookback_value': '1' if param_isLookback == 'yes' else '0',
    'srcname': srcname,
    'ruleid': ruleid,
    'jobrunid': int(jobrunid)
  }

  print(f"""*** PARAMS ***
    {environment_schema = }
    {jobrunid = }
    {ruleid = }
    {p_current_process_date = }
    {param_run_tests_mode = }
    {source_data_schema}
    {param_isLookback = }
  """)
  for k in cfg_db:
    print(f"{k}: `{cfg_db[k]}`")

# COMMAND ----------

# MAGIC %md ## Parameter Validation

# COMMAND ----------

if param_run_tests_mode != 'unit_test_only':
  if param_ENV not in ['UAT', 'PROD']:
    raise ValueError(f"INVALID ENV PARAM: `{param_ENV}`. Allowed environments are: `UAT`, `PROD`. Please provide a valid environment.")
  if param_run_tests_mode not in ['no', 'prod_data_test','data_test','unit_test_only']:
    raise ValueError("INVALID RUN MODE")
  if environment_schema not in ['uat', 'dbo', 'uomd']:
      raise ValueError(f"INVALID PRIMARY ENVIRONMENT/SCHEMA PARAM: `{environment_schema}`. Allowed environments are: `uat`, `dbo`. Please provide a valid environment_schema.")
  if ruleid != "20": 
    raise ValueError("THIS NOTEBOOK IS SET FOR AML 20") 

# COMMAND ----------

# MAGIC %md ## Get Parameters from DB

# COMMAND ----------

if param_run_tests_mode != 'unit_test_only':
  rule_param_cfg = get_rule_param_db(env = environment_schema, rule_id = ruleid)
  print(f"PARAMS:\n\t{rule_param_cfg}")

# COMMAND ----------

# MAGIC %md # Load Tables

# COMMAND ----------

# MAGIC %md ## Load Src Qualified Data

# COMMAND ----------

if param_run_tests_mode != 'unit_test_only':
  txn_window = get_processing_window(
    current_process_date = dt_current_process, 
    window_type = "daily", 
    window_unit = 7 + int(rule_param_cfg['days_to_generate_rolling_window'])   # WEEKLY i.e. 13 days for 7 Days Rolling Window Calculations
  )
  cfg_db['txn_window'] = txn_window # Set value for DB write params 

  load_window_days =  int(rule_param_cfg['RawDataLoadDays'])   + 3  # Additional number of days for which files are back loaded from the current process date (1 = no buffer)

  qualified_data = get_qualified_data(
    cfg_table_list = cfg_table_list,
    blobBaseUrl = blobBaseUrl,
    source_data_schema = source_data_schema,
    dt_current_process = dt_current_process,
    load_window_days =  load_window_days
  )

  print(f"DATA REVIEW WINDOW:\n\t{txn_window}")
  print(f"FILE LOAD WINDOW:\n\t{load_window_days} Days")

  AML_METRICS = AMLMetrics(
    qualified_data = qualified_data,
    txn_window = txn_window
  )

# COMMAND ----------

# MAGIC %md # Business Logic

# COMMAND ----------

# MAGIC %md ## Rolling Window Transactions

# COMMAND ----------

def get_rollingwindow_trans_df( tito_redeemed_df:DataFrame,
                                cancel_credit_redeemed_df:DataFrame,
                                eg_jackpot_redeemed_df:DataFrame,
)->DataFrame:
  
  combined_metrics_df = (tito_redeemed_df
                        .unionByName(cancel_credit_redeemed_df)
                        .unionByName(eg_jackpot_redeemed_df))
  
  combined_metrics_df.createOrReplaceTempView('vw_combined_metrics')

  return spark.sql(f"""
  WITH qualified_transactions AS (
    SELECT
      TransSource, 
      src_property,
      PTNID,
      TransID,
      TransDt,-- Timestamp
      TransTypName,
      QualifiedDebitAmt,
      PAPERREFNUM,
      CHECKNUM,
      (CAST(TRANSDT AS DATE) - INTERVAL {rule_param_cfg['days_to_generate_rolling_window']} DAY)  AS WindowStartDate,
      CAST(TRANSDT AS DATE) AS WindowEndDate
    FROM 
      vw_combined_metrics
    WHERE 
      QualifiedDebitAmt >= {rule_param_cfg['transactionAmountThreshold']}
  ),
  Rolling7Days AS (
    SELECT
      TransSource, 
      src_property,
      PTNID,
      TransID,
      TransDt,-- Timestamp
      TransTypName,
      QualifiedDebitAmt,
      PAPERREFNUM,
      CHECKNUM,
      WindowStartDate,
      WindowEndDate,
      COUNT(TRANSID) OVER (PARTITION  BY PTNID 
                                ORDER BY CAST(WindowEndDate AS TIMESTAMP) ASC 
                                  RANGE BETWEEN INTERVAL {rule_param_cfg['days_to_generate_rolling_window']} DAY PRECEDING 
                                                AND CURRENT ROW
      ) AS WindowTransactionCount,
      SUM(QualifiedDebitAmt) OVER (PARTITION BY PTNID 
                                                  ORDER BY CAST(WindowEndDate AS TIMESTAMP) ASC
                                                    RANGE BETWEEN INTERVAL {rule_param_cfg['days_to_generate_rolling_window']} DAY PRECEDING 
                                                                  AND CURRENT ROW
      ) AS WindowTransactionAmount
    FROM 
      qualified_transactions
  )
  SELECT
    TransSource, 
    src_property,
    PTNID,
    TransID,
    TransDt,-- Timestamp
    TransTypName,
    QualifiedDebitAmt,
    -- AUDIT Fields
    PAPERREFNUM,
    CHECKNUM,
    --- Rolling 7 Day Window Calculations
    WindowStartDate,
    WindowEndDate,
    WindowTransactionCount,
    WindowTransactionAmount
  FROM 
    Rolling7Days
  """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Rule Logic & Format Cases

# COMMAND ----------

def get_rule_case_transactions(
  rollingwindow_trans_df: DataFrame, # Ticket Refining Windowed Transactions Dataframe
  df_patron_details: DataFrame, # Patron customerNumber & Name
  cfg_rule: dict, # Threshold params
  cfg_db: dict # DB Write/Case formatting params
) -> DataFrame:
  """
  Calculates all positive cases for this rule and formats them for a DB write case hash for this rule - CancelCreditAndJackpot, R20 using:
  - Rule ID, PTNID, Case Grouping
  - The parameter values
  """
  rollingwindow_trans_df.createOrReplaceTempView("vw_rollingwindow_trans")
  df_patron_details.createOrReplaceTempView("vw_patron_details")

  return spark.sql(f"""
  WITH qualified_window_calculations AS (
    SELECT
      rt.PTNID,
      --- Rolling 7 Day Window Calculations
      rt.WindowStartDate,
      rt.WindowEndDate,
      rt.WindowTransactionCount,
      rt.WindowTransactionAmount
    FROM 
      vw_rollingwindow_trans rt 
    WHERE
      --  ALERTING
      CASE 
        WHEN (rt.WindowTransactionCount>={rule_param_cfg['transactionCountThreshold']} 
              AND rt.WindowTransactionAmount>{rule_param_cfg['debitAmountThreshold_1']}) THEN TRUE 
        WHEN (rt.WindowTransactionAmount >= {rule_param_cfg['debitAmountThreshold_2'] }) THEN TRUE
        ELSE FALSE
      END
    QUALIFY
      ROW_NUMBER() OVER(PARTITION BY  rt.PTNID
                                      ORDER BY rt.WindowEndDate DESC) = 1
  ),
  vw_alert_patrons AS (
    SELECT
      rt.TransSource, 
      rt.src_property,
      rt.PTNID,
      rt.TransID,
      rt.TransDt,-- Timestamp
      rt.TransTypName,
      rt.QualifiedDebitAmt,
      -- AUDIT Fields
      rt.PAPERREFNUM,
      rt.CHECKNUM,
      --- Rolling 7 Day Window Calculations
      qwc.WindowStartDate,
      qwc.WindowEndDate,
      qwc.WindowTransactionCount,
      qwc.WindowTransactionAmount,
      collect_list(CAST(rt.TransID AS STRING)) OVER (PARTITION BY rt.PTNID) AS TransIDString
    FROM 
      vw_rollingwindow_trans rt
    INNER JOIN qualified_window_calculations qwc
      ON rt.PTNID = qwc.PTNID
      AND CAST(rt.TransDt AS DATE) BETWEEN qwc.WindowStartDate AND qwc.WindowEndDate
  )
  SELECT
    txn.TransSource, 
    txn.src_property,
    txn.PTNID,
    txn.TransID,
    txn.TransDt,-- Timestamp
    txn.TransTypName,
    txn.QualifiedDebitAmt,
    -- AUDIT Fields
    txn.PAPERREFNUM,
    txn.CHECKNUM,
    --- Rolling 7 Day Window Calculations
    txn.WindowStartDate,
    txn.WindowEndDate,
    txn.WindowTransactionCount,
    txn.WindowTransactionAmount,
    sha2(txn.PTNID
          ||'{cfg_db['ruleid']}' 
          || txn.TransSource
          || concat_ws('',sort_array(txn.TransIDString)
    ),256) AS TransMonitoringCaseHashVal,
    mdm.CustomerName,
    '{cfg_db['ruleid']}' AS RuleID
  FROM 
    vw_alert_patrons txn
  LEFT JOIN vw_patron_details mdm 
    ON txn.PTNID = mdm.CustomerNumber
  QUALIFY 
    ROW_NUMBER() OVER(PARTITION BY  txn.src_property,
                                    txn.TransID
                                    ORDER BY txn.WindowEndDate DESC) = 1
    AND WindowEndDate >= to_date('{cfg_db['txn_window']['end']}') - INTERVAL {cfg_rule['days_to_generate_rolling_window']} DAY
  """)

# COMMAND ----------

# MAGIC %md
# MAGIC # Run Rule

# COMMAND ----------

if param_run_tests_mode!='unit_test_only':
  tito_redeemed_df = AML_METRICS.get_metric('tito_redeemed')
  cancel_credit_redeemed_df = AML_METRICS.get_metric('cancel_credit_redeemed')
  eg_jackpot_redeemed_df = AML_METRICS.get_metric('eg_jackpot_redeemed')
  
  rollingwindow_trans_df = get_rollingwindow_trans_df(
    tito_redeemed_df = tito_redeemed_df,
    cancel_credit_redeemed_df = cancel_credit_redeemed_df,
    eg_jackpot_redeemed_df = eg_jackpot_redeemed_df
  )

  df_patron_details = get_patron_details(table_mdm_current_customer) # Get patron Name

  AML_METRICS.cleanup_views() # Drops temp views of the transactional source tables  

  # Perform rule logic - get detected transactions w/ hash
  df_case_transactions = get_rule_case_transactions(
    rollingwindow_trans_df = rollingwindow_trans_df,
    df_patron_details = df_patron_details,
    cfg_rule = rule_param_cfg,
    cfg_db = cfg_db
  )

  # For pre-prod analysis purposes only -> show case transactions. This will also be shown in the case write function, however - that step can be skipped if the results of a run are the only thing desired
  if param_run_tests_mode != 'no':
    df_case_transactions.cache()
    df_case_transactions.display()   

# COMMAND ----------

# MAGIC %md
# MAGIC # Process/Insert New Cases

# COMMAND ----------

if param_run_tests_mode != 'unit_test_only':
  # Set the schema for the json payload to be put into fact_detail.TransAttributeVal
  cfg_db['sql_TransAttributeVal'] = """
    "CustomerNumber",             cast(fd.PTNID as bigint),
    "LegalCustomerName",          cast(fd.CustomerName as string),
    "TransactionType",            cast(fd.TransTypName as string),
    "TransactionDebitAmt",        cast(fd.QualifiedDebitAmt AS double),
    "DebitAmt",                   cast(fd.WindowTransactionAmount as double),
    "TotalCustomerTransactions",  cast(fd.WindowTransactionCount as bigint),
    "PreviousValue",              cast(fd.PAPERREFNUM as string),
    "Change_Attribute",           cast(fd.CHECKNUM as string)
  """
  # INSERT NEW ALERTS -> ALSO SHOW THESE WITH COMMIT_DISPLAY
  process_aml_alerts(df_alert_transactions = df_case_transactions, cfg_db = cfg_db, run_mode = 'COMMIT_DISPLAY')

# COMMAND ----------

# MAGIC %md
# MAGIC # *** NOTEBOOK END ***
