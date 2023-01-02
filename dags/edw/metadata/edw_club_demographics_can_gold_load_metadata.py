task_group_metadata = {
  "dev_ops":[
  {
    "IS_ENABLED": 1,
    "STEP_NUMBER": 1,
    "TASKS": [
      {
        "IS_ENABLED": 1,
        "MAX_PROCESS_TIME": 600,
        "RETRY_COUNT": 0,
        "STEP_NUMBER": 1,
        "TASK_GROUP_ID": 276,
        "TASK_ID": 466,
        "TASK_NAME": "edw_club_demographics_can_stg_raw_load",
        "TASK_PARAMETERS": {
          "fn_name": "club_demographics_load_file_data",
          "params": {
            "domain": "tango",
            "drop_location": "pf-dsdm-data-dev-raw/edw/tango/club_demographics_can_stg/",
            "file_read_locations": [
              "/outbound/New_Store_Demographics/processed/PFDemographicsCAN"
            ]
          },
          "tgt_database": "DEV_OPS",
          "tgt_schema": "CNTRL"
        },
        "TASK_TYPE": "api_ingestion"
      }
    ],
    "TASK_GROUP_ID": 276,
    "TASK_GROUP_NAME": "edw_club_demographics_can_raw_load"
  },
  {
    "IS_ENABLED": 0,
    "STEP_NUMBER": 2,
    "TASKS": [
      {
        "IS_ENABLED": 1,
        "MAX_PROCESS_TIME": 600,
        "RETRY_COUNT": 0,
        "STEP_NUMBER": 2,
        "TASK_GROUP_ID": 277,
        "TASK_ID": 467,
        "TASK_NAME": "edw_club_demographics_can_gold_load",
        "TASK_PARAMETERS": {
          "sp_name": "SP_TRANSFORM_TASK",
          "sp_params": {
            "kwargs": "{'start_date':'', 'end_date':'', 'rolling_window':''}",
            "load_type": "",
            "sid_columns": "",
            "sp_name": "SP_EDW_CLUB_DEMOGRAPHICS_CAN_STG_LOAD",
            "src_db": "DEV_RAW",
            "src_schema": "EDW",
            "src_tbl": "CLUB_DEMOGRAPHICS_CAN_STG",
            "stg_db": "DEV_STAGE",
            "stg_schema": "EDW",
            "tgt_db": "DEV_GOLD",
            "tgt_schema": "EDW",
            "tgt_tbl": "CLUB_DEMOGRAPHICS_CAN_STG"
          },
          "tgt_database": "DEV_OPS",
          "tgt_schema": "CNTRL"
        },
        "TASK_TYPE": "sp_pipe_transform"
      }
    ],
    "TASK_GROUP_ID": 277,
    "TASK_GROUP_NAME": "edw_club_demographics_can_gold_load"
  }
]}