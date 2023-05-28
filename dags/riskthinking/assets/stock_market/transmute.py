import os
import pandas as pd
import polars as pl
from riskthinking.assets.stock_market.extraction.kaggle_api import KaggleDataset
from riskthinking.assets.stock_market.read.read_local import PolarsReadRaw, DeltaReadTable
from riskthinking.assets.stock_market.mutate.mutate_stock_market import mutate_stock_market
from riskthinking.assets.stock_market.mutate.mutate_feature_engineering import mutate_feature_engineering
from riskthinking.assets.stock_market.mutate.mutate_integrate_ml import mutate_integrate_ml
from riskthinking.assets.stock_market.spawn.spawn_local import DeltaWrite, ModelWrite

def job_extract_dataset_metadata(dataset_name: str) -> dict:

    return KaggleDataset(dataset_name).Metadata()

def job_extract_dataset(dataset_name: str, path: str) -> dict:

    return KaggleDataset(dataset_name).Download(path)
    
def job_stock_market_data_process(paths: dict) -> dict:
    
    #reading files
    file_paths_etfs = [os.path.join(paths["etfs"], filename) for filename in os.listdir(paths["etfs"]) if filename.endswith('.csv')]
    file_paths_stocks = [os.path.join(paths["stocks"], filename) for filename in os.listdir(paths["stocks"]) if filename.endswith('.csv')]
    
    pl_df_stock_market = PolarsReadRaw(paths["main_path"]).MultipleCsvFilesFileName(file_paths_etfs + file_paths_stocks,2)
    pl_df_symbols_valid_meta = PolarsReadRaw(paths["main_path"]).ReadCsvFileFileName(paths["symbols_valid_meta"])
    
    #Defining the schema, merge and transformations function mutate
    pl_df_stock_market = mutate_stock_market(pl_df_stock_market, pl_df_symbols_valid_meta)

    status = DeltaWrite(paths["table_path_write"], pl_df_stock_market).WithoutPartitionOverwrite()

    return status

def job_feature_engineering(paths: dict) -> dict:
    
    df_feature_engineering = DeltaReadTable(paths["table_path_read"]).PandasFormat()

    df_feature_engineering = mutate_feature_engineering(df_feature_engineering)
    status = DeltaWrite(paths["table_path_write"], pl.from_pandas(df_feature_engineering)).WithoutPartitionOverwrite()

    return status

def job_integrate_ml(paths: dict) -> dict:
    
    df_integrate_ml = DeltaReadTable(paths["table_path_read"]).PandasFormat()
    
    model, logs = mutate_integrate_ml(df_integrate_ml)

    status_model = ModelWrite(model, logs).DumpPickle(paths["model_path_write"],paths["file_name_model"])
    status_log = ModelWrite(model, logs).TrainingMetrics(paths["model_path_write"],paths["file_name_log"])

    return {"Status Model": status_model, "Status Log": status_log}


