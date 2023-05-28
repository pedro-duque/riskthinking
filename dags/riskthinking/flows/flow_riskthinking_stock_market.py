from airflow.decorators import dag, task
import pendulum
import datetime
from airflow.models.baseoperator import chain
from pathlib import Path
import os
import sys

sys.path.insert(0, str(Path(os.path.abspath(__file__)).parents[2]))

@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2023, 5, 24, tz="UTC"),
    catchup=False,
    tags=["Kaggle", "Stock Market Data"],
    default_args={
        "owner": "Pedro Duque", 
        "retries": 1, 
        "retry_delay": datetime.timedelta(minutes=5)
        },

)

def flow_riskthinking_stock_market():
    
    from riskthinking.assets.stock_market.transmute import (
        job_extract_dataset_metadata, 
        job_extract_dataset, 
        job_stock_market_data_process, 
        job_feature_engineering,
        job_integrate_ml
    )
    
    @task
    def job_extract_dataset_metadata_op(**kwargs):
        
        metadata = job_extract_dataset_metadata("jacksoncrow/stock-market-dataset")

        #return metadata

    @task
    def job_extract_dataset_op(**kwargs):
        
        status = job_extract_dataset("jacksoncrow/stock-market-dataset", "data/landing/stock_market")
        print(status)
        return status

    @task
    def job_stock_market_data_process_op(**kwargs):

        status = job_stock_market_data_process({"main_path": "data/landing/stock_market", 
                            "etfs": "data/landing/stock_market/etfs",
                            "stocks": "data/landing/stock_market/stocks",
                            "symbols_valid_meta": "data/landing/stock_market/symbols_valid_meta.csv",
                            "table_path_write": "data/raw/stock_market"
                        })

        return status

    @task
    def job_feature_engineering_op(**kwargs):

        status = job_feature_engineering({
                    "table_path_read": "data/raw/stock_market",
                    "table_path_write": "data/stage/stock_market"
                })

        return status

    @task
    def job_integrate_ml_op(**kwargs):

        status = job_integrate_ml({
                "table_path_read": "data/stage/stock_market",
                "model_path_write": "data/model/stock_market",
                "file_name_model": "stock_market.pkl",
                "file_name_log": "log_stock_market.json"
            })

        return status

    chain(
        job_extract_dataset_metadata_op(), 
        job_extract_dataset_op(), 
        job_stock_market_data_process_op(), 
        job_feature_engineering_op(), 
        job_integrate_ml_op()
        )

dag = flow_riskthinking_stock_market()