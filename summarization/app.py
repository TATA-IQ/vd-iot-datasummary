"""main module"""
from json import dumps
from pymongo import MongoClient
from bson.json_util import dumps
import pandas as pd
from datetime import datetime, timedelta
import pandas as pd
import schedule
import time
import mysql.connector
from sqlalchemy import create_engine
import threading
from concurrent.futures import ThreadPoolExecutor


import os
import requests
import uvicorn
from typing import Union
import mysql.connector

from fastapi import FastAPI
from pydantic import BaseModel
from queue import Queue
# import multiprocessing as mp
# from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

from src.config_parser import Config
from src.createclient import CreateClient
from src.summarization import create_dataframe
from src.fetch_data import Mongo_Data, Sql_Data


def run():
    hour = datetime.now().hour
    print(f"summarization started at {hour}th hour")
    config = Config.yamlconfig("config/config.yaml")[0]
    dbconfig=config["db"]
    mongoconfig=config["mongodb"]
    apiconfig=config["apis"]

    clientobj = CreateClient(config)
    # sql_cnx = clientobj.connection_sql()
    mongo_collection = clientobj.mongo_client()
    start_time, end_time = Sql_Data.get_data(apiconfig["getsummarytime"])
    print(start_time, end_time)
    if end_time==None:
        print(end_time)
        latest_start_time = datetime.now()-timedelta(days=1,hours=1) ## should be replaced with lowest time in mongo
        latest_end_time = datetime.now().replace(minute=0, second=0,microsecond=0)
    else:
        latest_start_time = datetime.strptime(end_time,'%Y-%m-%dT%H:%M:%S')
        latest_end_time =  datetime.now().replace(minute=0, second=0, microsecond=0)
        
    print("==###===",latest_start_time, latest_end_time)  
    print((latest_start_time.replace(minute=0, second=0,microsecond=0)-latest_end_time).total_seconds()) 
    if (latest_end_time-latest_start_time.replace(minute=0, second=0,microsecond=0)).total_seconds() != 0: ##
        try:
            latest_start_time_str = latest_start_time.strftime('%Y-%m-%d %H:%M:%S')
        except:
            latest_start_time_str = None
        latest_end_time_str = latest_end_time.strftime('%Y-%m-%d %H:%M:%S')
        
        list_cur = Mongo_Data.get_data(mongo_collection, latest_start_time_str, latest_end_time_str)
        print(len(list_cur))
            
        print("latest_start_time_str, latest_end_time_str ",latest_start_time_str, latest_end_time_str)
        response = Sql_Data.update_data(apiconfig["updatesummarytime"], latest_start_time_str, latest_end_time_str)
        print("=====response=======", response)
        # cursor = mongo_collection.find()
        # list_cur = list(cursor)
        if len(list_cur)>0:
            dataframe_obj = create_dataframe()
            df_all = dataframe_obj.convert_mongo_to_db(list_cur) 
            df_final = dataframe_obj.summarization(df_all)

            # # # # # creating mysql engine and inserting the data in db
            clientobj.insert_into_db(df_final)
            print("===done===", datetime.now())
            # time.sleep(10)
        # df_final.to_csv('data/incident_summary1.csv')
    
    
## thread pool executor
    
def run_thread():
    hour_thread = threading.Thread(target = run)
    hour_thread.start()
    hour_thread.join()

def schedule_summarization():
    print("summarization started for hour", datetime.now().hour)
    schedule.every().hour.do(run)

# count = 0
# while True:
#     print("not started ", count)
#     schedule.run_pending()
#     time.sleep(1)
#     count +=1
#     if count==3600:
#         count=0
        
# def main():
#     while True:
#         print("summarization started for hour", datetime.now().hour)
#         run_thread()
#         time.sleep(3600)
        

# if __name__ == "__main__":
#     main()
    
# def main(executor):
#     # run()
#     executor.submit(run)
    
    
    
# #     # schedule.every().hour.do(run,threadexecutor) ##
    
# #     # threadexecutor.submit(schedule_summarization)
    
    
# #     #     # print("summarization started for hour", datetime.now().hour)
# #     # with ThreadPoolExecutor(max_workers=2) as executor:
# #     #     summarization_future = executor.submit(schedule_summarization)
        
# #     while True:
# #         schedule.run_pending()
# #         time.sleep(1)
# #         # run_thread()
        
# #         # time.sleep(3600)
        

# if __name__ == "__main__":
#     threadexecutor = ThreadPoolExecutor(max_workers=2)
    
#     # schedule.every().hour.do(main(threadexecutor)) ##
#     schedule.every(10).seconds.do(main(threadexecutor)) ##
    
#     # threadexecutor.submit(schedule_summarization)
    
    
#     #     # print("summarization started for hour", datetime.now().hour)
#     # with ThreadPoolExecutor(max_workers=2) as executor:
#     #     summarization_future = executor.submit(schedule_summarization)
        
#     while True:
#         schedule.run_pending()
#         time.sleep(1)
        # run_thread()
    # main()
    
    
def main():
    executor = ThreadPoolExecutor(max_workers=2)
    # schedule.every().hour.do(executor.submit, run) 
    schedule.every().hour.at(":05").do(executor.submit, run)
    # schedule.every(10).seconds.do(executor.submit, run)

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()
    # run()

# schedule.every().hour.at(":05").do(run)