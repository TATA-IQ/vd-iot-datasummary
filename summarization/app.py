"""main module"""
from json import dumps
from pymongo import MongoClient
from bson.json_util import dumps
import pandas as pd
from datetime import datetime, timedelta
import pandas as pd
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

def future_callback_error_logger(future):
    e = future.exception()
    print("Thread pool exception====>", e)


def get_service_address(consul_client,service_name,env):
    while True:
        
        try:
            services=consul_client.catalog.service(service_name)[1]
            print(services)
            for i in services:
                if env == i["ServiceID"].split("-")[-1]:
                    return i
        except:
            time.sleep(10)
            continue



def get_confdata(consul_conf):
    consul_client = consul.Consul(host=consul_conf["host"],port=consul_conf["port"])
    pipelineconf=get_service_address(consul_client,"pipelineconfig",consul_conf["env"])

    summaryconf=None
    dbconf=None
    
    env=consul_conf["env"]
    
    endpoint_addr="http://"+pipelineconf["ServiceAddress"]+":"+str(pipelineconf["ServicePort"])
    print("endpoint addr====",endpoint_addr)
    while True:
        
        try:
            res=requests.get(endpoint_addr+"/")
            endpoints=res.json()
            print("===got endpoints===",endpoints)
            break
        except Exception as ex:
            print("endpoint exception==>",ex)
            time.sleep(10)
            continue
    
    while True:
        try:
            res=requests.get(endpoint_addr+endpoints["endpoint"]["datasummary"])
            summaryconf=res.json()
            print("containerconf===>",containerconf)
            break
            

        except Exception as ex:
            print("containerconf exception==>",ex)
            time.sleep(10)
            continue
    print("=======searching for dbapi====")
    while True:
        try:
            print("=====consul search====")
            dbconf=get_service_address(consul_client,"dbapi",consul_conf["env"])
            print("****",dbconf)
            dbhost=dbconf["ServiceAddress"]
            dbport=dbconf["ServicePort"]
            res=requests.get(endpoint_addr+endpoints["endpoint"]["dbapi"])
            dbres=res.json()
            print("===got db conf===")
            print(dbres)
            break
        except Exception as ex:
            print("db discovery exception===",ex)
            time.sleep(10)
            continue
    for i in dbres["apis"]:
        print("====>",i)
        dbres["apis"][i]="http://"+dbhost+":"+str(dbport)+dbres["apis"][i]

    
    print("======dbres======")
    print(dbres)
    print(containerconf)
    return  dbres,containerconf


def run():
    hour = datetime.now().hour
    print(f"summarization started at {hour}th hour")
    config = Config.yamlconfig("config/config.yaml")[0]
    config_db,config_summary=get_confdata(config["consul"])
    dbconfig=config_summary["db"]
    mongoconfig=config_summary["mongodb"]
    apiconfig=config_db["apis"]

    clientobj = CreateClient(config_db)
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
        print("len list cur ",len(list_cur))
            
        print("latest_start_time_str, latest_end_time_str ",latest_start_time_str, latest_end_time_str)
        response = Sql_Data.update_data(apiconfig["updatesummarytime"], latest_start_time_str, latest_end_time_str)
        print("=====response=======", response)
        # cursor = mongo_collection.find()
        # list_cur = list(cursor)
        if len(list_cur)>0:
            print("len of list cur > 0")
            dataframe_obj = create_dataframe()
            df_all = dataframe_obj.convert_mongo_to_db(list_cur) 
            df_final = dataframe_obj.summarization(df_all)
            df_final.to_csv('data/incident_summary1.csv')

            # # # # # creating mysql engine and inserting the data in db
            try:
                clientobj.insert_into_db(df_final)
                print("===done===", datetime.now())
            except Exception as e:
                print(e)
                print("couldnt save summarization in db")
                
            # time.sleep(10)
        # df_final.to_csv('data/incident_summary1.csv')
    
    
## thread pool executor
    
def run_thread():
    hour_thread = threading.Thread(target = run)
    hour_thread.start()
    hour_thread.join()

def schedule_summarization():
    print("summarization started for hour", datetime.now().hour)
    schedule.every().hour.at(":05").do(run)
    
def starthourly_summarization():  
    threadexecutor = ThreadPoolExecutor(max_workers=2) 
    while True:
        current_time = datetime.now()
        print(f"current time {current_time} and minute {current_time.minute}")
        # if current_time.second >= 5 and current_time.second <= 10:
        if current_time.minute >= 5 and current_time.minute <= 10:     
            summarization_future = threadexecutor.submit(run)
            summarization_future.add_done_callback(future_callback_error_logger)
                            
        time.sleep(300)
        # time.sleep(5)

        
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
    
    
# def main():
#     executor = ThreadPoolExecutor(max_workers=2)
#     # schedule.every().hour.do(executor.submit, run) 
#     schedule.every().hour.at(":05").do(executor.submit, run)
#     # schedule.every(10).seconds.do(executor.submit, run)

#     while True:
#         schedule.run_pending()
#         time.sleep(1)

if __name__ == "__main__":
    # main()
    starthourly_summarization()
    # run()

# schedule.every().hour.at(":05").do(run)