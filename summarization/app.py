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
import consul

from fastapi import FastAPI
from pydantic import BaseModel
from queue import Queue
# import multiprocessing as mp
# from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

from src.config_parser import Config
from src.createclient import CreateClient
from src.summarization import create_dataframe
from src.fetch_data import Mongo_Data, Sql_Data

from sourcelogs.logger import create_rotating_log
from console_logging.console import Console
console=Console()
os.makedirs("logs", exist_ok=True)
log = create_rotating_log("logs/logs.log")

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
            log.info(f"===got endpoints==={endpoints}")
            console.info(f"===got endpoints==={endpoints}")
            break
        except Exception as ex:
            log.error(f"endpoint exception==>{ex}")
            console.error(f"endpoint exception==>{ex}")
            time.sleep(10)
            continue
    
    while True:
        try:
            res=requests.get(endpoint_addr+endpoints["endpoint"]["datasummary"])
            summaryconf=res.json()
            log.info(f"summaryconf===>{summaryconf}")
            console.info(f"summaryconf===>{summaryconf}")
            break
            

        except Exception as ex:
            log.error(f"summaryconf exception==>{ex}")
            console.error(f"summaryconf exception==>{ex}")
            time.sleep(10)
            continue
    console.info("=======searching for dbapi====")
    while True:
        try:
            log.info("=====consul search====")
            console.info("=====consul search====")
            dbconf=get_service_address(consul_client,"dbapi",consul_conf["env"])
            dbhost=dbconf["ServiceAddress"]
            dbport=dbconf["ServicePort"]
            res=requests.get(endpoint_addr+endpoints["endpoint"]["dbapi"])
            dbres=res.json()
            console.info(f"===got db conf==={ dbres}")
            print(dbres)
            break
        except Exception as ex:
            log.error("db discovery exception==={0}".format(ex))
            console.error("db discovery exception==={0}".format(ex))
            time.sleep(10)
            continue
    for i in dbres["apis"]:
        print("====>",i)
        dbres["apis"][i]="http://"+dbhost+":"+str(dbport)+dbres["apis"][i]

    
    console.info("======dbres======")
    log.info(dbres)
    log.info(summaryconf)
    console.info(dbres)
    console.info(summaryconf)
    return  dbres,summaryconf


def run(config_db,config_summary):
    log.info("started=== in run ")  
    console.info("started=== in run ")
    hour = datetime.now().hour
    log.info(f"summarization started at {hour}th hour")
    console.info(f"summarization started at {hour}th hour")
    # config = Config.yamlconfig("config/config.yaml")[0]
    # config_db,config_summary=get_confdata(config["consul"])
    print("here===========================")
    print("="*100)
    print(f"config_db==={config_db}")
    print("config_summary====",config_summary)
    print("="*100)
    dbconfig=config_summary["db"]
    mongoconfig=config_summary["mongodb"]
    apiconfig=config_db["apis"]

    clientobj = CreateClient(config_summary, log)
    # sql_cnx = clientobj.connection_sql()
    mongo_collection = clientobj.mongo_client()
    start_time, end_time = Sql_Data.get_data(apiconfig["getsummarytime"])
    print(f"from database start time == {start_time} and endtime is {end_time} ")
    log.info(f"from database start time == {start_time} and endtime is {end_time} ")
    console.info(f"from database start time == {start_time} and endtime is {end_time} ")
    if end_time==None:
        log.info(end_time)
        console.info(end_time)
        latest_start_time = datetime.now()-timedelta(days=10,hours=1) ## should be replaced with lowest time in mongo
        latest_end_time = datetime.now().replace(minute=0, second=0,microsecond=0)
    else:
        latest_start_time = datetime.strptime(end_time,'%Y-%m-%dT%H:%M:%S')
        latest_end_time =  datetime.now().replace(minute=0, second=0, microsecond=0)
        
    print(f"==### final===,{latest_start_time}, {latest_end_time}")  
    log.info(f"==### final===,{latest_start_time}, {latest_end_time}")  
    console.info(f"==### final===,{latest_start_time}, {latest_end_time}")  
    print((latest_start_time.replace(minute=0, second=0,microsecond=0)-latest_end_time).total_seconds()) 
    console.info((latest_start_time.replace(minute=0, second=0,microsecond=0)-latest_end_time).total_seconds()) 
    if (latest_end_time-latest_start_time.replace(minute=0, second=0,microsecond=0)).total_seconds() != 0: ##
        try:
            latest_start_time_str = latest_start_time.strftime('%Y-%m-%d %H:%M:%S')
        except:
            latest_start_time_str = None
        latest_end_time_str = latest_end_time.strftime('%Y-%m-%d %H:%M:%S')
        console.info(f"latest_start_time_str, latest_end_time_str before quering {latest_start_time_str} and {latest_end_time_str}")
        log.info(f"latest_start_time_str, latest_end_time_str before quering {latest_start_time_str} and {latest_end_time_str}")
        list_cur = Mongo_Data.get_data(mongo_collection, latest_start_time_str, latest_end_time_str, log)
        print(f"len list cur {len(list_cur)}")
        console.info(f"len list cur {len(list_cur)}")
        log.info(f"len list cur {len(list_cur)}")
            
        print("latest_start_time_str, latest_end_time_str ",latest_start_time_str, latest_end_time_str)
        response = Sql_Data.update_data(apiconfig["updatesummarytime"], latest_start_time_str, latest_end_time_str)
        print(f"=====response=======, {response}")
        console.info(f"=====response=======, {response}")
        log.info(f"=====response=======, {response}")
        # cursor = mongo_collection.find()
        # list_cur = list(cursor)
        if len(list_cur)>0:
            console.info("len of list cur > 0")
            log.info("len of list cur > 0")
            dataframe_obj = create_dataframe()
            df_all = dataframe_obj.convert_mongo_to_db(list_cur) 
            df_final = dataframe_obj.summarization(df_all)
            df_final.to_csv('data/incident_summary1.csv')

            # # # # # creating mysql engine and inserting the data in db
            try:
                clientobj.insert_into_db(df_final)
                log.success(f"===summarization done at==={ datetime.now()}")
                console.success(f"===done==={ datetime.now()}")
            except Exception as ex:
                print(ex)
                console.error(f"couldnt save summarization in db {ex}")
                log.error(f"couldnt save summarization in db {ex}")
                
            # time.sleep(10)
        # df_final.to_csv('data/incident_summary1.csv')
    else:
        console.info(f"start hour and end hour times are same ie., {latest_end_time},{latest_start_time}")
        log.info(f"start hour and end hour times are same ie., {latest_end_time},{latest_start_time}")
    
## thread pool executor
    
def run_thread():
    hour_thread = threading.Thread(target = run)
    hour_thread.start()
    hour_thread.join()

# def schedule_summarization():
#     print("summarization started for hour", datetime.now().hour)
#     schedule.every().hour.at(":05").do(run)
    
def starthourly_summarization(config_db,config_summary):  
    threadexecutor = ThreadPoolExecutor(max_workers=2) 
    while True:
        current_time = datetime.now()
        print(f"current time {current_time} and minute {current_time.minute}")
        log.info(f"current time {current_time} and minute {current_time.minute}")
        console.info(f"current time {current_time} and minute {current_time.minute}")
        # if current_time.second >= 5 and current_time.second <= 10:
        if current_time.minute >= 0 and current_time.minute < 5:     
            summarization_future = threadexecutor.submit(run,config_db,config_summary)
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

# if __name__ == "__main__":
#     # main()
#     starthourly_summarization()
#     # run()
print("started summarization")
config = Config.yamlconfig("config/config.yaml")[0]
config_db,config_summary=get_confdata(config["consul"])
starthourly_summarization(config_db,config_summary)
        
# schedule.every().hour.at(":05").do(run)