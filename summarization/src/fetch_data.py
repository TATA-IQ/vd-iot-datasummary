import requests
from datetime import datetime

from console_logging.console import Console
console=Console()
class Mongo_Data:
    def get_data(mongo_collection,start_time=None, end_time=None, logger=None):
        logger.info("in mongo data class")
        if start_time and end_time:
            print({"time.UTC_time": {"$gt":start_time,"$lte":end_time}})
            console.info({"time.UTC_time": {"$gt":start_time,"$lte":end_time}})
            # logger.info({"time.UTC_time": {"$gt":start_time,"$lte":end_time}})
            start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
            start_time = int(str(int(start_time.timestamp()))+"000")
            end_time = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
            end_time = int(str(int(end_time.timestamp()))+"000")
            print({"time.timestamp": {"$gt":start_time,"$lte":end_time}})
            console.info({"time.timestamp": {"$gt":start_time,"$lte":end_time}})
            # cursor = mongo_collection.find({"time.UTC_time": {"$gt":start_time,"$lte":end_time}})
            cursor = mongo_collection.find({"time.timestamp": {"$gt":start_time,"$lte":end_time}})
            list_cur = list(cursor)
            return list_cur
        else:
            print({"time.UTC_time": {"$lte":end_time}})
            console.info({"time.UTC_time": {"$lte":end_time}})
            logger.info({"time.UTC_time": {"$lte":end_time}})
            # cursor = mongo_collection.find({"time.UTC_time": {"$lte":end_time}})
            end_time = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
            end_time = int(str(int(end_time.timestamp()))+"000")
            # cursor = mongo_collection.find({"time.UTC_time": {"$gt":start_time,"$lte":end_time}})
            cursor = mongo_collection.find({"time.timestamp": {"$lte":end_time}})
            list_cur = list(cursor)
            return list_cur
        # print("in mongo data")
    # def get_data(mongo_collection,start_time=None, end_time=None):
    #     if start_time and end_time:
    #         print({"time.UTC_time": {"$gt":start_time,"$lte":end_time}})
    #         cursor = mongo_collection.find({"time.UTC_time": {"$gt":start_time,"$lte":end_time}})
    #         list_cur = list(cursor)
    #         return list_cur
    #     else:
    #         print({"time.UTC_time": {"$lte":end_time}})
    #         cursor = mongo_collection.find({"time.UTC_time": {"$lte":end_time}})
    #         list_cur = list(cursor)
    #         return list_cur
class Sql_Data:
    def get_data(url):
        response=requests.get(url)
        result = response.json()['data'][0]
        # print(result)
        return (result['start_time'], result['end_time'])
    
    def update_data(url, start_time, end_time):
        print("updating start and end time in incident_summary_time")
        response=requests.post(url,json={"start_time":start_time,"end_time":end_time})
        print(response.json())
        return response.json()
        
        
        