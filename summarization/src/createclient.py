# from minio import Minio
# from minio.error import S3Error
from pymongo import MongoClient
import mysql.connector
from sqlalchemy import create_engine

class CreateClient():
    def __init__(self,config):
        print("====in create client=====")
        self.config=config
        self.dbconfig = self.config['db']
        self.mongodbconf = self.config['mongodb']
    
    def connection_sql(self,):
        print("======creating mysql connection ======")
        cnx = mysql.connector.connect(
        user=self.dbconfig["username"],
        password=self.dbconfig["password"],
        host=self.dbconfig["host"],
        database=self.dbconfig["db"],
        )
        return cnx
  
    def mongo_client(self):
        print("======creating mongo client ======")
        if self.mongodbconf['username'] and self.mongodbconf['password']:
            mongo_client = MongoClient(
                host=self.mongodbconf["host"], port=self.mongodbconf["port"], 
                username=self.mongodbconf["username"],
                password=self.mongodbconf["password"],
                connect=self.mongodbconf["connect"],
                authSource=self.mongodbconf["database"]
            )
            
        else:
            mongo_client = MongoClient(
            host=self.mongodbconf["host"], port=self.mongodbconf["port"], 
            connect=self.mongodbconf["connect"]
        )
        # mongo_client = MongoClient(host = self.mongodbconf['host'], 
        #                             port = self.mongodbconf['port'],
        #                             connect=self.mongodbconf['connect'])

        database = mongo_client[self.mongodbconf['database']]
        collection  = database[self.mongodbconf['collection']]

        return collection
    
    def create_sql_engine(self,):
        print("===== creating mysql engine ======")
        engine=create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=self.dbconfig["host"], port = str(self.dbconfig["port"]), db=self.dbconfig["db"], user=self.dbconfig["username"], pw=self.dbconfig["password"]))
        return engine
    
    def insert_into_db(self,df):
        print("===== inside into insert into db =====")
        engine = self.create_sql_engine()
        print(df.head())
        df.to_sql(self.dbconfig["tablename"], engine, if_exists='append', index=False)
        