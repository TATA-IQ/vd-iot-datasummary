# from minio import Minio
# from minio.error import S3Error
from pymongo import MongoClient
import mysql.connector
from sqlalchemy import create_engine
import urllib
from console_logging.console import Console
console=Console()

class CreateClient():
    def __init__(self, config, logger):
        print(f"====in create client====={config}")
        self.config=config
        self.dbconfig = self.config['db']
        self.mongodbconf = self.config['mongodb']
        self.log = logger
    
    def connection_sql(self,):
        console.info("======creating mysql connection ======")
        self.log.info("======creating mysql connection ======")
        cnx = mysql.connector.connect(
        user=self.dbconfig["username"],
        password=self.dbconfig["password"],
        host=self.dbconfig["host"],
        database=self.dbconfig["db"],
        )
        return cnx
  
    def mongo_client(self):
        console.info("======creating mongo client ======")
        self.log.info("======creating mongo client ======")
        if "username" not in self.mongodbconf or "password" not in self.mongodbconf :
            mongo_client = MongoClient(
            host=self.mongodbconf["host"], port=self.mongodbconf["port"], 
            connect=self.mongodbconf["connect"]
        )


        elif self.mongodbconf['username'] and self.mongodbconf['password']:
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
        self.log.info("collection created")
        console.info("collection created")

        return collection
    
    def create_sql_engine(self,):
        console.info("===== creating mysql engine ======")
        self.log.info("===== creating mysql engine ======")
        password = self.dbconfig["password"]
        updated_password = urllib.parse.quote_plus(password)
        console.info(f"password: {password}")
        self.log.info(f"password: {password}")
        console.info(f"updated_password: {updated_password}")
        self.log.info(f"updated_password: {updated_password}")

        engine=create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=self.dbconfig["host"], port = str(self.dbconfig["port"]), db=self.dbconfig["db"], user=self.dbconfig["username"], pw=updated_password))
        return engine
    
    def insert_into_db(self,df):
        console.info(f"===== inside into insert into db, {self.dbconfig['tablename']} =====")
        self.log.info(f"===== inside into insert into db {self.dbconfig['tablename']}=====")
        engine = self.create_sql_engine()
        print(df.head())
        df.to_sql(self.dbconfig["tablename"], engine, if_exists='append', index=False)
        console.success(f"inserted into db {self.dbconfig['tablename']}")