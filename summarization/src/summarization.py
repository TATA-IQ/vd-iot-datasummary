import pandas as pd
from datetime import datetime

class create_dataframe:
    def __init__(self,):
        return
        
    # def convert_mongo_to_db(data):
    #     camera_id=data["hierarchy"]["camera_id"], 
    #     camera_name=data["hierarchy"]["camera_name"], 
    #     zone_id=data["hierarchy"]["zone_id"], 
    #     zone_name=data["hierarchy"]["zone_name"],
    #     subsite_id=data["hierarchy"]["subsite_id"], 
    #     subsite_name=data["hierarchy"]["subsite_name"],  
    #     city_id=data["hierarchy"]["city_id"], 
    #     city_name=data["hierarchy"]["city_name"], 
    #     location_id=data["hierarchy"]["location_id"], 
    #     location_name=data["hierarchy"]["location_name"],
    #     customer_id=data["hierarchy"]["customer_id"], 
    #     customer_name=data["hierarchy"]["customer_name"], 
    #     usecase_id = data["usecase"]["id"], 
    #     usecase_name = data["usecase"]["name"], 
    #     incident_date_local = data["time"]["incident_time"][0:10], 
    #     incident_hour_local = data["time"]["incident_time"][11:13],
    #     incident_date_gmt=data["time"]["UTC_time"][0:10], 
    #     incident_hour_gmt=data["time"]["UTC_time"][11:13]
    #     db = None
    #     df_all = pd.DataFrame(columns =['camera_id', 'camera_name', 'zone_id', 'zone_name', 'subsite_id', 'subsite_name',
    #                                    'location_id', 'location_name', 'customer_id', 'customer_name', 'usecase_id', 'usecase_name',
    #                                     'incident_date_local', 'incident_hour_local', 'incident_date_gmt', 'incident_hour_gmt',
    #                                     'incident_id', 'incident_name', 'incident_value', 'incident_label'])
    #     return db  
    
    def incidentlist(self, masters, incidents):
        final_list = []
        for w in incidents:
            incident = []
            if "misc" in w:
                d = 0
                t = 0
                for data_lbl in w["misc"]:
                    print(data_lbl)
                    if("data" in data_lbl): ## text in number plate 
                        try:
                            d=d+int(data_lbl["data"])
                        except ValueError:
                            continue
                    if("text" in data_lbl):
                        t+=1 
                temp1 = [w["incident_id"], w["name"], d, t]
                temp = masters + temp1 
            else:
                temp1 = [w["incident_id"], w["name"], 0, 0]
                temp = masters + temp1
            final_list.append(temp)
        return final_list

  
    
    def convert_mongo_to_db(self,list_cur):
        print("in convert_mongo_to_db")
        df_all = pd.DataFrame(columns =['camera_id', 'camera_name', 'zone_id', 'zone_name', 'subsite_id', 'subsite_name',
                                       'location_id', 'location_name', 'customer_id', 'customer_name', 'usecase_id', 'usecase_name',
                                        'incident_date_local', 'incident_hour_local', 'incident_date_gmt', 'incident_hour_gmt',
                                        'incident_id', 'incident_name', 'incident_value', 'incident_label'])
        for data in list_cur:
            masters = [data["hierarchy"]["camera_id"], data["hierarchy"]["camera_name"], data["hierarchy"]["zone_id"], data["hierarchy"]["zone_name"],
                    data["hierarchy"]["subsite_id"], data["hierarchy"]["subsite_name"],  data["hierarchy"]["city_id"], data["hierarchy"]["city_name"], data["hierarchy"]["location_id"], data["hierarchy"]["location_name"],
                    data["hierarchy"]["customer_id"], data["hierarchy"]["customer_name"], data["usecase"]["usecase_id"], data["usecase"]["name"], 
                    data["time"]["incident_time"][0:10], data["time"]["incident_time"][11:13], data["time"]["UTC_time"][0:10], data["time"]["UTC_time"][11:13]]
            final_list = self.incidentlist(masters, data['incident'])
            df = pd.DataFrame(final_list, columns =['camera_id', 'camera_name', 'zone_id', 'zone_name', 'subsite_id', 'subsite_name', 'city_id', 'city_name',
                                            'location_id', 'location_name', 'customer_id', 'customer_name', 'usecase_id', 'usecase_name',
                                                'incident_date_local', 'incident_hour_local', 'incident_date_gmt', 'incident_hour_gmt',
                                                'incident_id', 'incident_name', 'incident_value', 'incident_label'])
            df_all=pd.concat([df_all, df])
        print("completed convert_mongo_to_db")
        df_all.to_csv("df_all.csv")
        return df_all
    @staticmethod
    def summarization(df_all):
        print("in summarization")
        summary = df_all.groupby(['camera_id', 'camera_name', 'zone_id', 'zone_name', 'subsite_id', 'subsite_name', 'city_id','city_name',
                    'location_id', 'location_name', 'customer_id', 'customer_name', 'usecase_id', 'usecase_name',
                    'incident_date_local', 'incident_hour_local', 'incident_date_gmt', 'incident_hour_gmt',
                    'incident_id', 'incident_name']).agg({'incident_id': ['count'], 'incident_label':  ['count'], 'incident_value':['sum','min', 'max', 'mean']})

        s = summary.reset_index()

        s.columns = ['camera_id', 'camera_name', 'zone_id', 'zone_name', 'subsite_id', 'subsite_name', 'city_id','city_name',
                            'location_id', 'location_name', 'customer_id', 'customer_name', 'usecase_id', 'usecase_name',
                            'incident_date_local', 'incident_hour_local', 'incident_date_gmt', 'incident_hour_gmt',
                            'incident_id', 'incident_name', 'incident_count','incident_label_count','incident_val_sum', 'incident_val_min','incident_val_max', 'incident_val_avg']

        s.head()

        ct = str(int(datetime.now().timestamp()))+'000' #datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        s[["created_by", "created_date", "deleted_by", "deleted_date", "is_deleted", "modified_by", "modified_date"]] = 1, ct, 1, ct, 0, 1, ct 

        df_final = s[['created_by', 'created_date', 'deleted_by', 'deleted_date', 'is_deleted', 'modified_by', 'modified_date', 'incident_count', 'incident_date_gmt', 'incident_date_local', 'incident_hour_gmt', 'incident_hour_local', 'incident_label_count', 'incident_val_avg', 'incident_val_max', 'incident_val_min', 'incident_val_sum', 'camera_id', 'city_id', 'customer_id', 'incident_id', 'location_id', 'subsite_id', 'usecase_id', 'zone_id']]
        print("completed summarization")
        return df_final