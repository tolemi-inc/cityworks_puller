import requests
import logging
import json
import csv
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
import sys
import time
import pandas as pd

class Cityworks:
    def __init__(self, login_name, password, base_url):
        self.login_name = login_name
        self.password = password
        self.base_url = base_url

    def make_api_call(self, method, url, payload=None):
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
        }

        try:
            if payload:
                response = requests.request(method, url, headers=headers, data=payload)
            else:
                response = requests.request(method, url, headers=headers)

            logging.info(
                "Response: " + str(response.status_code) + ", " + response.reason
            )

            if response.status_code == 200:
                return response.json()
            
            elif response.status_code == 503:
                time.sleep(5)
                self.make_api_call(self, method, url, payload)
            
            else:
                logging.error("Api request returned a non-200 response")
                logging.error(response.json())
                raise Exception("Error making api request")

        except:
            raise Exception("Error making api request")

    def get_access_token(self):
        url = f"{self.base_url}/General/Authentication/Authenticate"
        payload = {"data": json.dumps({
                "LoginName": self.login_name,
                "Password": self.password
            })
        }

        response = self.make_api_call("POST", url, payload)
        logging.info("Successfully got access token")

        return response["Value"]["Token"]
    
    def search_objects(self, token, url, filter_criteria=None):
        if filter_criteria:
            payload = {
                "token": token,
                "data": json.dumps(filter_criteria)
            }
        else:
            payload = {
                "token": token
            }        
        
        response = self.make_api_call("GET", url, payload)

        if len(response["Value"]) == 200000:
            logging.error("Too many records. Pick a smaller window of months")
            sys.exit()

        logging.info(f"Successfully searched for objects from the following endpoint: {url}")

        return response["Value"]
    
    def generate_date_filter_criteria(self, months, start_date_text, end_date_text):
        end = date.today()
        start = end - relativedelta(months=months)
        return {start_date_text: start.strftime("%Y-%m-%d"), end_date_text: end.strftime("%Y-%m-%d")}
    
    def search_cases(self, token, filter_criteria=None):
        url = f"{self.base_url}/Pll/CaseObject/Search"
        cases = self.search_objects(token, url, filter_criteria)
        return cases 

    def search_inspections(self, token, months=1):
        url = f"{self.base_url}/Ams/Inspection/Search"
        date_filter = self.generate_date_filter_criteria(months, "InitiateDateBegin", "InitiateDateEnd")
        inspections = self.search_objects(token, url, date_filter)
        return inspections 

    def search_work_orders(self, token, months=1):
        url = f"{self.base_url}/Ams/WorkOrder/Search"
        date_filter = self.generate_date_filter_criteria(months, "InitiateDateBegin", "InitiateDateEnd")
        work_orders = self.search_objects(token, url, date_filter)
        return work_orders 
       
    def search_requests(self, token, months=1):
        url = f"{self.base_url}/Ams/ServiceRequest/Search"
        date_filter = self.generate_date_filter_criteria(months, "DateTimeInitBegin", "DateTimeInitEnd")
        requests = self.search_objects(token, url, date_filter)
        return requests   
    
    def search_case_addresses(self, token):
        url = f"{self.base_url}/Pll/CaseAddress/SearchObject"
        requests = self.search_objects(token, url)
        return requests 
    
    def search_case_tasks(self, token):
        url = f"{self.base_url}/Pll/CaseTask/Search"
        filter_criteria = {'TaskComplete': False}
        requests = self.search_objects(token, url, filter_criteria)
        print(len(requests))
        return requests 

    def get_object_by_ids(self, token, url, ids, id_name, batch_size=500):
        objects = []
        for i in range(0, len(ids), batch_size):
            batch_ids = ids[i:i+batch_size]
            payload = {
                "token": token,
                "data": json.dumps({id_name: batch_ids})   
            }
            response = self.make_api_call("GET", url, payload)
            objects.extend(response["Value"])
            logging.info(f"{i+len(batch_ids)} out of {len(ids)} objects retrieved successfully")
        
        logging.info(f"Successfully got objects from {id_name}")
        return pd.DataFrame(objects)

    def get_cases_by_ids(self, token, ids):
        url = f"{self.base_url}/Pll/CaseObject/ByIds"
        cases = self.get_object_by_ids(token, url, ids, "CaObjectIds")
        return cases
    
    def get_recent_case_ids(self, token, months):
        code_enforcement_case_types = ['CE-CASE', 'CE-COURT']
        all_case_object_ids = []
        for case_type in code_enforcement_case_types:
            filter_criteria = {'CaseType': case_type}
            case_object_ids = self.search_cases(token, filter_criteria)
            all_case_object_ids.extend(case_object_ids)

        cases = self.get_cases_by_ids(token, all_case_object_ids)

        cutoff = pd.Timestamp(date.today() - relativedelta(months=months))
        cases['DateModified'] = pd.to_datetime(cases['DateModified'], format='%Y-%m-%dT%H:%M:%SZ', errors='coerce')
        cases['DateEntered'] = pd.to_datetime(cases['DateEntered'], format='%Y-%m-%dT%H:%M:%SZ', errors='coerce')
    
        recent_cases = cases[
            (cases['DateModified'] >= cutoff) | 
            (cases['DateModified'].isna() & (cases['DateEntered'] >= cutoff))
        ]

        return recent_cases['CaObjectId'].tolist()

    def get_inspections_by_ids(self, token, ids):
        url = f"{self.base_url}/Ams/Inspection/ByIds"
        inspections = self.get_object_by_ids(token, url, ids, "InspectionIds")
        return inspections
    
    def get_work_orders_by_ids(self, token, ids):
        url = f"{self.base_url}/Ams/WorkOrder/ByIds"
        work_orders = self.get_object_by_ids(token, url, ids, "WorkOrderIds")
        return work_orders
    
    def get_requests_by_ids(self, token, ids):
        url = f"{self.base_url}/Ams/ServiceRequest/ByIds"
        requests = self.get_object_by_ids(token, url, ids, "RequestIds")
        return requests
    
    def get_inspection_questions_by_ids(self, token, ids):
        url = f"{self.base_url}/Ams/Inspection/Questions"
        requests = self.get_object_by_ids(token, url, ids, "InspectionIds")
        return requests
    
    def get_related_object_by_case_id(self, object_type, token, case_ids):
        url = f"{self.base_url}/Pll/{object_type}/ByCaObjectId"
        i = 1
        num_cases = len(case_ids)
        related_objects = []
        for case_id in case_ids:
            payload = {
                "token": token,
                "data": json.dumps({'CaObjectId': case_id})   
            }
            response = self.make_api_call("GET", url, payload)
            if response["Value"] == None or len(response["Value"]) == 0:
                logging.info(f"Case {i} out of {num_cases} has no {object_type}")
            else:
                logging.info(f"Case {i} out of {num_cases} has {object_type}")
                related_objects.extend(response["Value"])
            i += 1
        logging.info(f"Successfully got {object_type} from Cityworks")
        return pd.DataFrame(related_objects)
    
    def get_case_fees_by_id(self, token, case_ids):
        return self.get_related_object_by_case_id("CaseFees", token, case_ids)
    
    def get_case_payments_by_id(self, token, case_ids):
        return self.get_related_object_by_case_id("CasePayment", token, case_ids)

    def get_case_tasks_by_id(self, token, case_ids):
        return self.get_related_object_by_case_id("CaseTask", token, case_ids)
    
    def get_task_corrections_by_id(self, token, task_ids):
        url = f"{self.base_url}/Pll/CaseCorrections/ByCaTaskIds"
        task_ids = pd.read_csv('case_task_ids.csv')
        corrections = self.get_object_by_ids(token, url, task_ids['cataskid'].tolist(), "CaTaskIds")
        return corrections

    def get_cases_with_addresses(self, token):
        case_addresses = pd.DataFrame(self.search_case_addresses(token))
        case_addresses = case_addresses[['CaObjectId', 'CaseNumber', 'Location']]

        case_object_ids = self.search_cases(token)
        cases = pd.DataFrame(self.get_cases_by_ids(token, case_object_ids))

        cases_with_addresses = pd.merge(cases, case_addresses, on='CaObjectId', how='left')
        return cases_with_addresses

    def create_csv(self, data, path):
        try:
            if len(data) > 0:
                field_names = data.columns.tolist()
                data.to_csv(path, header=True, index=False)
                return field_names
            else:
                return []
        except:
            raise Exception("Error writing to csv file")
