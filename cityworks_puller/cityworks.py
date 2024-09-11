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
    
    def search_objects(self, token, url, months=None, start_date_text=None, end_date_text=None):
        if start_date_text:
            end = date.today()
            start = end - relativedelta(months=months)
            payload = {
                "token": token,
                "data": json.dumps({start_date_text: start.strftime("%Y-%m-%d"), end_date_text: end.strftime("%Y-%m-%d")})
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
    
    def search_cases(self, token):
        url = f"{self.base_url}/Pll/CaseObject/Search"
        cases = self.search_objects(token, url)
        return cases 

    def search_inspections(self, token, months=1):
        url = f"{self.base_url}/Ams/Inspection/Search"
        inspections = self.search_objects(token, url, months, "InitiateDateBegin", "InitiateDateEnd")
        return inspections 

    def search_work_orders(self, token, months=1):
        url = f"{self.base_url}/Ams/WorkOrder/Search"
        work_orders = self.search_objects(token, url, months, "InitiateDateBegin", "InitiateDateEnd")
        return work_orders 
       
    def search_requests(self, token, months=1):
        url = f"{self.base_url}/Ams/ServiceRequest/Search"
        requests = self.search_objects(token, url, months, "DateTimeInitBegin", "DateTimeInitEnd")
        return requests   
    
    def search_case_addresses(self, token):
        url = f"{self.base_url}/Pll/CaseAddress/SearchObject"
        requests = self.search_objects(token, url)
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
        case_object_ids = self.search_cases(token)
        cases = self.get_cases_by_ids (token, case_object_ids)

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
    
    def get_case_fees_by_id(self, token, case_ids):
        url = f"{self.base_url}/Pll/CaseFees/ByCaObjectId"
        i = 1
        num_cases = len(case_ids)
        fees = []
        for case_id in case_ids:
            payload = {
                "token": token,
                "data": json.dumps({'CaObjectId': case_id})   
            }
            fee_response = self.make_api_call("GET", url, payload)
            if len(fee_response["Value"]) > 0:
                logging.info(f"Case {i} out of {num_cases} has fees")
                fees.extend(fee_response["Value"])
            else:
                logging.info(f"Case {i} out of {num_cases} has no fees")
            i += 1
        logging.info(f"Successfully got case fees from Cityworks")
        return pd.DataFrame(fees)

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
                data.to_csv(path, header=False, index=False)
                return field_names
            else:
                return []
        except:
            raise Exception("Error writing to csv file")
