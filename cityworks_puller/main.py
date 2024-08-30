from cityworks import Cityworks
from config import Config
from config_error import ConfigError
import json
import traceback
import argparse
import logging

parser = argparse.ArgumentParser(
    description='Process inputs')

parser.add_argument('--config', type=str, help='Path to config file')

args = parser.parse_args()

logging.getLogger().setLevel(logging.INFO)

def run(config):

    cityworks = Cityworks(config.login_name, config.password, "https://cityworks.toledo.oh.gov/Cityworks/services")

    token = cityworks.get_access_token()

    report_name = config.report_name
    try:
        months_to_include = int(config.months)
    except:
        raise Exception("Unable to convert number of months to integer")

    if report_name == 'Cases':
        case_object_ids = cityworks.search_cases(token)
        out_data = cityworks.get_cases_by_ids(token, case_object_ids)
    elif report_name == 'Inspections':
        inspection_ids = cityworks.search_inspections(token, months_to_include)
        out_data = cityworks.get_inspections_by_ids(token, inspection_ids)
    elif report_name == 'Work Orders':
        work_order_ids = cityworks.search_work_orders(token, months_to_include)
        out_data = cityworks.get_work_orders_by_ids(token, work_order_ids)
    elif report_name == 'Requests':
        request_ids = cityworks.search_requests(token, months_to_include)
        out_data = cityworks.get_requests_by_ids(token, request_ids)
    elif report_name == 'Case Fees':
        recent_case_ids = cityworks.get_recent_case_ids(token, months_to_include)
        out_data = cityworks.get_case_fees_by_id(token, recent_case_ids)
    elif report_name == 'Inspection Questions': 
        inspection_ids = cityworks.search_inspections(token, months_to_include)
        out_data = cityworks.get_inspection_questions_by_ids(token, inspection_ids)
    
    csv_headers = cityworks.create_csv(out_data, config.data_file_path)
    headers_dict = [{"name": header, "type": "VARCHAR"} for header in csv_headers]

    output_object = {'status': 'ok',
                     'file_name': config.data_file_path, 'columns': headers_dict}
    print('DONE', json.dumps(output_object))


def fail(error):
    result = {
        "status": "error",
        "error": """{}
         {}""".format(str(error), traceback.format_exc())
    }

    output_json = json.dumps(result)
    print('DONE', output_json)


def load_config(file_path):
    raw_config = load_json(file_path)
    # print('RAW CONFIG', raw_config)

    data_file_path = raw_config.get('dataFilePath', None)

    sub_config = raw_config.get('config')
    login_name = sub_config.get('login_name')
    password = sub_config.get('password')
    report_name = sub_config.get('report_name')
    months = sub_config.get('months')

    return Config(data_file_path, login_name, password, report_name, months)


def load_json(file_path):
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
        return data
    except FileNotFoundError:
        True
        print(f"File '{file_path}' not found.")
    except json.JSONDecodeError as e:
        True
        print(f"JSON decoding error: {e}")
    except Exception as e:
        True
        print(f"An error occurred: {e}")

# Main Program
if __name__ == "__main__":
    try:
        config = load_config(args.config)
        run(config)
    except ConfigError as e:
        fail(e)
