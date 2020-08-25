import requests, json
from texttable import Texttable
import ConfigParser


config = ConfigParser.ConfigParser()
config.read('api_token.cfg')
api_token = config('Token','api_token')
api_url_base = "https://api.datorama.com"
brand_id = '70157'

header = {'Authorization': api_token, 'Content-Type': 'application/json; charset=UTF-8'}

data_stream_attributes_list = ['#', 'id','name', "dataSourceName","dataLoadMode", "lastRunStatus","lastDataDate","retrievalPeriodHour", "retrievalPeriodType", "lastRowsRetrieved", "templateId"]
data_stream_log_attributes_list = ['#',"id" , "status",  "dataStartDate", "dataEndDate", "startExecutionTime", "endExecutionTime"]
data_stream_log_attributes_dtype = ['t',"t" , "t",  "t", "t", "t", "t"]


def get_account_workspaces():
    response = requests.get('{0}/v1/workspaces'.format(api_url_base), headers=header)
    if response.status_code == 200:
        request_data = json.loads(response.content.decode('utf-8'))
        account_info_list = []
        account_info_list.append(['id','name'])
        account_info= []
        for record in request_data:
            account_info.append(record["id"])
            account_info.append(record["name"])
            account_info_list.append(account_info)
            account_info = []
        return account_info_list
    else:
        return None


def get_data_streams_list(workspace_id, data_stream_id = 0):
    data_stream_url = ''
    if data_stream_id != 0:
        data_stream_url = "/{0}".format(data_stream_id)
    api_endpoint = '{0}/v1/workspaces/{1}/data-streams{2}'.format(api_url_base, workspace_id, data_stream_url)
    print("URL:{0}".format(api_endpoint))
    response = requests.get(api_endpoint, headers=header)
    if response.status_code == 200:
        response_data = json.loads(response.content.decode('utf-8'))
        data_streams_list = [data_stream_attributes_list]
        data_stream = []
        ctr = 0
        if type(response_data) is not list:
            response_data = [response_data]
        for record in response_data:
                ctr += 1
                data_stream.append(ctr)
                for attribute in data_stream_attributes_list[1:]:
                    data_stream.append(record[attribute])
                data_streams_list.append(data_stream)
                data_stream = []
        return data_streams_list
    else:
        return None


def get_latest_log(data_stream_id):
    body = {"pageSize": 10}
    api_endpoint = '{0}/v1/data-streams/{1}/stats'.format(api_url_base, data_stream_id)
    print("URL:{0}".format(api_endpoint))
    response = requests.post(api_endpoint, headers=header, json=body)
    if response.status_code == 200:
        response_data = json.loads(response.content.decode('utf-8'))
        jobs_list = [data_stream_log_attributes_list]
        job = []
        ctr = 0
        for record in response_data:
                ctr += 1
                job.append(ctr)
                for attribute in data_stream_log_attributes_list[1:]:
                    job.append(record[attribute])
                jobs_list.append(job)
                job = []
        return jobs_list
    else:
        return None

def print_list_with_header(list_name_2d):
    t = Texttable()
    t.set_cols_dtype(data_stream_log_attributes_dtype)
    t.add_rows(list_name_2d)
    print(t.draw())

if __name__ == "__main__":
    #print_workspace_info()
    #print_list_with_header(get_data_streams_list(70157, 2698170))
    print_list_with_header(get_latest_log(2698170))