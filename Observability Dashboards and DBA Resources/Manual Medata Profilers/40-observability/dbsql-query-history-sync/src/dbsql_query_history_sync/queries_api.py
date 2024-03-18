import requests
import datetime, dateutil
import sys, os
import json
import time

def sync_query_history(dbx_token, workspace_url, warehouse_ids, start_ts_ms, 
                      query_sink_fn, sink_batch_size,
                      end_ts_ms=None, user_ids=None, statuses=None, 
                      stop_fetch_limit=2147483647):
    '''Pull the query history from the API and call query_sink_fn.
    query_sink_fn: for every query_batch_size queries, push them into this sink and
    assume that the sink does the right thing with them.
    Note that the batch size will not be exactly respected. I.e. as soon as the accumulated
    queries go over the query_batch_size, query_sink_fn will be called.
    '''
    print(f'sync_query_history({locals()})')
    #workspace_url = "e2-demo-field-eng.cloud.databricks.com"
    uri = f"https://{workspace_url}/api/2.0/sql/history/queries"
    print(uri)
    headers_auth = {"Authorization":f"Bearer {dbx_token}"}
    request_dict = {}
    request_dict.update({"filter_by":{"warehouse_ids": warehouse_ids}})
    time_filter = {"start_time_ms": start_ts_ms}
    if end_ts_ms:
      time_filter.update({'end_time_ms': end_ts_ms})
    request_dict['filter_by'].update({"query_start_time_range": time_filter})
    if statuses:
      request_dict['filter_by'].update({"statuses": statuses})
    if user_ids:
      request_dict['filter_by'].update({"user_ids": user_ids})
    max_single_call_results = min(sink_batch_size, 1000, stop_fetch_limit)
    request_dict.update({'include_metrics': "true", "max_results": f"{max_single_call_results}"})

    ## Convert dict to json
    print(f'REQUEST: {request_dict}')
    v = json.dumps(request_dict)
        
    uri = f"https://{workspace_url}/api/2.0/sql/history/queries"
    headers_auth = {"Authorization":f"Bearer {dbx_token}"}

    #### Get Query History Results from API
    endp_resp = requests.get(uri, data=v, headers=headers_auth).json()
    #print(endp_resp)
    resp = endp_resp.get("res")
        
    if resp is None:
        print('no results!')
        return []

    next_page = endp_resp.get("next_page_token")
    has_next_page = endp_resp.get("has_next_page")

    total_fetch_count = len(resp)

    while has_next_page:
        #len_resp = len(resp)
        if len(resp) >= sink_batch_size:  #or len(resp) + total_count >= stop_fetch_limit:
          query_sink_fn(resp)
          resp = []

        if total_fetch_count >= stop_fetch_limit:
          break

        print(f"Getting results for next page... {next_page}")

        raw_page_request = {
        "include_metrics": "true",
        "max_results": max_single_call_results,
        "page_token": next_page
        }

        json_page_request = json.dumps(raw_page_request)

        current_page_resp = requests.get(uri,data=json_page_request, headers=headers_auth).json()
        current_page_queries = current_page_resp.get("res")

        resp.extend(current_page_queries)
        total_fetch_count += len(current_page_queries)

        ## Get next page
        next_page = current_page_resp.get("next_page_token")
        has_next_page = current_page_resp.get("has_next_page")

    if resp:
      query_sink_fn(resp)

    return total_fetch_count



def get_query_history(dbx_token, workspace_url, warehouse_ids, start_ts_ms,
                      end_ts_ms=None, user_ids=None, statuses=None, 
                      stop_fetch_limit=10000):
  query_sink = []
  def _fn(qh):
    print(f"got {len(qh)} queries")
    query_sink.extend(qh)
  
  total_fetch_count = sync_query_history(
     dbx_token, workspace_url, warehouse_ids, start_ts_ms,
    _fn, 100,
    end_ts_ms=end_ts_ms, user_ids=user_ids, statuses=statuses, 
    stop_fetch_limit=stop_fetch_limit)
  
  print(f"total_fetch_count: {total_fetch_count}")
  return query_sink



    
