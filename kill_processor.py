import requests
import re

import warnings
warnings.filterwarnings("ignore")

import sqlite3
import json
from datetime import timedelta

def convert_to_gb(mem_val):
    if mem_val.startswith('0'):
        return 0

    num = float(mem_val[:-2])

    if mem_val.endswith('G'):
        return num
    elif mem_val.endswith('MB'):
        return num / 1024
    elif mem_val.endswith('KB'):
        return num / (1024 * 1024)
    elif mem_val.endswith('T'):
        return num * 1024
    elif mem_val.endswith('P'):
        return num * 1024 * 1024
    elif mem_val.endswith('B'):
        return num / (1024 * 1024 * 1024)

def convert_to_seconds(time_val):
    if time_val.endswith('us'):
        num = float(time_val[:-2])
        return timedelta( seconds=(num / 1000000 )).seconds
    elif time_val.endswith('ms'):
        num = float(time_val[:-2])
        return timedelta( seconds=(num / 1000 )).seconds
    elif time_val.endswith('ns'):
        return 0

    num = float(time_val[:-1])

    if time_val.endswith('s'):
        return timedelta(seconds=num).seconds
    elif time_val.endswith('m'):
        return timedelta(minutes=num).seconds
    elif time_val.endswith('h'):
        return timedelta(hours=num).seconds
    elif time_val.endswith('d'):
        return timedelta(days=num).seconds


def get_queries(presto_ui, presto_port=8443, use_ssl=True, verify=False):
    if use_ssl:
        request_url = "https://" + presto_ui + ":" + str(presto_port) + "/v1/query"
    else:
        request_url = "http://" + presto_ui + ":" + str(presto_port) + "/v1/query"

    response = requests.get(request_url, verify=False)
    json_payload = response.json()
    return json_payload

def find_killed_queries(query_json):

    kill_query = re.compile("CALL system.runtime.kill_query\(\'(\w*)\'\)")

    query_ids = list()
    for record in query_json:
        #print(record["query"])
        #print(record["state"] == "RUNNING")
        #print(record["queryId"])
        if kill_query.match(record["query"]) and record["state"] in ["QUEUED", "RUNNING"]:
            query_id = kill_query.match(record["query"]).group(1)
            query_ids.append(query_id)

    return query_ids

def find_looker_catalog_queries(query_json):

    looker_query = re.compile("SELECT DISTINCT CONCAT\(CONCAT\(table_catalog\,")

    query_ids = list()
    for record in query_json:
        if looker_query.match(record["query"]) and record["state"] in ["QUEUED", "RUNNING"]:
            query_ids.append(record["queryId"])

    return query_ids

def log_queries(query_json):

    init_query = """CREATE TABLE IF NOT EXISTS queries (
                    queryId TEXT PRIMARY KEY, 
                    memoryPool TEXT,
                    query TEXT,
                    blockedReasons TEXT,
                    completedDrivers NUMERIC,
                    createTime TEXT,
                    endTime TEXT,
                    executionTime TEXT,
                    elapsedTime REAL,
                    fullyBlocked TEXT,
                    peakMem REAL,
                    totalCpuTime REAL,
                    totalDrivers NUMERIC,
                    totalMemoryReservation REAL,
                    lookupCatalog TEXT,
                    catalogProperties TEXT,
                    preparedStatements TEXT,
                    source TEXT,
                    startTime NUMERIC,
                    systemProperties TEXT,
                    timeZoneKey TEXT,
                    prestoUser TEXT,
                    queryState TEXT
                    );"""

    conn = sqlite3.connect("logged_queries.sqlite")
    conn.execute(init_query)
    records = list()

    for query in query_json:
        queryId = query["queryId"]
        memoryPool = query["memoryPool"]
        queryString = query["query"]
        blockedReasons = ",".join(query["queryStats"]["blockedReasons"])
        completedDrivers = query["queryStats"]["completedDrivers"]
        createTime = query["queryStats"]["createTime"]
        elapsedTime = convert_to_seconds(query["queryStats"]["elapsedTime"])
        endTime = query["queryStats"].get("endTime")
        executionTime = convert_to_seconds(query["queryStats"]["executionTime"])
        fullyBlocked = str(query["queryStats"]["fullyBlocked"])
        peakMem = convert_to_gb(query["queryStats"]["peakMemoryReservation"])
        totalCpuTime = convert_to_seconds(query["queryStats"]["totalCpuTime"])
        totalDrivers = query["queryStats"]["totalDrivers"]
        totalMemoryReservation = convert_to_gb(query["queryStats"]["totalMemoryReservation"])
        lookupCatalog = query["session"].get("catalog", "default")
        catalogProperties = json.dumps(query["session"]["catalogProperties"])
        preparedStatements = json.dumps(query["session"]["preparedStatements"])
        source = query["session"]["source"]
        startTime = query["session"]["startTime"]
        systemProperties = json.dumps(query["session"]["systemProperties"])
        timeZoneKey = str(query["session"]["timeZoneKey"])
        prestoUser = query["session"]["user"]
        queryState = query["state"]
        record = (queryId, memoryPool, queryString, blockedReasons, completedDrivers, createTime, elapsedTime,
                  endTime, executionTime, fullyBlocked, peakMem, totalCpuTime, totalDrivers, totalMemoryReservation,
                  lookupCatalog, catalogProperties, preparedStatements, source, startTime, systemProperties,
                  timeZoneKey, prestoUser, queryState)
        records.append(record)

    insert_query = """INSERT OR REPLACE INTO queries VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
    conn.executemany(insert_query, records)
    conn.commit()
    conn.close()

def kill_queries(presto_ui, list_of_queryIds, presto_port=8443, use_ssl=True, verify=False):
    if use_ssl:
        request_url = "https://" + presto_ui + ":" + str(presto_port) + "/v1/query"
    else:
        request_url = "http://" + presto_ui + ":" + str(presto_port) + "/v1/query"

    for query_id in list_of_queryIds:
        print("killing query: " + query_id)
        referrer = "https://avapresto01.ad.avant.com:8443/query.html?" + query_id
        kill_url = request_url + "/" + query_id

        print(kill_url)
        response = requests.delete(kill_url, verify=False, headers={'referer': referrer})
        print(repr(response))

if __name__ == "__main__":
    queries = get_queries("avapresto01.ad.avant.com")
    queries_to_kill = list()

    queries_to_kill = queries_to_kill + find_killed_queries(queries)
    queries_to_kill = queries_to_kill + find_looker_catalog_queries(queries)
    kill_queries("avapresto01.ad.avant.com", set(queries_to_kill))
    log_queries(queries)
