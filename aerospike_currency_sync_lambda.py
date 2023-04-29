import json
import aerospike
import requests
import os


hosts       = os.environ['hosts']
currencies  = os.environ['currencies']
namespace   = os.environ['namespace']
apikey      = os.environ['apikey']

currencies_list = currencies.split(",")

api = "https://api.apilayer.com/exchangerates_data/latest?symbols=" + currencies + "&base=USD"

headers= {
    "apikey": apikey
}

def parse_hosts(hosts: str):
    host_list = list()
    hosts = hosts.split(",")
    for host in hosts:
        host_port = host.split(":")
        t = (host_port[0], int(host_port[1]))
        host_list.append(t)
    return host_list

def get_currency(api: str, headers: map, currency: str):
    res = requests.get(api, headers)
    data = res.json()
    status_code = res.status_code
    rate = data["rates"][currency]
    return float(rate)


def update_aerospike_value(hosts: list, api: str, headers: map, namespace: str):
    conf = {
        "hosts": hosts,
        "policies": {
            "timeout": 100  # milliseconds
        }
    }
    client = aerospike.client(conf).connect()
    for i in currencies_list:
        key = (namespace, "currency", i)
        value = {i: get_currency(api, headers, i)}
        client.put(key, value)
    client.close()

def lambda_handler(event, context):
    # TODO implement
    print("hosts:", hosts)
    print("currencies:", currencies)
    print("currencies_list:", currencies_list)
    print("namespace:", namespace)
    print("url:", api)
    update_aerospike_value(parse_hosts(hosts), api, headers, namespace)
    return None