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

def get_currency(api: str, headers: dict):
    res = requests.get(api, headers)
    data = res.json()
    if (res.status_code != 200):
        print("Response status code:", res.status_code)
        raise ValueError("Failed connect to API")
    return data


def update_aerospike_value(hosts: list, api: str, headers: dict, namespace: str):
    data = get_currency(api, headers)
    conf = {
        "hosts": hosts,
        "policies": {
            "timeout": 100,  # milliseconds
            "maxRetries": 5
        }
    }
    try:
        client = aerospike.client(conf).connect()
    except:
        raise RuntimeError("Failed connect to Aerospike")
    for i in currencies_list:
        key = (namespace, "currency", "currency")
        round_value = round(data["rates"][i], 4)
        value = {i: round_value }
        client.put(key, value, meta={'ttl': aerospike.TTL_NEVER_EXPIRE})
        (key_, meta, bin) = client.get(key)
        if (round_value != bin[i]):
            print("API value=",round_value," Write bin=", bin[i])
            raise ValueError("API and Write values are not equal")
        print("Currency",i,"was updated with value=",round_value)
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