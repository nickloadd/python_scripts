import aerospike
import argparse

import requests

def parse_args():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--hosts", type=str, help="aerospike hosts")
    arg_parser.add_argument("--namespace", type=str, help="aerospike namespace")
    arg_parser.add_argument("--currencies", type=str, help="currency list")
    arg_parser.add_argument("--apikey", type=str, help="api key")
    args = arg_parser.parse_args()
    if any(x is None for x in [args.hosts, args.namespace, args.currencies, args.apikey]):
        return None
    return args


def parse_hosts(hosts: str):
    host_list = list()
    hosts = hosts.split(",")
    for host in hosts:
        host_port = host.split(":")
        t = (host_port[0], int(host_port[1]))
        host_list.append(t)
    return host_list

def parse_currencies(currencies: str):
    currencies_list = currencies.split(",")
    return currencies_list

def build_headers(apikey: str):
    headers = {
        "apikey": apikey
    }
    return headers

def get_currency(url: str, headers: dict):
    res = requests.get(url, headers)
    data = res.json()
    if (res.status_code != 200):
            print("Response status code:", res.status_code)
            raise ValueError("Failed connect to API")
    return data

def update_aerospike_value(url: str, hosts: list, namespace: str, currencies: list, apikey: str):
    data = get_currency(url, build_headers(apikey))
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
    for i in currencies:
        key = (namespace, "currency", "currency")
        round_value = round(data["rates"][i], 4)
        value = {i: round_value }
        client.put(key, value, meta={'ttl': aerospike.TTL_NEVER_EXPIRE}) #store forever
        (key_, meta, bin) = client.get(key)
        if (round_value != bin[i]):
            print("API value=",round_value," Write bin=", bin[i])
            raise ValueError("API and Write values are not equal")
    client.close()

def main():
    args = parse_args()
    url  = "https://api.apilayer.com/exchangerates_data/latest?symbols=" + args.currencies + "&base=USD"
    if args is None:
        raise ValueError("Invalid arguments")
    update_aerospike_value(url, parse_hosts(args.hosts), args.namespace, parse_currencies(args.currencies), args.apikey)

if __name__ == "__main__":
    main()
