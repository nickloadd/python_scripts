import json
from urllib import response
import requests
import sys

API_KEY="UzQxLikm_46KxDFnbjN7cQjmw6wocia"
API_SECRET="46L26ydpkwMaKZV6uVdDWe"
domain="010starrocks.com"
url="https://api.ote-godaddy.com/v1/domains/"+ domain +"/records"


json_headers = {
  "accept": "application/json",
  "Content-Type": "application/json",
  "Authorization": "sso-key " + API_KEY + ":" + API_SECRET
}

def add(publisher):
  add_body = """[{ "type": "CNAME", "name": """ + '"' + publisher + '"'  + """, "data": """ + '"' + publisher + '.' + domain + '"'  + """ }]"""
  print(add_body)
  return (requests.patch(url, headers=json_headers, data=add_body))

def remove(publisher):
  del_url=url + "/CNAME/" + publisher
  del_body = """[{ "type": "CNAME", "name": """ + '"' + publisher + '"' + """, "domain": """ + '"' + domain + '"'  + """ }]"""
  print(del_body)
  return (requests.delete(del_url, headers=json_headers, data=del_body))

if len(sys.argv) < 3:
    print("Wrong count of argv \nRun: '" + sys.argv[0] + " add/remove CNAME'")
    exit(1)

cname=sys.argv[2]
action=sys.argv[1]

if action == "add" :
  print(cname + "will be add")
  print(add(cname))
elif action == "remove":
  print(cname + "will be remove")
  print(remove(cname))
else:
  print("Error: Wrong args \nRun: '" + sys.argv[0] + " add/remove CNAME'")
  exit(2)

