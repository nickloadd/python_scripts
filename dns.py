import json
from urllib import response
import requests

add_url="https://api.ote-godaddy.com/v1/domains/010starrocks.com/records"
del_url="https://api.ote-godaddy.com/v1/domains/010starrocks.com/records/CNAME/prefix"
API_KEY="UzQxLikm_46KxDFnbjN7cQjmw6wocia"
API_SECRET="46L26ydpkwMaKZV6uVdDWe"

publisher=input()
#publisher='"prefix"'
name_sersver=input()
#name_sersver='"010starrocks.com"'

add_body = """[{ "type": "CNAME", "name": """ + publisher + """, "data": "prefix.010starrocks.com" }]"""
del_body = """[{ "type": "CNAME", "name": """ + publisher + """, "domain": "010starrocks.com" }]"""

print("Add:", add_body)
print("Delete:" ,del_body)

json_headers={
  "accept": "application/json",
  "Content-Type": "application/json",
  "Authorization": "sso-key " + API_KEY + ":" + API_SECRET
}

response=requests.patch(add_url, headers=json_headers, data=add_body)
#response=requests.delete(del_url, headers=json_headers, data=del_body)

print(response.status_code)
print(response._content)