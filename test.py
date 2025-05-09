from curl_cffi import requests
import json

url = "https://www.directchemistoutlet.com.au/graphql?query=query+getAmStoreLocatorsByState%7BamStoreLocatorsByState%7Bstate_code+state_id+items%7Bid+is_new+state+name+url_key+__typename%7D__typename%7D%7D&operationName=getAmStoreLocatorsByState&variables=%7B%7D"

payload = {}
headers = {
  'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0',
}

response = requests.get(url, headers=headers, data=payload, impersonate="chrome131")

print(response.text)
