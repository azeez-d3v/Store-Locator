from curl_cffi import requests

url = "https://www.chemistwarehouse.com.au/webapi/store/store-locator?BusinessGroupId=2&SearchByState=&SortByDistance=false"

payload = {}
headers = {
  'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0',
}

response = requests.get(url, headers=headers, data=payload, impersonate="edge101")

print(response.text)
