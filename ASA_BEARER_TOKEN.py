import requests
import json
client_id= ""
client_secret= ""
tenant_id = ""
url = "https://login.microsoftonline.com/"tenant_id+"/oauth2/token"

payload = 'grant_type=client_credentials&client_id='+client_id+'&client_secret='+client_secret+'&resource=https%3A//*.asazure.windows.net'
headers = {
  'Content-Type': 'application/x-www-form-urlencoded',
  'Cookie': 'stsservicecookie=ests; fpc=AnZ_VedXpZxDu1S5_EloEcH6M5VHBAAAAFsj19YOAAAA; x-ms-gateway-slice=estsfd'
}
response = requests.request("POST", url, headers=headers, data = payload)
bear = json.loads(response.text.encode('utf8'))
bearertoken = json.dumps(bear['access_token'])
tokentype = json.dumps((bear['token_type']))
pickytoken = tokentype.translate({ord('"'): None}) +" "+bearertoken.translate({ord('"'): None})#probably the worst way to do this
