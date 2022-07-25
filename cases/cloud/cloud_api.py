import requests
from http.cookiejar import CookieJar

cj = CookieJar()

headers = {
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json"
}


def get_auth_token():
    url = 'http://uc.spiderio.cn/uc/api/auth/login'
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json"
    }
    data = {"email": "bding@taosdata.com", "password": "Dbo@123456"}

    resp = requests.post(url, json=data, headers=headers, cookies=cj)
    status = resp.status_code
    body = resp.json()
    if status != 200 or body['msg'] is not None:
        print(resp.status_code)
        print(body)
        raise Exception("login error")
    return body["data"]["token"]


def app_list(token):
    url = 'http://console.spiderio.cn/api/app/list'
    hds = headers.copy()
    hds["Authorization"] = "Bearer " + token
    resp = requests.get(url, headers=hds, cookies=cj)
    import datetime
    datetime.datetime.fromtimestamp()
    return resp


if __name__ == '__main__':
    token = get_auth_token()
    app_list(token)
