import requests
from http.cookiejar import CookieJar


class CloudApi:
    cj = CookieJar()

    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json"
    }

    def __init__(self):
        self.token = None
        # self.login_domain = "http://uc.spiderio.cn/"
        # self.cloud_domain = "http://console.spiderio.cn"
        # self.app_id = "1552075667132174336"
        self.login_domain = "https://uc.cloud.tdengine.com/"
        self.cloud_domain = "https://gcp.cloud.tdengine.com/"
        self.app_id = "1549749987899842560"

    def get_auth_token(self):
        url = self.login_domain + '/uc/api/auth/login'
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json"
        }
        data = {"email": "bding@taosdata.com", "password": "Dbo@123456"}

        res = requests.post(url, headers=headers, json=data, cookies=self.cj)
        status = res.status_code
        if status != 200:
            print(res.status_code)
            raise Exception("login error")

        body = res.json()
        if body['msg'] is not None:
            self.token = body["data"]["token"]
            self.headers["Authorization"] = "Bearer " + self.token
            return self.token
        else:
            raise Exception("login error: %s", body)

    def app_list(self):
        url = self.cloud_domain + 'api/app/list'
        return requests.get(url, headers=self.headers, cookies=self.cj)

    def sql(self, sql, app_id=None):
        if app_id is None:
            app_id = self.app_id
        url = self.cloud_domain + "api/data/sql/" + app_id
        data = {"sql": sql}
        return requests.post(url, headers=self.headers, json=data, cookies=self.cj)


if __name__ == '__main__':
    api = CloudApi()
    api.get_auth_token()
    resp = api.sql("show databases")
    print(resp.json())
