import requests
import hmac
import hashlib
import base64
from datetime import datetime
from urllib.parse import urlparse, parse_qs
import xml.etree.ElementTree as ET


# Define a function to recursively convert XML into a dictionary
def xml_to_dict(element):
    if len(element) == 0:
        return element.text
    result = {}
    for child in element:
        child_data = xml_to_dict(child)
        if child.tag in result:
            if isinstance(result[child.tag], list):
                result[child.tag].append(child_data)
            else:
                result[child.tag] = [result[child.tag], child_data]
        else:
            result[child.tag] = child_data
    return result


# Get the current time
def get_utc_now():
    return datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')


class Azure:
    def __init__(self, account_name, account_key, container_name):
        self.account_name = account_name
        self.account_key = account_key
        self.container_name = container_name

    def blob_list(self):
        url = f'https://{self.account_name}.blob.core.windows.net/{self.container_name}?comp=list&restype=container&timeout=20'
        return self.console_get(url)

    def generate_signature(self, url):
        date = get_utc_now()
        version = '2021-08-06'
        string_to_sign = (f"GET\n\n\n\n\n\n\n\n\n\n\n\n"
                          f"x-ms-date:{date}\n"
                          f"x-ms-version:{version}\n"
                          f"/{self.account_name}/{self.container_name}")
        query_params = parse_qs(urlparse(url).query)
        for param in query_params:
            string_to_sign += "\n%s:%s" % (param, query_params[param][0])
        decoded_key = base64.b64decode(self.account_key)
        signed_string = hmac.new(decoded_key, string_to_sign.encode('utf-8'), hashlib.sha256).digest()
        signature = base64.b64encode(signed_string).decode('utf-8')
        headers = {
            'x-ms-date': date,
            'x-ms-version': version,
            'Authorization': f'SharedKey {self.account_name}:{signature}'
        }
        return headers

    def console_get(self, url):
        # Generate authorization header
        headers = self.generate_signature(url)
        # request
        response = requests.get(url, headers=headers)
        xml_data = response.text
        # Parse XML data
        root = ET.fromstring(xml_data)

        # Convert XML to Dictionary
        data_dict = xml_to_dict(root)
        return data_dict


if __name__ == '__main__':
    # Set request parameters
    account_name = 'fd2d01cd892f844eeaa2273'
    account_key = '1234'
    container_name = 'td-test'

    Azure = Azure(account_name, account_key, container_name)
    result = Azure.blob_list()
    # Print JSON data
    for blob in result["Blobs"]["Blob"]:
        print(blob["Name"])
