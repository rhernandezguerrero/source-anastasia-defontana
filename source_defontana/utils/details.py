from datetime import datetime, timezone, date
import requests
from requests.adapters import HTTPAdapter, Retry
import re
import collections

current_date_timestamp = int(datetime.timestamp(datetime.now()))
request_session = requests.Session()
adapter = HTTPAdapter(max_retries=Retry(
    total=10,
    backoff_factor=0.1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"]
))
request_session.mount("https://", adapter)


class Details:

    def __init__(self, access_token):
        self.access_token = access_token
        self.date = date.today()

    def format_code(self, code):
        return code.replace(" ", "%20")

    def format_date(self):
        return self.date.strftime("%Y-%m-%d")


class ProductDetails(Details):

    def get_product_cost(self, code):
        url = f"https://api.defontana.com/api/sale/GetCurrentCost"
        params = {
            "code": self.format_code(code),
            "Date": self.format_date()
        }
        headers = {
            "Authorization": f"bearer {self.access_token}"
        }
        response = request_session.get(url, params=params, headers=headers)
        if response.status_code != 200:
            return 0
        return float(response.json()["cost"])


class CategoryDetails(Details):

    def get_category_details(self, category):
        pass

