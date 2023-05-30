import sys
import os

sys.path.append(f'{os.path.dirname(os.path.realpath(__file__))}/utils/')
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import (Stream, IncrementalMixin)
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import NoAuth, TokenAuthenticator
from datetime import datetime, timedelta, timezone
from .streams.clients import Clients
from .streams.products import Products
from .streams.stocks import Stocks, StockDetails
from .streams.sales import Sales
from .streams.purchaseorders import PurchaseOrder
from .utils.auth import getAccessToken


class SourceDefontana(AbstractSource):

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        if getAccessToken(config):
            return True, None
        return False, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        config['access_token'] = getAccessToken(config)
        auth = TokenAuthenticator(token=config['access_token'], auth_method="Bearer")
        start_date = datetime(1970, 1, 1, 0, 0, 0)
        return [Clients(authenticator=auth, config=config),
                Products(authenticator=auth, config=config, start_date=start_date),
                Stocks(authenticator=auth, config=config, start_date=start_date),
                StockDetails(authenticator=auth, config=config, start_date=start_date),
                PurchaseOrder(authenticator=auth, config=config, start_date=start_date),
                Sales(authenticator=auth, config=config, start_date=start_date)]

