import pandas as pd
from zenpy import Zenpy
from zdbcon.credentials import Credentials

class ZDBC:
    def __init__(self, credentials: Credentials):
        self._credentials = credentials
        self.connect_zendesk()

    def connect_zendesk(self):
        self.zendesk_client: Zenpy = Zenpy(
            subdomain=self._credentials.subdomain,
            email=self._credentials.email,
            token=self._credentials.token
        )
