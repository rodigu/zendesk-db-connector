import pandas as pd
from datetime import datetime
from typing import Generator

from zenpy import Zenpy
from zenpy.lib.api_objects import Ticket, Audit
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

    def fetch_last_updated_tickets(self, since_date: datetime) -> Generator[Ticket, None, None]:
        pass

    def fetch_ticket_audits(self, ticket: Ticket) -> Generator[Audit, None, None]:
        pass

    @staticmethod
    def dict_from_ticket(ticket: Ticket) -> dict:
        pass

    def dict_from_audit(audit: Audit) -> dict:
        pass

    @staticmethod
    def extract_audit_field_events(audit: Audit, field_name: str) -> Generator[dict, None, None]:
        pass

    @staticmethod
    def extract_audit_chat_history(audit: Audit) -> Generator[dict, None, None]:
        pass