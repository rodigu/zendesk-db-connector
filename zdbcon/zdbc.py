import pandas as pd
from datetime import datetime
from typing import Generator
from dateutil import tz

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

    def fetch_last_updated_tickets(self, since_datetime: datetime, include: list[str]=['metric_sets']) -> Generator[Ticket, None, None]:
        """Fetches last updated tickets since `since_datetime`, sorted by `updated_at` in ascending order.

        :param datetime since_datetime: datetime to use as starting point for API GET
        :yield Generator[Ticket, None, None]: ticket search generator
        """
        for ticket in self.client.search(type='ticket', updated_at_after=since_datetime.astimezone(tz.tzutc()), sort_by='updated_at', sort_order='asc'):
            yield self.client.tickets(id=ticket.id, include=include)

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