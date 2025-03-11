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
        """Creates an API client with ZenPy using the credentials given at `__init__`.
        """
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

    def fetch_deleted_tickets(self) -> Generator[Ticket, None, None]:
        """Fetches the last deleted tickets from the API. They are returned in `deleted_at` ascending erder.

        :return None: returns the ZenPy deleted tickets generator
        :yield Generator[Ticket, None, None]: deleted tickets generator
        """
        return self.client.tickets.deleted(sort_by='deleted_at', sort_order='asc')

    @staticmethod
    def dict_from_ticket(ticket: Ticket) -> dict:
        """Parses given `ZenPy` `Ticket` instance into a python dictionary

        :param Ticket ticket: ticket instance
        :return dict: dictionary with ticket data
        """
        pass

    @staticmethod
    def dict_from_audit(audit: Audit) -> dict:
        """Parses `Audit` instance into Python dictionary

        :param Audit audit: ticket audit
        :return dict: dictionary
        """
        pass

    @staticmethod
    def extract_audit_field_events(audit: Audit, field_name: str) -> Generator[dict, None, None]:
        """Extracts events with given `field_name`

        :param Audit audit: ZenPy Audit instance
        :param str field_name: name of the field to be extracted and parsed into a dictionary
        :yield Generator[dict, None, None]: dictionary with a field event
        """
        pass

    @staticmethod
    def extract_audit_chat_history(audit: Audit) -> Generator[dict, None, None]:
        """Extracts ticket chat history from given `Audit`

        :param Audit audit: `ZenPy` `Audit` instance
        :yield Generator[dict, None, None]: dictionary with a chat event
        """
        pass
