import pandas as pd
from datetime import datetime
from typing import Generator
from dateutil import tz

from zenpy.lib.response import GenericCursorResultsGenerator
from zenpy import Zenpy
from zenpy.lib.api_objects import Ticket, Audit
from zenpy.lib.proxy import ProxyDict

from flatten_dictionary import flatten_dict

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

    def fetch_ticket_audits(self, ticket_id: int) -> GenericCursorResultsGenerator[Audit]:
        """Generator for ticket audits

        :param int ticket_id: ticket id
        :return GenericCursorResultsGenerator[Audit]: generator for ticket audits
        """
        return self.zendesk_client.tickets.audits(ticket=ticket_id)

    def fetch_last_updated_tickets(self, since_datetime: datetime, include: list[str]=['metric_sets']) -> Generator[Ticket, None, None]:
        """Fetches last updated tickets since `since_datetime`, sorted by `updated_at` in ascending order.

        :param datetime since_datetime: datetime to use as starting point for API GET
        :yield Generator[Ticket, None, None]: ticket search generator
        """
        for ticket in self.zendesk_client.search(type='ticket', updated_at_after=since_datetime.astimezone(tz.tzutc()), sort_by='updated_at', sort_order='asc'):
            yield self.zendesk_client.tickets(id=ticket.id, include=include)

    def fetch_deleted_tickets(self) -> Generator[Ticket, None, None]:
        """Fetches the last deleted tickets from the API. They are returned in `deleted_at` ascending erder.

        :return None: returns the ZenPy deleted tickets generator
        :yield Generator[Ticket, None, None]: deleted tickets generator
        """
        return self.zendesk_client.tickets.deleted(sort_by='deleted_at', sort_order='asc')

    @staticmethod
    def dict_from_ticket(ticket: Ticket) -> dict:
        """Parses given `ZenPy` `Ticket` instance into a python dictionary

        - normalizes `custom_fields`
        - converts dates from `UTC` to `GMT-3`

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
        d:ProxyDict = audit.to_dict()
        d.pop('events', None)
        if 'decoration' in d['metadata']:
            d['metadata']['decoration'].pop('links', None)
        return flatten_dict(d, ['id', 'type'])

    @staticmethod
    def extract_field_events_from_audits(ticket_id: int, audits: list[Audit], field_name: str):
        """Extract all events with givin field name form list of ticket audits

        :param int ticket_id: ID of the ticket that originates the audits
        :param list[Audit] audits: list of audits from ticket with `ticket_id`
        :param str field_name: name of the field to be extracted
        """
        def audits_gen():
            for a in audits:
                yield a.to_dict()
        keys = {'field_name'}
        return (
            {
                'ticket_id': ticket_id,
                'changed_at': audit['created_at'],
                **{x: event[x] for x in event if x not in keys},
                'id': f"{audit['id']}-{event['id']}",
                'audit_id': audit['id'],
                'event_id': event['id']
            }
            for audit in audits_gen()
                for event in audit['events']
                    if (event['type'] == 'Change' or event['type'] == 'Create')
                    and event['field_name'] == str(field_name)
        )

    @staticmethod
    def extract_audit_chat_history(audit: Audit) -> Generator[dict, None, None]:
        """Extracts ticket chat history from given `Audit`

        :param Audit audit: `ZenPy` `Audit` instance
        :yield Generator[dict, None, None]: dictionary with a chat event
        """
        pass
