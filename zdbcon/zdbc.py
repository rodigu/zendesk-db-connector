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
        keys = {'field_name'}
        return (
            {
                'ticket_id': ticket_id,
                'changed_at': audit.created_at,
                **{x: event[x] for x in event if x not in keys},
                'id': f"{audit.id}-{event['id']}",
                'audit_id': audit.id,
                'event_id': event['id']
            }
            for audit in audits
                for event in audit.events
                    if (event['type'] == 'Change' or event['type'] == 'Create')
                    and event['field_name'] == str(field_name)
        )

    @staticmethod
    def extract_audit_chat_history(ticket_id: int, audits: list[Audit]) -> Generator[dict, None, None]:
        """Extracts ticket chat history from given `Audit`

        :param int ticket_id: ID of the ticket that originates the audits
        :param list[Audit] audit: `ZenPy` `Audit` instance list
        :yield Generator[dict, None, None]: dictionary with a chat event
        """
        return (
            history for audit in audits
            for event in audit.events
                if event['type'] == 'ChatStartedEvent'
            for history in ZDBC.format_chat_history(event, ticket_id)
        )

    @staticmethod
    def extract_chat_history_from_event(ticket_id: int, event: dict) -> Generator[dict, None, None]:
        """Extracts chat history from given event

        :param int ticket_id: ID of tikcet that originates the event
        :param dict event: event from `audit.events`
        :yield Generator[dict, None, None]: generator for chat dictionaries
        """
        return (
            {
                "ticket_id": ticket_id,
                "id": f"{event['id']}-{idx}",
                "content": history,
                "chat_id": event['id']
            }
            for idx, history in enumerate(event['value']['history'])
                if history['type'] == "ChatMessage"
        )

    @staticmethod
    def extract_sla_history(ticket_id: int, audits: list[Audit]) -> Generator[dict, None, None]:
        """Extracts SLA history from givent Audits list

        :param int ticket_id: ID of the ticket that originates the audits
        :param list[Audit] audits: list of audits from ticket with `ticket_id`
        :yield Generator[dict, None, None]: dictionary with SLA events
        """
        return (
            ZDBC.format_event(ticket_id=ticket_id, event=event, audit=audit)
            for audit in audits
                for event in audit.events
                    if ZDBC.is_sla_change(event)
        )

    @staticmethod
    def is_sla_change(event: dict) -> bool:
        """Checks if given event is an SLA change

        :param dict event: event dictionary from `audit.events`
        :return bool: True if event is an SLA change
        """
        return (
            (event['type'] == 'Change')
            and ('via' in event)
            and (event['via']['source']['rel'] == 'sla_target_change')
        )

    @staticmethod
    def format_event(ticket_id: int, audit: Audit, event: dict) -> dict:
        """Formats `event` from the `audit` for `ticket_id` into a dictionary

        :param int ticket_id:
        :param Audit audit:
        :param dict event:
        :return dict:
        """
        return {
            **dict(x for x in event.items() if x[0]!='previous_value'),
            'id': f"{audit.id}-{event['id']}",
            'event_id': event['id'],
            'audit_id': audit.id,
            'ticket_id': ticket_id,
            'changed_at': audit.created_at
        }