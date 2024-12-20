from zdbcon.zp import Zendesk
import pandas as pd
from zenpy.lib.response import GenericCursorResultsGenerator
from zenpy.lib.api_objects import Audit
from zenpy.lib.proxy import ProxyDict

class ZenAudit(Zendesk):
    """Pipeline for audit metadata.

    Metadata includes some data that does not show up on tickets.

    For instance, the full text of a social media post gets cut in the subject, raw_subject and description fields of a ticket.

    :param Zendesk: Zendesk parent class
    """
    def __init__(self, credentials: dict[str, str], type_mapping: dict[str, dict[str, str]]):
        super().__init__(table='Audits', credentials=credentials, type_mapping=type_mapping)

    def get_ticket_audits(self, id: int) -> GenericCursorResultsGenerator[Audit]:
        """Generator for ticket audits

        :param int id: ticket id
        :return GenericCursorResultsGenerator[Audit]: generator for ticket audits
        """
        return self.client.tickets.audits(ticket=id)

    @staticmethod
    def process_audit(audit: Audit) -> dict:
        """Turns Audit into flattened dict.

        Lists of dicts are recursively flattened.

        :param Audit audit: Zenpy Audit object
        :return dict: flattened dictionary
        """
        d:ProxyDict = audit.to_dict()
        d.pop('events', None)
        if 'decoration' in d['metadata']:
            d['metadata']['decoration'].pop('links', None)
        return Zendesk.flatten_dict(d, ['id', 'type'])

    def map_type(self, column: str, pd_type: str) -> str:
        """Maps a given column with the pandas datatype to a SQL datatype using mapping dict from a JSON file

        :param str column: column name
        :param str pd_type: pandas datatype
        :return str: SQL compatible data type
        """
        direct_map: dict = self.mapping_dict['direct']
        date_map: dict = self.mapping_dict['date_fields']
        except_map: dict = self.mapping_dict['except']

        if column in date_map or column[-3:] == '_at':
            return 'datetime'
        if pd_type in direct_map:
            return direct_map[pd_type]
        if column in except_map:
            return except_map[column]
        return pd_type

    def field_events(self, ticket_id: int, field_name: str | int, audit_iter: list=None):
        keys = {'field_name'}
        audits = audit_iter or self.get_ticket_audits(ticket_id)
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
                    and event['field_name'] == field_name
        )

    def status_change_events(self, ticket_id: int) -> dict[str, any]:
        """Status changes events dicts for the given ticket id

        :param int ticket_id:
        :return dict: status change events dictionary
        """
        return self.field_events(ticket_id, 'status')

    def commercial_status_change_events(self, ticket_id: int) -> dict[str, any]:
        """Commercial status changes events dicts for the given ticket id

        :param int ticket_id:
        :return dict: commercial status change events dictionary
        """
        return self.field_events(ticket_id, 26870412763796)

    def audit_type_list(self, audit: Audit) -> list[dict[str, str]]:
        """Type list for given audit

        :param Audit audit: Zenpy Audit object
        :return list[dict[str, str]]: type list
        """
        df = pd.json_normalize(ZenAudit.process_audit(audit))
        pd_types = df.dtypes.to_dict()
        return [{'column': key, 'value': val, 'type': self.map_type(key, str(pd_types[key]))} for key, val in df.iloc[0].to_dict().items() if val is not None]
