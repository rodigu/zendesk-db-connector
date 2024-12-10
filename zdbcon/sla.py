from zdbcon.audit import ZenAudit
from zenpy.lib.api_objects import Audit

class ZenSLA(ZenAudit):
    def __init__(self, cred_json='./auth/zendesk.json'):
        """Zendesk SLA integration

        :param str cred_json: zendesk API credentials JSON, defaults to './auth/zendesk.json'
        """
        super().__init__(cred_json=cred_json)
        self.table = "SLAAudit"

    @staticmethod
    def format_event(event: dict, audit: Audit, ticket_id: int) -> dict:
        """Formats `event` from the `audit` for `ticket_id` into a dictionary

        :param dict event:
        :param Audit audit:
        :param int ticket_id:
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

    @staticmethod
    def is_sla_change(event: dict) -> bool:
        """Checks if given event is an SLA change

        :param dict event:
        :return bool: True if event is an SLA change
        """
        return event['type'] =='Change' and  'via' in event and event['via']['source']['rel'] == 'sla_target_change'

    def get_sla_changes(self, ticket_id: int) -> GeneratorExit:
        """Yields dictionaries for each event in the Audits for `ticket_id`

        :param int ticket_id:
        :yield GeneratorExit[dict]: event dictionary
        """
        return (self.format_event(e, a, ticket_id) for a in self.client.tickets.audits(ticket=ticket_id) for e in a.events if ZenSLA.is_sla_change(e))

    def append_ticket_sla_changes(self, ticket_id: int, force=False):
        """Appends all SLA changes for `ticket_id` to SLAAudit Table

        :param int ticket_id:
        :param bool force: If True, SLA changes will be force updated even if they are already found in the table, defaults to False
        """
        self.vp(">\tAppending SLA Changes")
        for h in self.get_sla_changes(ticket_id):
            self.append_obj(h, recache=False, force=force)