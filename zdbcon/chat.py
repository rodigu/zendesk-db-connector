from zdbcon.audit import ZenAudit

class ZenChat(ZenAudit):
    def __init__(self, credentials: dict[str, str]):
        """Zendesk integration for chat logs

        :param dict[str, str] credentials: Zendesk API credentials dictionary
        """
        super().__init__(credentials=credentials)
        self.table = "ChatLogs"

    @staticmethod
    def format_chat_history(event: dict, ticket_id: int) -> list[dict]:
        """Formats chat history into list of dictionaries

        :param dict event: event dictionary from zendesk api
        :param int ticket_id:
        :return list: chat history list
        """
        return [
            {
                "ticket_id": ticket_id,
                "id": f"{event['id']}-{idx}",
                "content": h,
                "chat_id": event['id']
            }
            for idx, h in enumerate(event['value']['history'])
                if h['type'] == "ChatMessage"
        ]

    def get_chat_history(self, ticket_id: int) -> GeneratorExit:
        """Retrieves chat history for given `ticket_id` from the Zendesk Audit API

        :param int ticket_id:
        :yield GeneratorExit[list[dict]]: yields a formatted chat history using `ZenChat.format_chat_history`
        """
        return (
            h for audit in self.client.tickets.audits(ticket=ticket_id)
            for event in audit.events
                if event['type'] == 'ChatStartedEvent'
            for h in self.format_chat_history(event, ticket_id)
        )

    def append_ticket_chat(self, ticket_id: int):
        """Appends chat for `ticket_id` to ChatLogs table

        :param int ticket_id:
        """
        self.vp("Appending Ticket Chat")
        for h in self.get_chat_history(ticket_id):
            self.append_obj(h, recache=False, force=False)
