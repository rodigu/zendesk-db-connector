import pandas as pd
from zdbcon.zp import Zendesk
from zenpy.lib.api_objects import Ticket, TicketField

class ZenTicket(Zendesk):
    def __init__(self, credentials: dict[str, str], mapping_dict: dict[str, dict[str, str]]):
        """Zendesk API integration for individual tickets

        :param dict[str, str] credentials: Zendesk API credentials
        """
        super().__init__('Tickets', credentials=credentials, type_mapping=mapping_dict)
        self.ticket_fields = { str(f.id): f for f in self.client.ticket_fields() }

    def recache_ticket_fields(self):
        self.ticket_fields = { str(f.id): f for f in self.client.ticket_fields() }

    def ticket_dict(self, ticket: Ticket | dict) -> dict:
        d: dict = ticket if type(ticket) == dict else ticket.to_dict()
        d.pop('metric_events', None)
        return d

    def ticket_to_table(self, ticket: Ticket | dict) -> pd.DataFrame:
        """Converts ticket instance to pandas dataframe.

        :param Ticket|dict ticket: ticket instance or dictionary
        :return pd.DataFrame: pandas dataframe
        """
        td: dict = self.ticket_dict(ticket)
        for f in ['custom_fields', 'fields']:
            td.update({f: Zendesk._normalized_fields(td[f])})
        return pd.json_normalize(td)
    
    def get_sample_ticket(self) -> Ticket:
        """Retrieves sample ticket from Zendesk.

        :return Ticket: `Ticket` instance
        """
        return self.client.tickets()[:1][0]
    
    def get_field(self, id: int) -> TicketField:
        """Retrieves ticket field with given ID

        :param int id: ID of the ticket field
        :return TicketField: 
        """
        return self.client.ticket_fields(id=id)
    
    def raw_ticket_field_types(self, ticket_table: pd.DataFrame|None=None) -> dict[str, str]:
        """Creates dictionary with raw ticket types (as defined by `pandas`). Converts custom field types using Zendesk's api.

        :param pd.DataFrame | None ticket_table: optional reference ticket's table, defaults to None and uses `self.get_sample_ticket` along with `self.ticket_to_table`
        :return dict[str, str]: 
        """
        ticket_table = ticket_table if ticket_table is not None else self.ticket_to_table(self.get_sample_ticket())
        return { name: (str(t) if 'custom_fields.' not in name else self.ticket_fields[name.split('.')[1]].type) for name, t in ticket_table.dtypes.to_dict().items()}

    def ticket_field_types(self, ticket_table: pd.DataFrame|None = None, varchar_buffer=5) -> dict[str, str]:
        """Uses data mapping `JSON` to map ticket types to SQL types.

        Expects 3 keys: `"direct"`, `"except"`, `"date_fields"`.
        

        Sample:
        ```
        {
            "direct": {
                "int64": "bigint",
            },
            "except": {
                "25195420522772": "tinyint",
            },
            "date_fields": [
                "created_at",
            ]
        }
        ```

        :param pd.DataFrame | None ticket_table: dataframe created from ticket, defaults to None
        :param int varchar_buffer: buffer multiplier for max size string to be used as `varchar(max * varchar_buffer)`, defaults to 3
        :return dict[str, str]: dictionary with mapped types.
        """
        raw_types = self.raw_ticket_field_types(ticket_table)
        direct_map: dict = self.mapping_dict['direct']
        date_map: dict = self.mapping_dict['date_fields']
        except_map: dict = self.mapping_dict['except']

        for field_name, t in raw_types.items():
            update_dict = {}
            if t in direct_map:
                update_dict = {field_name: direct_map[t]}
            if field_name in date_map:
                update_dict = {field_name: 'datetime'}
            if 'custom_fields.' in field_name:
                id = int(field_name.split('.')[1])
                if self.get_field(id).type == 'tagger':
                    max_char = max([len(cfo.name) for cfo in self.client.ticket_fields(id=id).custom_field_options])
                    update_dict = {field_name: f'varchar({max_char * varchar_buffer})'}
                if (sid:=str(id)) in except_map:
                    update_dict = {field_name: except_map[sid]}
            
            raw_types.update(update_dict)
        
        return raw_types

    def append_ticket(self, ticket: Ticket|dict, force_update=False) -> bool:
        """Appends given ticket to Ticket table. Returns `True` if append was successful.
        
        Tickets that are closed and have already been inserted are ignored.
        
        Tickets that are closed but have not been inserted are appended to the table.

        Tickets that are not closed are updated.

        :param Ticket|dict ticket: Ticket instance or ticket dictionary from `Ticket().to_dict()`
        :param bool force_update: forces ticket update even if ticket is closed and already in table
        :return bool: 
        """
        if type(ticket) == dict:
            ticket_status = ticket['status']
            ticket_id = ticket['id']
        else:
            ticket_status = ticket.status
            ticket_id = ticket.id
        table_ids = self.get_table_ids(recache=False)
        if ticket_status == 'closed' and (ticket_id in table_ids):
            self.vp("Ticket is closed and already in table.")
            if not force_update:
                return False
            self.vp("Forcing update anyway.")

        df: pd.DataFrame = self.ticket_to_table(ticket)

        ftypes = self.ticket_field_types(ticket_table=df)

        ticket_list: list[dict[str, str]] = list(filter(lambda x: x['value'] is not None, ({'column': key, 'value': val, 'type': ftypes[key]} for key, val in df.iloc[0].to_dict().items())))

        column_list: list[str] = [t['column'] for t in ticket_list]
        columns = f"[{'], ['.join(column_list)}]"

        parsed_values: dict[str, str] = dict(map(Zendesk.parse_value, ticket_list))
        values = ', '.join(parsed_values.values())

        for col_name, col_type in ftypes.items():
            if not self.has_column(col_name):
                self.vp(f'Column {col_name} ({col_type}) does not exist, adding it now.')
                self.add_column(col_name, col_type)
                self.cache_table_columns()

        if ticket_id in table_ids:
            self.vp(f'Updating {ticket_status} ticket.')
            self.execute(f"update {self.table} set {', '.join([f'[{c}]={v}' for c, v in parsed_values.items()])} where id={ticket_id}")
        else:
            self.vp('Inserting ticket.')
            self.execute(self.sql_insertion_str(columns=columns, values=values))
        
        self.commit()

        self.id_cache.add(ticket_id)

        return True
