import pyodbc as db
import pandas as pd
from dateutil import parser as date_parser
from dateutil import tz
from zenpy import Zenpy
from zenpy.lib.api_objects import Ticket
from datetime import datetime, timedelta
from icecream import ic

class Zendesk:
    PandasTypeMap = {
        "int64": "bigint",
        "object": "varchar(max)",
        "bool": "bit",
    }

    def __init__(self, table: str, credentials: dict[str, str], type_mapping: dict[str, dict[str,str]]={}):
        """Zenpy wrapper that connects the API to a pyodbc SQL Database

        Example `credentials` for Zendesk:

        ```json
        {
            "email": EMAIL,
            "token": API_TOKEN,
            "subdomain": SUBDOMAIN
        }
        ```

        Example `type_mapping`:

        ```json
        {
            "direct": {
                "int64": "bigint",
                "object": "varchar(max)",
                "bool": "bit",
                "text": "varchar(max)",
                "integer": "bigint",
                "tagger": "varchar(max)",
                "checkbox": "bit",
                "date": "datetime",
                "float64": "decimal"
            },
            "except": {
                "25195420522772": "int",
                "28904987445396": "int",
                "29276570401556": "int",
                "29276198281492": "int",
                "29276105521044": "int",
                "29276100335508": "int",
                "20947766232980": "int",
                "20947745134356": "int",
                "20789097773204": "int",
                "28083771248020": "varchar(max)",
                "metadata.system.latitude": "decimal(10,6)",
                "metadata.system.longitude": "decimal(10,6)"
            },
            "date_fields": [
                "created_at",
                "due_at",
                "updated_at",
                "content.original_message.source.original_message_timestamp",
                "content.original_message.received"
            ]
        }
        ```

        :param str table: _description_
        :param dict[str, str] credentials: _description_
        :param dict[str, dict[str,str]] type_mapping: _description_, defaults to {}
        """
        self.connect_zendesk(credentials)
        self.mapping_dict: dict = type_mapping

        self.table = table
        self.table_columns: set[str] = set()
        self.VERBOSE = False
        self.id_cache = None

    def reconnect(self):
        """Attempts to reconnect to the database and to Zendesk
        """
        self.connect_db(self.db_credentials)
        self.connect_zendesk(self.credentials)

    def type_list(self, obj_dict: dict) -> list[dict[str, any]]:
        """List with column name, value and types extracted from given object dictionary.

        :param dict obj_dict: dictionary
        :return list[dict[str, any]]: list of dictionaries
        """
        df = pd.json_normalize(obj_dict)
        pd_types = df.dtypes.to_dict()
        return [
            {
                'column': key,
                'value': val,
                'type': self.map_type(key, str(pd_types[key]))
            }
            for key, val in df.iloc[0].to_dict().items()
                if val is not None
        ]

    @staticmethod
    def flatten_dict(d: dict, key: str|list[str]) -> dict:
        """Recusively flattens a dictionary (dictionaries inside of dictionaries are also flattened).

        Lists with dictionaries are turned into dicts with keys using `key`.

        If `key` is a list, it attempts to use each item from the list in order as a key.

        :param dict d: dictionary to be flattened
        :param str | list[str] key: key or keys to be used in keying lists of dictionaries
        :return dict: flatenned dictionary
        """
        return { k: Zendesk.flatten_dict_list(v, key) for k, v in d.items() }

    @staticmethod
    def flatten_dict_list(dlist: list[dict], key: str|list[str]) -> dict:
        """Flattens list of dictionaries recursively

        :param list[dict] dlist: list of dictionaries
        :param str | list[str] key: key(s) to be extrated from the dictionaries and turned into dict keys
        :return dict: flattened dictionary
        """
        wrong_type = lambda x: type(x)!=list and type(x)!=dict

        if wrong_type(dlist) or (type(dlist)==list and len(dlist) > 0 and wrong_type(dlist[0])):
            return dlist

        if type(dlist)==dict:
            return Zendesk.flatten_dict(dlist, key)

        def choose_key(d: dict) -> str:
            if type(key)==str: return key
            for k in key:
                if k in d: return k

        flattened = { d[choose_key(d)]: Zendesk.flatten_dict_list(d, key) for d in dlist }

        for v in flattened.values():
            v.pop(choose_key(v))

        return flattened

    def connect_zendesk(self, credentials: dict[str, str]):
        """
        Creates a connection to the Zendesk API using the given credentials.

        See http://docs.facetoe.com.au/zenpy.html
        
        API credentials format:

        ```json
        {
            "email": EMAIL,
            "token": API_TOKEN,
            "subdomain": SUBDOMAIN
        }
        ```

        Note: password connection to the Zendesk API has been deprecated.

        :param dict[str, str] credentials: JSON string with Zendesk API credential information
        """
        self.credentials: dict[str, str] = credentials
        self.client: Zenpy = Zenpy(**self.credentials)

    def connect_db(self, db_credentials: str) -> db.Connection:
        """Connects to SQL database using pyodbc.

        See https://github.com/mkleehammer/pyodbc/wiki/Getting-started

        Credentials string sample:

        ```
        Driver={SQL Server};
        Server=SERVER_IP;
        Database=ZENDESK;
        UID=USER_ID;
        PWD=PASSWORD;
        Trusted_Connection=no;
        ```

        :param str db_credentials: string with SQL database connection information
        :return db.Connection: `pyodbc` connection to the SQL database
        """
        self.db_credentials = db_credentials
        self.db = db.connect(self.db_credentials)
        return self.db

    def sql_update_str(self, type_list: list[dict[str, any]], id: str) -> str:
        """Creates SQL query to update row with `id` using values from the `Zendesk.type_list` function

        :param list[dict[str, any]] type_list: output from type_list function
        :param str id: row ID
        :return str: SQL query
        """
        parsed_values: dict[str, str] = dict(map(Zendesk.parse_value, type_list))
        return f"update {self.table} set {', '.join([f'[{c}]={v}' for c, v in parsed_values.items()])} where id={id if type(id)==int else f"'{id}'"}"

    def sql_insertion_str(self, columns: str, values: str) -> str:
        """Inserts new row into table.

        :param str columns: columns with new values
        :param str values: values for the columns
        :return str: SQL query
        """
        return f'insert into {self.table} ({columns}) values ({values})'

    def has_table(self, table: str) -> bool:
        """Returns `True` if the connected database contains the given table.

        :param str table: name of the table to check for
        :return bool: True if table exists
        """
        return bool(self.db.cursor().tables(table=table, tableType='TABLE').fetchone())

    def add_columns(self, type_list: list[dict[str, str]]):
        """Adds columns to the table `self.table` using the given `type_list`.

        :param list[dict[str, str]] type_list: output from the `Zendesk.type_list` function
        """
        for t in type_list:
            col_name, col_type = t['column'], t['type']
            if not self.has_column(col_name):
                self.vp(f'Column {col_name} ({col_type}) does not exist, adding it now.')
                self.add_column(col_name, col_type)
                self.cache_table_columns()

    @staticmethod
    def _normalized_fields(dlist: list[dict[str, any]]) -> dict[int, any]:
        """Normalizes the Zendesk fields data.

        :param list[dict[str, any]] dlist: list of fields
        :return dict[int, any]: normalized (flattened) dictionary
        """
        if not dlist: return {}
        return { f['id']:f['value'] for f in dlist }
    
    def vp(self, txt: str):
        """Verbose print.
        Only prints if `self.VERBOSE` is `True`

        :param str txt: text to be passed onto `ic`
        """
        if self.VERBOSE: ic(txt)

    def commit(self, tries=10):
        """Wrapper for pyodbc `Connection.commit`

        While unsuccessful:
        - reconnects to the database
        - retries `tries` number of times.

        :param int tries: number of times to retry the commit, defaults to 10
        """
        try:
            self.db.commit()
        except db.Error as pe:
            self.vp(f"Error: {pe}")
            self.vp('Retrying commit')
            try:
                self.reconnect()
                self.commit(tries - 1)
            except:
                self.vp("Couldn't reconnect")

    def execute(self, sql_query: str, tries=10, is_first=True):
        """Executes the given SQL query.

        Returns result of query includes a `SELECT`.

        :param str sql_query: SQL query
        :param int tries: number of times to retry, defaults to 10
        :param bool is_first: used by the recursion algorithm to determine first run, defaults to True
        :return list|None: same return as `Connection.execute`
        """
        self.vp(f'> Executing SQL query to {self.table}')

        if tries==0:
            return self.db.execute(sql_query)

        try:
            r = self.db.execute(sql_query)
            if not is_first:
                self.vp(f">   Execute successful to {self.table}")
            return r
        except db.Error as pe:
            self.vp(f">   Error: {pe}")
            try:
                self.vp(f'>   Retrying execute to {self.table} ({tries} attempts left)')
                return self.execute(sql_query, tries - 1, False)
            except:
                self.vp(f">   Couldn't execute to {self.table}")
                return

    def map_type(self, column: str, pd_type: str) -> str:
        """Maps column with Pandas datatype to SQL datatype.

        :param str column: column name
        :param str pd_type: datatype assigned by Pandas
        :return str: string for SQL data type
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

    def translated_custom_fields(self, ticket: Ticket) -> dict[str, str]:
        tdict = ticket.to_dict()
        custom_fields: dict[str, str|int|float] = {}
        for f in tdict['custom_fields']:
            custom_fields[self.client.ticket_fields(id=f['id']).title] = f['value']
        return custom_fields

    def append_obj(self, obj_dict: dict, recache=True, force=False) -> bool:
        """Appends generic dictionary to table.

        If `force` is set to `True`, object will be updated if its ID already exists in the table.

        :param dict obj_dict: dictionary to be appended
        :param bool recache: whether to recache table ids, defaults to True
        :param bool force: force append object, defaults to False
        :return bool: if append was successful
        """
        id=obj_dict['id']
        type_list = self.type_list(obj_dict)
        if id in self.get_table_ids(recache=recache, type_list=type_list):
            self.vp(f"{obj_dict['id']} already in {self.table}")
            if not force:
                return False
            self.vp("Appending object anyway")

        self.add_columns(type_list)

        columns, values = self.sql_columns_and_values(type_list)

        sql_query = self.sql_insertion_str(columns, values)

        if id in self.get_table_ids(recache):
            sql_query = self.sql_update_str(type_list, id)

        self.id_cache.add(obj_dict['id'])

        self.execute(sql_query=sql_query)
        self.commit()
        return True

    @staticmethod
    def iso_date_to_datetime(iso_date: str) -> datetime:
        """Converts Zendesk's [ISO date format](https://developer.zendesk.com/api-reference/introduction/requests/) to `datetime` type.

        :param str iso_date: ISO date string
        :return datetime: 
        """
        return date_parser.parse(iso_date).astimezone(tz.tzlocal())

    def create_table(self, type_list: list[dict[str, str]]):
        """Creates table `table` using `type_list` for column names and types.

        :param list[dict[str, str]] type_list: list of dictionaries with `column` and `type` keys
        """
        cursor = self.db.cursor()
        columns = ', '.join([f'[{t['column']}] {t['type']}' for t in type_list])
        cursor.execute(f'CREATE TABLE [{self.table}]({columns})')
        cursor.commit()
        self.connect_zendesk(self.credentials)
    
    def alter_fields(self, type_list: list[dict[str, str]]):
        """Changes column types based on given dict list

        :param list[dict[str, str]] type_list: list of dictionaries that have `'column'` and `'type'` keys
        """
        for t in type_list:
            self.execute(f'alter table [{self.table}] alter column [{t['column']}] {t['type']}')
        self.commit()
    
    def end_db_connection(self):
        self.db.close()
    
    def get_table_ids(self, recache=True, type_list: list[dict]=[]) -> set[int]:
        """Retrieves ID column from given table.

        :param bool recache: if True, table IDs weill be recached through a call to the SQL table.
        :param bool type_list: type list from sample dict, used if table does not exist.
        :return set: set with IDs
        """
        if recache or self.id_cache is None:
            try:
                ids = self.execute(f'select [id] from [{self.table}]')
                self.id_cache = set(i[0] for i in ([] if ids is None else ids.fetchall()))
            except db.Error as ex:
                error_code = ex.args[0]
                if error_code == '42S02':
                    self.create_table(type_list=type_list)
        return self.id_cache

    def sql_columns_and_values(self, type_list: list[dict[str, str]]) -> tuple[str, str]:
        return f"[{'], ['.join([t['column'] for t in type_list])}]", ', '.join(dict(map(Zendesk.parse_value, type_list)).values())

    @staticmethod
    def parse_value(data: dict) -> tuple[str, str]:
        """Converts python values into valid SQL values.

        Expects dictionary with `column`, `value` and `type` keys.
        
        :param dict[str, str] data: original python data
        :return tuple[str, str]: tuple of column name and SQL-type
        """
        c = data['column']
        v = data['value']
        t = data['type']

        if (v is None) or (type(v)==str and len(v)==0):
            return (c, 'NULL')
        if t in {'datetime', 'date'}:
            try:
                d = Zendesk.iso_date_to_datetime(v).strftime('%Y-%m-%d %H:%M:%S')
                return (c, f"'{d}'")
            except:
                return (c, 'NULL')
        if t == 'bit':
            return (c, str(int(v)))
        if 'int' in t:
            return (c, f'{v}')
        return (c, f"N'{str(v).replace("'", '"')}'")

    def cache_table_columns(self):
        """Caches table columns from SQL database
        """
        select_columns_query = f"select column_name from information_schema.columns where TABLE_NAME='{self.table}'"
        self.table_columns = {
            cn[0] for cn in self.execute(select_columns_query).fetchall()
        }
    
    def get_table_columns(self):
        if not len(self.table_columns):
            self.cache_table_columns()
        return self.table_columns
    
    def has_column(self, column_name: str):
        return column_name in self.get_table_columns()

    def add_column(self, col_name: str, col_type: str):
        self.execute(f'alter table [{self.table}] add [{col_name}] {col_type} NULL')
        self.db.commit()

    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    def get_tickets(self, updated_at_after: str):
        """Retrieves tickets with `updated_at` after `updated_at_after` date string.

        Does _not_ include tickets created exactly _at_ `updated_at_after`.

        :param str updated_at_after: date string in the format `"%Y-%m-%d %H:%M:%S"`
        :return Generator: zenpy client search generator
        """
        return self.client.search(
            type='ticket',
            updated_at_after=datetime.strptime(
                    updated_at_after,
                    Zendesk.DATE_FORMAT
                ),
            sort_by='updated_at',
            sort_order='asc'
        )

    def single_ticket_fetch(self, updated_at_after: str) -> Ticket:
        """Returns first ticket with `updated_at` date after given `updated_at_after` date.

        :param str updated_at_after: reference date in the format `"%Y-%m-%d %H:%M:%S"`
        :return Ticket: first ticket to be updated after `udpated_at_after` date
        """
        return self.get_tickets(updated_at_after=updated_at_after).next()

    def get_ticket(self, id: int) -> Ticket:
        return self.client.tickets(id=id)

    def get_deleted_tickets(self):
        return self.client.tickets.deleted(sort_by='deleted_at', sort_order='asc')

    def select_tickets_open_for_over(self, days=30) -> pd.DataFrame:
        return pd.read_sql_query(
            f"SELECT id FROM ZENDESK.DBO.Tickets WHERE DATEDIFF(DD, created_at, GETDATE()) > {days} AND [status]!='closed'",
            self.db
        )
