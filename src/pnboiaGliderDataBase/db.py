
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
from urllib.parse import quote

class GetData():

    load_dotenv()

    def __init__(self, host=None, database=None, user=None, password=None, conn=None):

        self._host = host
        self._db = database
        self._user = user
        self._psw = password

        if conn:
            self.conn = conn
        else:
            self.conn = self.engine_create()

    def get(self, table=None, limit=None, join="", start_date=None, end_date=None, last=None, query=None, **kwargs):

        if not query:
            query = f"SELECT * FROM {table}{join} WHERE true"

            if start_date:
                query += f" AND date_time >= '{start_date}'"
            if end_date:
                query += f" AND date_time <= '{end_date}'"
            if kwargs:
                query = self.create_query(query, kwargs)
            if limit:
                query += f" LIMIT {limit}"
            if last:
                query += " ORDER BY date_time DESC LIMIT 1"

            print(query)

        df = pd.read_sql(query, self.conn)

        return df

    def post(self, table, schema, data, overwrite=False,**kwargs):
        if kwargs:
            query = ""
            query = self.create_query(query, kwargs)
            if overwrite:
                self.delete(table=table, schema=schema, query=query)

        data.to_sql(con=self.conn, name=table, schema=schema, if_exists='append', index=False)
        print(f'{data.shape[0]} rows inserted in table {schema}.{table}')
        print(f'({str(data.date_time.iloc[0])} to {str(data.date_time.iloc[-1])})')

    def delete(self, table, schema, query):

        query = f"DELETE FROM {schema}.{table} WHERE true {query}"
        self.conn.execute(query)
        print(f"deleted data using query {query}")

    def create_query(self, query, kwargs):
        for key, value in kwargs.items():
            if type(value[1]) == list:
                if len(value[1]) == 1:
                    query += f" AND {key} {value[0]} ('{value[1][0]}')"
                else:
                    query += f" AND {key} {value[0]} {tuple(value[1])}"
            else:
                query += f" AND {key} {value[0]} '{value[1]}'"

        return query

    def engine_create(self):

        password = quote(self._psw)

        engine = create_engine(f"postgresql+psycopg2://{self._user}:{password}@{self._host}/{self._db}")

        return engine
