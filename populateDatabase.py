from typing import Iterator, Dict, Any, Optional
from stringio import StringIteratorIO
import pandas as pd
import psycopg2
import numpy as np
import math


class populateDB():
    def __init__(self,
                 sql_config,
                 allowed_meta_cols_fields):
        self.sql_config = sql_config
        self.connetcdatabase()
        self.allowed_meta_cols_fields = allowed_meta_cols_fields

    def connetcdatabase(self):
        self.conn = psycopg2.connect(
            host=self.sql_config['host'],
            database=self.sql_config['database'],
            user=self.sql_config['username'],
            password=self.sql_config['password'])

    def clean_csv_value(self, value: Optional[Any]) -> str:
        if value in ['None', "NaT"]:
            value=None
        if value is None:
            return r'\N'
        return str(value).replace('\n', '\\n')

    def copy_string_iterator(self,
                             gene_dcm: Iterator[Dict[str, Any]],
                             size: int = 8192) -> None:
        with self.conn.cursor() as cursor:
            gene_dcm_string_iterator = StringIteratorIO((
                '|'.join(map(self.clean_csv_value,
                         tuple(list(dcm.values())))) + '\n'
                for dcm in gene_dcm
            ))
            cursor.copy_from(gene_dcm_string_iterator,
                             self.sql_config['table_name'],
                             sep='|',
                             size=size)

    def parse_values(self, df):
        df = self.parse_floats(df)
        df = self.parse_strings(df)
        df = self.parse_time(df)
        df = self.parse_ints(df)
        df = self.parse_time_stamp(df)
        df['rowid'] = df.index
        #if self.dict_mata != None:
        df = df[list(self.allowed_meta_cols_fields.keys())]
        return df

    def parse_floats(self, df):
        float_keys = [k for k, v in
                      self.allowed_meta_cols_fields.items()
                      if v.startswith('float')]
        for key in float_keys:
            df = df.replace({key: {"None": None}})
        return df
    
    def parse_ints(self, df):
        float_keys = [k for k, v in
                      self.allowed_meta_cols_fields.items()
                      if v.startswith('int')]
        for key in float_keys:
            #if key == "NumberOfFrames":
            #    print('pik')
            df[key] = df[key].astype(str)
            df[key] = df[key].apply(
                lambda x: x if x=='None' else int(float(x)))
        return df

    def parse_strings(self, df):
        var_keys = [k for k, v in
                    self.allowed_meta_cols_fields.items()
                    if v.startswith(('TEXT', 'varchar'))]
        for key in var_keys:
            df[key] = df[key].str.replace("\r", "r")
            df[key] = df[key].str.replace("\\", "")
            #df[key] = df[key].apply(lambda s: unicode(s, "utf-8"))
        return df

    def parse_time(self, df):
        time_keys = [k for k, v in
                     self.allowed_meta_cols_fields.items()
                     if v.startswith('time')]
        for key in time_keys:
            df[key] = df[key].astype(str)
            df[key] = df[key].apply(lambda x: '{0:0<6}'.format(x))
            df[key] = df[key].apply(lambda x: x[0:8])
            df.replace({key: {np.nan: 0}}, inplace=True)
        return df

    def parse_time_stamp(self, df):
        time_keys = [k for k, v in
                     self.allowed_meta_cols_fields.items()
                     if v.startswith('TIMESTAMP')]
        for key in time_keys:
            df[key] = df[key].astype(str)
            df.replace({'TimeStamp': {"NaT": None}}, inplace=True)
        return df

    def __call__(self, df):
        liste = []
        for key in self.allowed_meta_cols_fields.keys():
            val = self.allowed_meta_cols_fields[key]
            string = "\"" + key + "\"" + ' ' + val
            liste.append(string)
        col_vals = ",\n".join(liste)
        self.cursor = self.conn.cursor()
        sql = """
            CREATE TABLE {} (
                {}
            );
            """.format(self.sql_config['table_name'], col_vals)
        self.cursor.execute(sql)
        self.cursor.execute('COMMIT;')
        df = df.where(pd.notnull(df), None)
        df = self.parse_values(df)
        records = df.to_dict('records')
        generator = (y for y in records)
        self.cursor.close()
        self.copy_string_iterator(generator)
        self.conn.close()
