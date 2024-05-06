import pandas as pd
import sqlite3
import argparse
import os
import numpy as np
import time
from sqlalchemy import event, create_engine
import psycopg2
from populateDatabase import populateDB
from pats_dict_db import get_meta_pats

parser = argparse.ArgumentParser(
    description='Define inputs for building database.')
parser.add_argument(
    '--PatsPath', type=str,
    help="Pats path to file")
parser.add_argument(
    '--database', type=str,
    help="database name")
parser.add_argument(
    '--username', type=str,
    help="username for database")
parser.add_argument(
    '--password', type=str,
    help="password for database")
parser.add_argument(
    '--host', type=str,
    help="host for database")
parser.add_argument(
    '--port', type=str,
    help="port for database")
parser.add_argument(
    '--table_name', type=str,
    help="table name for database")


class BuildPats():
    def __init__(self, sql_config, PatsPath):
        start = time.time()
        self.sql_config = sql_config
        dtype_dict = {"dbt": np.float32}
        self.df_pats = pd.read_csv(PatsPath, sep='\t',
                                   dtype=dtype_dict)
        self.df_pats = pd.read_csv(PatsPath, sep='\t',
                                   dtype=str)
        # z = zip(self.df_pats_not_str.columns.to_list(), self.df_pats_not_str.dtypes.to_list())
        # print(*list(z), sep='\n')
        self.preparePats()
        self.reOrdercols()
        pupulater = populateDB(sql_config=self.sql_config,
                               allowed_meta_cols_fields=get_meta_pats())
        pupulater(self.df_pats_no_dup_loc)
        end = time.time()
        print('time elapsed', end-start)
        print('done')

    def preparePats(self):
        self.df_pats = self.df_pats[~self.df_pats[
            'procedurestart_lokalbed_velse'].isna()]
        self.get_cols_mult_entry()

    def get_cols_mult_entry(self):
        cols = self.df_pats.columns.to_list()
        cols.remove('bth_pid')
        cols.remove('procedurestart_lokalbed_velse')
        ddf = self.df_pats.groupby(
            ['bth_pid', 'entryid', 'procedurestart_lokalbed_velse'])[
                cols].nunique()
        nunique = ddf.nunique()
        cols_to_drop = nunique[nunique == 1].index
        self.cols_mult_entry = list(set(cols) - set(cols_to_drop.to_list()))

    def reOrdercols(self):
        df_stenose = self.df_pats.pivot(
            index=['bth_pid', 'entryid',
                   'procedurestart_lokalbed_velse',
                   'sub_entryid'],
            columns=['proceduresite'],
            values=['stenosegrad_eyeball']).add_prefix('Sten %: ')
        df_stenose.columns = df_stenose.columns.droplevel()
        df_stenose = df_stenose.reset_index()

        df_ffr = self.df_pats.pivot(
            index=['bth_pid', 'entryid',
                   'procedurestart_lokalbed_velse',
                   'sub_entryid'],
            columns=['proceduresite'],
            values=['ffr']).add_prefix('ffr %: ')
        df_ffr.columns = df_ffr.columns.droplevel()
        df_ffr = df_ffr.reset_index()

        df_kartype = self.df_pats.pivot(
            index=['bth_pid', 'entryid',
                   'procedurestart_lokalbed_velse',
                   'sub_entryid'],
            columns=['proceduresite'],
            values=['kartype']).add_prefix('kartype: ')
        df_kartype.columns = df_kartype.columns.droplevel()
        df_kartype = df_kartype.reset_index()
        # Merge the above dfs

        df_pats_location = df_stenose.merge(
            df_ffr,
            left_on=['bth_pid', 'procedurestart_lokalbed_velse', 'entryid'],
            right_on=['bth_pid', 'procedurestart_lokalbed_velse', 'entryid'],
            how='inner')

        df_pats_location = df_pats_location.merge(
            df_kartype,
            left_on=['bth_pid', 'procedurestart_lokalbed_velse', 'entryid'],
            right_on=['bth_pid', 'procedurestart_lokalbed_velse', 'entryid'],
            how='inner')

        df_pats_location = df_pats_location.drop_duplicates(
            ['bth_pid', 'entryid',
             'procedurestart_lokalbed_velse',
             'sub_entryid'])
        # Merge with self.df_pats
        df_pats_no_dup = self.df_pats.drop_duplicates(
            ['bth_pid', 'procedurestart_lokalbed_velse', 'entryid'])

        df_pats_no_dup_loc = df_pats_no_dup.merge(
            df_pats_location,
            left_on=['bth_pid', 'procedurestart_lokalbed_velse', 'entryid'],
            right_on=['bth_pid', 'procedurestart_lokalbed_velse', 'entryid'],
            how='inner')
        self.df_pats_no_dup_loc = df_pats_no_dup_loc.drop(
            columns=['proceduresite', 'ffr',
                     'stenosegrad_eyeball',
                     'sub_entryid_y', 'sub_entryid_x', 'kartype'])

        # self.engine= create_engine("sqlite:///"+ self.DataBasePath)
        # @event.listens_for(self.engine, 'before_cursor_execute')
        # def plugin_bef_cursor_execute(conn, cursor, statement, params, context,executemany):
        #     if executemany:
        #         cursor.fast_executemany = True  # replace from execute many to fast_executemany.
        #         cursor.commit()
        start =time.time()
        # conn = self.engine.execute()
        self.df_pats_no_dup_loc.rename(
            columns={'procedurestart_lokalbed_velse': "timestamp", 'bth_pid':'bth_pid'},
            inplace=True)
        self.df_pats_no_dup_loc['timestamp']=pd.to_datetime(
            self.df_pats_no_dup_loc['timestamp'])
        self.df_pats_no_dup_loc.drop(
            columns=['kartype: nan', 'ffr %: nan', 'Sten %: nan'],
            inplace=True)

        # # parse some values
        # self.df_pats_no_dup_loc['hjerteklapoperation_type'] = \
        #     self.df_pats_no_dup_loc['hjerteklapoperation_type']. \
        #     astype(str)
        # self.df_pats_no_dup_loc['ekkodato'] = \
        #     self.df_pats_no_dup_loc['ekkodato']. \
        #     astype(str)
        # self.df_pats_no_dup_loc['othercardiacprocedures'] = \
        #     self.df_pats_no_dup_loc['othercardiacprocedures']. \
        #     astype(str)
        # self.df_pats_no_dup_loc['kontrastmiddelnavn'] = \
        #     self.df_pats_no_dup_loc['kontrastmiddelnavn']. \
        #     astype(str)
        # self.df_pats_no_dup_loc['intraaorticballoonpumpused'] = \
        #     self.df_pats_no_dup_loc['intraaorticballoonpumpused']. \
        #     astype(str)
        # self.df_pats_no_dup_loc['andrerisikofaktorer'] = \
        #     self.df_pats_no_dup_loc['andrerisikofaktorer']. \
        #     astype(str)
        # self.df_pats_no_dup_loc['type'] = \
        #     self.df_pats_no_dup_loc['type']. \
        #     astype(str)
        # self.df_pats_no_dup_loc['rimaanastomosetilsegment1'] = \
        #     self.df_pats_no_dup_loc['rimaanastomosetilsegment1']. \
        #     astype(str)
        # self.df_pats_no_dup_loc['aepiploicatilsegment'] = \
        #     self.df_pats_no_dup_loc['aepiploicatilsegment']. \
        #     astype(str)
        # self.df_pats_no_dup_loc['proceduredato'] = \
        #     self.df_pats_no_dup_loc['proceduredato']. \
        #     astype(str)
        # self.df_pats_no_dup_loc["ventetid_beregnes"] = \
        #     self.df_pats_no_dup_loc["ventetid_beregnes"]. \
        #     astype(str)
        # # self.df_pats_no_dup_loc['entryid'] = \
        # #     self.df_pats_no_dup_loc['entryid']. \
        # #     astype(str)
        # self.df_pats_no_dup_loc["antalpakke_r"] = \
        #     self.df_pats_no_dup_loc["antalpakke_r"]. \
        #     astype(str)
        # self.df_pats_no_dup_loc["senestami"] = \
        #     self.df_pats_no_dup_loc["senestami"]. \
        #     astype(str)
        # self.df_pats_no_dup_loc['ejectionfractionvalueifknown'] = \
        #     self.df_pats_no_dup_loc['ejectionfractionvalueifknown']. \
        #     astype(str)
        # self.df_pats_no_dup_loc["senestepci"] = \
        #     self.df_pats_no_dup_loc["senestepci"]. \
        #     astype(str)
        # self.df_pats_no_dup_loc["antaltidligerecabg"] = \
        #     self.df_pats_no_dup_loc["antaltidligerecabg"]. \
        #     astype(str)
        # self.df_pats_no_dup_loc["senestecabg"] = \
        #     self.df_pats_no_dup_loc["senestecabg"]. \
        #     astype(str)
        # self.df_pats_no_dup_loc["lukkedevice"] = \
        #     self.df_pats_no_dup_loc["lukkedevice"]. \
        #     astype(str)


if __name__ == '__main__':
    
    args = parser.parse_args()

    sql_config = {'database':
                  args.database,
                  'username':
                  args.username,
                  'password':
                  args.password,
                  'host':
                  args.host,
                  'port':
                  args.port,
                  'table_name':
                  args.table_name
                  }
    
    sql_config = {'database':
                  "mydb",
                  'username':
                  "alatar",
                  'password':
                  "123qweasd",
                  'host':
                  "localhost",
                  'port':
                  5432,
                  'table_name':
                  "test_deploy"
                  }
    merger = BuildPats(
        sql_config,
        args.PatsPath)
