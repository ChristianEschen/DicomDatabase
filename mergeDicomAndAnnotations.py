import pandas as pd
import argparse
import os
import numpy as np
import time
import psycopg2
from dicom_dict_db import get_meta_dicom
from populateDatabase import populateDB

parser = argparse.ArgumentParser(
    description='Define inputs for building database.')
parser.add_argument(
    '--csv_path', type=str,
    help="Pats path to file")
parser.add_argument(
    '--query', type=str,
    help="query")
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


class MergeDcmLabels():
    def __init__(self, sql_config, query, csv_files):
        self.sql_config = sql_config
        self.csv_files = csv_files
        self.query = query

    def getDataFromDatabase(self):
        self.conn = psycopg2.connect(
            host=self.sql_config['host'],
            database=self.sql_config['database'],
            user=self.sql_config['username'],
            password=self.sql_config['password'])

        df = pd.read_sql_query(self.query, self.conn)
        if len(df) == 0:
            print('The requested query does not have any data!')
        # df['DcmPathFlatten'] = df['DcmPathFlatten'].apply(
        #             lambda x: os.path.join(self.DataSetPath, x))
        return df

    def appendDataframes(self):
        li = []
        for filename in self.csv_files:
            df = pd.read_csv(filename, index_col=None,
                             header=0, dtype=str)
            li.append(df)
        df_annotated = pd.concat(li, axis=0, ignore_index=True)
        return df_annotated

    def mergeDf_and_annotations(self):
        self.df_annotated = self.df_annotated[
            ['PatientID', 'SeriesInstanceUID',
             'StudyInstanceUID', 'SOPInstanceUID', 'labels']]
        df_merge = self.df.merge(
            self.df_annotated,
            on=['PatientID', 'SeriesInstanceUID',
                'StudyInstanceUID', 'SOPInstanceUID'],
            how='left')
        df_merge['labels'] = df_merge['labels_y']
        df_merge.drop('labels_y', axis=1, inplace=True)
        return df_merge


    def __call__(self):
        self.df = self.getDataFromDatabase()
        self.df_annotated =self.appendDataframes()
        self.df_merge = self.mergeDf_and_annotations()
        start = time.time()
        sql = """
            DROP TABLE {};
            """.format(self.sql_config['table_name'])
        self.cursor = self.conn.cursor()
        self.cursor.execute(sql)
        self.cursor.execute('COMMIT;')
        self.cursor.close()
      #  self.sql_config['table_name'] = "dicom_table2"
        pupulater = populateDB(sql_config=self.sql_config,
                               allowed_meta_cols_fields=get_meta_dicom())
        pupulater(self.df_merge)
        end = time.time()
        print('time:', end-start)

if __name__ == '__main__':
    args = parser.parse_args()
    csv_files = [
        'batch1_kag_neg_0_100/dicom_labels.csv',
        'batch1_kag_neg_100_200/dicom_labels.csv',
        'batch1_kag_neg_200_300/dicom_labels.csv',
        'batch1_kag_pos_100_200/dicom_labels.csv',
        'batch1_kag_pos_200_250/dicom_labels.csv',
        'batch1_kag_pos_250_300/dicom_labels.csv',
        'batch1_kag_pos_300_350/dicom_labels.csv',
        'batch1_kag_pos_350_400/dicom_labels.csv']
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
    csv_files = [os.path.join(args.csv_path, i) for i in csv_files]
    merger = MergeDcmLabels(
        sql_config,
        args.query,
        csv_files)
    merger()
