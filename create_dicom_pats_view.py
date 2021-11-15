import psycopg2
import pandas as pd
import argparse
from pats_dict_db import get_meta_pats
from dicom_dict_db import get_meta_dicom
from psycopg2.extras import execute_batch
import tqdm

parser = argparse.ArgumentParser(
    description='Define inputs for building database.')
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
    '--table_name1', type=str,
    help="table name for database")
parser.add_argument(
    '--table_name2', type=str,
    help="table name for database")
parser.add_argument(
    '--table_name_view', type=str,
    help="table name for database")
parser.add_argument(
    '--csv_test_file', type=str,
    help="csv test file")


class CreateView():
    def __init__(self, sql_config, csv_test_file):
        self.sql_config = sql_config
        self.csv_test_file = csv_test_file
        self.connectDB()
        self.jointables()

    def connectDB(self):
        self.conn = psycopg2.connect(
            host=self.sql_config['host'],
            database=self.sql_config['database'],
            user=self.sql_config['username'],
            password=self.sql_config['password'])

    def get_test_list(self):
        df = pd.read_csv(self.csv_test_file, index_col=None,
                         header=0, dtype=str)
        return df['PatientID'].to_list()

    def update_items(self, rows_to_update):
        sql_query = """UPDATE dicom_table SET
                        phase = 'test'
                        FROM (VALUES %s) AS data ("PatientID")
                        WHERE dicom_table."PatientID" = data."PatientID"
                    """
        cur = self.conn.cursor()
        n = 100
        with tqdm(total=len(rows_to_update)) as pbar:
            for i in range(0, len(rows_to_update), n):
                psycopg2.extras.execute_values (
                cur, sql_query, rows_to_update[i:i + n], template=None, page_size=n
                )
                conn.commit()
                pbar.update(cur.rowcount)
        cur.close()
        conn.close()

    def jointables(self):
        pats_tb_col = list(get_meta_pats().keys())[1:]
        test_pid = tuple(self.get_test_list())
        dicom_tb_col = list(get_meta_dicom().keys())#[1:]
        dicom_tb_col.remove('PatientID')
        dicom_tb_col.remove('DcmPathFlatten')
        dicom_tb_col = ['DcmPathFlatten'] + dicom_tb_col
        dicom_tb_col = ['PatientID'] + dicom_tb_col

        dicom_tb_col = ["dicom_table." + "\"" + i+"\"" for i in dicom_tb_col]
        pats_tb_col = ["pats." + "\"" + i+"\"" for i in pats_tb_col]
        cols = ', '.join(dicom_tb_col+pats_tb_col)
        sql = """
        CREATE VIEW "{table3}" AS
        SELECT DISTINCT ON ("{table1}"."PatientID", "{table1}"."DcmPathFlatten")
            {cols_sel}
        FROM "{table1}"
        LEFT JOIN "{table2}"
        ON ("{table2}"."timestamp" BETWEEN "{table1}"."TimeStamp" - interval '6 hours'
            AND "{table1}"."TimeStamp" + interval '6 hours')
            AND ("{table1}"."PatientID" = "{table2}".bth_pid);
        """.format(cols_sel=cols,
                   table1=self.sql_config['table_name1'],
                   table2=self.sql_config['table_name2'],
                   table3=self.sql_config['table_name_view'],
                   test_pid=test_pid)
        self.cursor = self.conn.cursor()
        self.cursor.execute(sql)
        self.cursor.execute('COMMIT;')
        self.cursor.close()

        
        # INSERT phase "test"
        sql = """
        UPDATE dicom_table SET "phase" = 'test' WHERE "PatientID" IN %s
        """
        self.cursor = self.conn.cursor()
        self.cursor.execute(sql, (test_pid, ))
        self.cursor.execute('COMMIT;')
        self.cursor.close()
       # self.connectDB()

        #df_dicom_pats_train = pd.read_sql("""
        #     SELECT * FROM "dicom_pats_train";
        #     """, self.conn)

        # df_dicom_pats_test = pd.read_sql("""
        #     SELECT * FROM "dicom_pats_test";
        #     """, self.conn)
        # df_pats = pd.read_sql("""
        #     SELECT * FROM "pats";
        #     """, self.conn)
        
        # df_dicom = pd.read_sql("""
        #     SELECT * FROM "dicom_table";
        #     """, self.conn)
            
        #self.conn.close()

        # test_pids = df_dicom_pats_test[['rowid', 'PatientID']].drop_duplicates(['PatientID'])
        # test_pids.to_csv('test_pid.csv', index=False)
        print('done')
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
                  'table_name1':
                  args.table_name1,
                  'table_name2':
                  args.table_name2,
                  'table_name_view':
                  args.table_name_view
                  }
    merger = CreateView(
        sql_config,
        args.csv_test_file)
# GRAVEYARD
#   sql2 = """
#         CREATE VIEW "{table3}" AS
#         SELECT DISTINCT ON ("{table1}"."PatientID", "{table1}"."DcmPathFlatten")
#             {cols_sel}
#         FROM "{table1}"
#         LEFT JOIN "{table2}"
#         ON ("{table2}"."timestamp" BETWEEN "{table1}"."TimeStamp" - interval '6 hours'
#             AND "{table1}"."TimeStamp" + interval '6 hours')
#             AND ("{table1}"."PatientID" = "{table2}".bth_pid)
#         WHERE {table1}."PatientID" NOT IN {test_pid};
#         """.format(cols_sel=cols,
#                    table1=self.sql_config['table_name1'],
#                    table2=self.sql_config['table_name2'],
#                    table3=self.sql_config['table_name_train'],
#                    test_pid=test_pid)
 
#         self.cursor = self.conn.cursor()
