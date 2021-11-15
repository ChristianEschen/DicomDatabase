from BuildDatabase import PreDcmLoader
from populateDatabase import populateDB
from dicom_dict_db import get_meta_dicom
import os
import time
import argparse
from shutil import rmtree
import psycopg2


parser = argparse.ArgumentParser(
    description='Define inputs for building database.')
parser.add_argument(
    '--input_folder', type=str,
    help="The path for the input (assumened) falletenet directory")
parser.add_argument(
    '--temp_dir', type=str,
    help="temp dir")
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
    help="table name in database")
parser.add_argument(
    '--num_workers', type=int,
    help="The number of workers to use")
parser.add_argument(
    '--dicom_reader_backend', type=str,
    default='pydicom',
    help="The backend to use [pydicom | sitk]")
parser.add_argument(
    '--para_method', type=str,
    default='system_call',
    help="the way to do parallelization ['system_call | concurrent_futures]")


class UpdateDatabase():
    def __init__(self, input_folder, temp_dir,
                 sql_config,
                 dicom_reader_backend, num_workers,
                 para_method,
                 table_name_update):
        self.dicom_reader_backend = dicom_reader_backend
        self.input_folder = input_folder
        self.temp_dir = temp_dir
        self.csv_folder = os.path.join(temp_dir, "csv_folder")
        self.csv_dcm_folder = os.path.join(temp_dir,
                                           "csv_dcm_folder")
        self.allowed_meta_cols_fields = get_meta_dicom()
        self.num_workers = num_workers
        self.para_method = para_method
        self.sql_config = sql_config
        self.table_name_update = table_name_update

    def connectDB(self):
        self.conn = psycopg2.connect(
            host=self.sql_config['host'],
            database=self.sql_config['database'],
            user=self.sql_config['username'],
            password=self.sql_config['password'])

    def buildDatabase(self):
        preDcmLoader = PreDcmLoader(
            self.input_folder, self.temp_dir, sql_config=self.sql_config,
            dicom_reader_backend=self.dicom_reader_backend,
            num_workers=self.num_workers,
            para_method=self.para_method)
        preDcmLoader()
        df = preDcmLoader.appendDataframes()
        df = df.drop_duplicates(
            ['PatientID', 'StudyInstanceUID',
             'SeriesInstanceUID', 'SOPInstanceUID'])
        populater = populateDB(
            sql_config=sql_config,
            allowed_meta_cols_fields=get_meta_dicom()
                           )
        populater(df)
        rmtree(self.temp_dir)

    def __call__(self):
        self.buildDatabase()

        #TODO load original database

        self.connectDB()
        
        temp_table2 = "temp_table2"

        sql = """
        DROP SEQUENCE IF EXISTS s;
        """
        self.cursor = self.conn.cursor()
        self.cursor.execute(sql)
        self.cursor.execute("COMMIT;")
        self.cursor.close()

        # Create table with rows not in original table
        sql = """
        SELECT * INTO temp_table2 FROM dicom_table_temp
        WHERE ("PatientID", "StudyInstanceUID", "SeriesInstanceUID", "SOPInstanceUID") NOT IN
            (SELECT "PatientID", "StudyInstanceUID", "SeriesInstanceUID", "SOPInstanceUID" FROM {table});
        """.format(table=self.table_name_update)

        self.cursor = self.conn.cursor()
        self.cursor.execute(sql)
        self.cursor.execute("COMMIT;")
        self.cursor.close()

        sql_count = """
        SELECT COUNT("DcmPathFlatten") FROM {};
        """.format(self.table_name_update)
        self.cursor = self.conn.cursor()
        self.cursor.execute(sql_count)
        number_of_rows = self.cursor.fetchone()
        number_of_rows = number_of_rows[0] + 1
        self.cursor.execute("COMMIT;")
        self.cursor.close()

        sql_sequnece = """
        CREATE SEQUENCE s OWNED BY {}."rowid";
        """.format(self.table_name_update)
        self.cursor = self.conn.cursor()
        self.cursor.execute(sql_sequnece)
        self.cursor.execute("COMMIT;")
        self.cursor.close()

        sql = """
        ALTER SEQUENCE s RESTART WITH {};
        """.format(number_of_rows)

        self.cursor = self.conn.cursor()
        self.cursor.execute(sql)
        self.cursor.execute("COMMIT;")
        self.cursor.close()

        sql = """
        UPDATE {} SET "rowid"=nextval('s');
        """.format(temp_table2)

        self.cursor = self.conn.cursor()
        self.cursor.execute(sql)
        self.cursor.execute("COMMIT;")
        self.cursor.close()

        sql = """
        INSERT INTO {table1}
           SELECT * FROM {table2};
        """.format(table1=self.table_name_update,
                   table2=temp_table2)
        self.cursor = self.conn.cursor()

        self.cursor.execute(sql)
        self.cursor.execute("COMMIT;")
        self.cursor.close()

        # get new paths

        sql = """
        SELECT "DcmPathFlatten", "PatientID", "StudyInstanceUID",
        "SeriesInstanceUID", "SOPInstanceUID" FROM {}
        """.format(self.sql_config['table_name'])
        self.cursor = self.conn.cursor()
        self.cursor.execute(sql)
        data = self.cursor.fetchall()
        # update paths

        sql = """
            UPDATE {table_name}
            SET
                "DcmPathFlatten" = "dicom_table_temp"."DcmPathFlatten"
                FROM "dicom_table_temp"
            WHERE
                {table_name}."PatientID" = dicom_table_temp."PatientID" AND
                {table_name}."StudyInstanceUID"= dicom_table_temp."StudyInstanceUID" AND
                {table_name}."SeriesInstanceUID"= dicom_table_temp."SeriesInstanceUID" AND
                {table_name}."SOPInstanceUID"= dicom_table_temp."SOPInstanceUID"
                ;
        """.format(table_name=self.table_name_update)
        self.cursor = self.conn.cursor()
        self.cursor.execute(sql)
        self.cursor.execute("COMMIT;")
        self.cursor.close()

        sql = """
        DROP TABLE {temp_table2};
        """.format(
            temp_table2=self.sql_config['table_name'])

        self.cursor = self.conn.cursor()
        self.cursor.execute(sql)
        self.cursor.execute("COMMIT;")
        self.cursor.close()

        sql = """
        DROP TABLE {temp_table1};
        """.format(
            temp_table1=temp_table2)

        self.cursor = self.conn.cursor()
        self.cursor.execute(sql)
        self.cursor.execute("COMMIT;")
        self.cursor.close()

        self.conn.close()

if __name__ == '__main__':
    start_time = time.time()
    args = parser.parse_args()
    input_folder = args.input_folder
    temp_table_name = 'dicom_table_temp'
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
                  'dicom_table_temp'}

    num_workers = args.num_workers
    temp_dir = args.temp_dir
    para_method = args.para_method
    dicom_reader_backend = args.dicom_reader_backend

    Updater = UpdateDatabase(
        input_folder, temp_dir, sql_config=sql_config,
        dicom_reader_backend=dicom_reader_backend,
        num_workers=num_workers,
        para_method=para_method,
        table_name_update=args.table_name)
    Updater()
    print('done updating database')
    end = time.time()
    print('time elapsed:', end - start_time)


## GRAVEYARD

#  def __call__(self):
#         self.buildDatabase()

#         #TODO load original database

#         self.connectDB()
        
#         temp_table = "temp_table"
#         temp_table2 = "temp_table2"

#         sql = """
#         DROP SEQUENCE IF EXISTS s;
#         """
#         self.cursor = self.conn.cursor()
#         self.cursor.execute(sql)
#         self.cursor.execute("COMMIT;")
#         self.cursor.close()

#         # Create table with rows not in original table
#         sql = """
#         SELECT * INTO {} FROM dicom_table_temp
#         WHERE "DcmPathFlatten" NOT IN
#             (SELECT "DcmPathFlatten" FROM dicom_table);
#         """.format(temp_table2)
#         self.cursor = self.conn.cursor()
#         self.cursor.execute(sql)
#         self.cursor.execute("COMMIT;")
#         self.cursor.close()

#         sql_count = """
#         SELECT COUNT("DcmPathFlatten") FROM dicom_table;
#         """.format(temp_table2)
#         self.cursor = self.conn.cursor()
#         self.cursor.execute(sql_count)
#         number_of_rows = self.cursor.fetchone()
#         number_of_rows = number_of_rows[0] + 1
#         self.cursor.execute("COMMIT;")
#         self.cursor.close()

#         sql_sequnece = """
#         CREATE SEQUENCE s OWNED BY dicom_table."rowid";
#         """
#         self.cursor = self.conn.cursor()
#         self.cursor.execute(sql_sequnece)
#         self.cursor.execute("COMMIT;")
#         self.cursor.close()

#         sql = """
#         ALTER SEQUENCE s RESTART WITH {};
#         """.format(number_of_rows)

#         self.cursor = self.conn.cursor()
#         self.cursor.execute(sql)
#         self.cursor.execute("COMMIT;")
#         self.cursor.close()

#         sql = """
#         UPDATE {} SET "rowid"=nextval('s');
#         """.format(temp_table2)

#         self.cursor = self.conn.cursor()
#         self.cursor.execute(sql)
#         self.cursor.execute("COMMIT;")
#         self.cursor.close()

#         sql_1 = """
#         CREATE TABLE {temp_table} AS
#             SELECT *
#                 FROM {table1}

#             UNION

#             SELECT *
#                 FROM {table2};
#         """.format(temp_table=temp_table,
#                    table1=self.table_name_update,
#                    table2=temp_table2)

#         self.cursor = self.conn.cursor()
#         self.cursor.execute(sql_1)
#         self.cursor.execute("COMMIT;")
#         self.cursor.close()
    
#         sql_2 = """
#         DROP TABLE {table1} CASCADE;
#         """.format(table1=self.table_name_update)

#         self.cursor = self.conn.cursor()
#         self.cursor.execute(sql_2)
#         self.cursor.execute("COMMIT;")
#         self.cursor.close()

#         sql_3 = """
#         CREATE TABLE {table1} AS
#         TABLE {temp_table};
#         """.format(temp_table=temp_table,
#                    table1=self.table_name_update)
            
#         self.cursor = self.conn.cursor()
#         self.cursor.execute(sql_3)
#         self.cursor.execute("COMMIT;")
#         self.cursor.close()
    
#         sql = """
#         DROP TABLE {table1} CASCADE;
#         """.format(table1=temp_table)

#         self.cursor = self.conn.cursor()
#         self.cursor.execute(sql)
#         self.cursor.execute("COMMIT;")
#         self.cursor.close()

#         sql = """
#         DROP TABLE {table1} CASCADE;
#         """.format(table1=temp_table2)

#         self.cursor = self.conn.cursor()
#         self.cursor.execute(sql)
#         self.cursor.execute("COMMIT;")
#         self.cursor.close()
    
#         sql = """
#         DROP TABLE {table1} CASCADE;
#         """.format(table1=self.sql_config['table_name'])

#         self.cursor = self.conn.cursor()
#         self.cursor.execute(sql)
#         self.cursor.execute("COMMIT;")
#         self.cursor.close()
    
#         self.conn.close()