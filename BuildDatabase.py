from BaseDcmLoader import BaseDcmLoader
import os
import numpy as np
import pandas as pd
import concurrent.futures
import psycopg2
import argparse
import time
from shutil import copyfile
from shutil import rmtree
from typing import Iterator, Dict, Any, Optional
from stringio import StringIteratorIO
from populateDatabase import populateDB

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


class PreDcmLoader(BaseDcmLoader):
    def __init__(self, input_folder, temp_dir,
                 sql_config,
                 dicom_reader_backend, num_workers,
                 para_method):
        self.dicom_reader_backend = dicom_reader_backend
        self.input_folder = input_folder
        self.temp_dir = temp_dir
        self.csv_folder = os.path.join(temp_dir, "csv_folder")
        self.csv_dcm_folder = os.path.join(temp_dir,
                                           "csv_dcm_folder")
        self.allowed_meta_cols_fields = {
            'rowid': 'BIGINT PRIMARY KEY',
            'DcmPathFlatten': 'varchar(255)',
            'SeriesDate': 'varchar (255)',
            'StudyDate': 'varchar (255)',
            'TimeStamp': 'TIMESTAMP',
            'DateStamp': 'varchar(255)',
            'StudyTime': 'time',
            'SeriesTime': 'time',
            'BodyPartExamined': 'varchar(255)',
            'Modality': 'varchar(255)',
            'PatientID': 'varchar(255)',
            'StudyInstanceUID': 'varchar(255)',
            'SeriesInstanceUID': 'varchar(255)',
            'SOPInstanceUID': 'varchar(255)',
            'StudyDescription': 'varchar(255)',
            'SeriesDescription': 'varchar(255)',
            'AdditionalPatientHistory': 'varchar(255)',
            'Manufacturer': 'varchar(255)',
            'InstitutionalName': 'varchar(255)',
            'NumberOfFrames': 'int8',
            'FrameIncrementPointer': 'varchar(255)',
            'FrameTime': 'float8',
            'PositionerMotion': 'varchar(255)',
            'DistanceSourceToPatient': 'float8',
            'DistanceSourceToDetector': 'float8',
            'PositionerPrimaryAngle': 'float8',
            'PositionerSecondaryAngle': 'float8',
            'CineRate': 'float8',
            'labels': 'int8',
            'labels_transformed': 'int8',
            'predictions': 'int8',
            'confidences': 'TEXT []'
            }
        self.num_workers = num_workers
        self.para_method = para_method
        self.sql_config = sql_config
        if dicom_reader_backend == 'pydicom':
            from loaders.pydicom_loader import pydicom_loader
            self.loader = pydicom_loader(
                self.input_folder, self.allowed_meta_cols_fields)
        elif dicom_reader_backend == 'sitk':
            from loaders.sitk_loader import sitk_loader
            self.loader = sitk_loader(self.input_folder, self.allowed_meta_cols_fields)
        else:
            raise ValueError('Backend not recognized')

    def split_csv_files(self, input_files, num_workers=8):
        def batch(iterable, n=1):
            liste = len(iterable)
            for ndx in range(0, liste, n):
                yield iterable[ndx:min(ndx + n, liste)]
        batch_size = int(np.ceil(len(input_files)/num_workers))
        batches = batch(input_files, n=batch_size)
        return batches

    def writeFileNames(self, input_files, csvfile_path):
        batches = self.split_csv_files(input_files, self.num_workers)
        c = 0
        self.mkFolder(csvfile_path)

        csv_files = [
            os.path.join(csvfile_path, str(counter) + '.csv')
            for counter in range(0, self.num_workers)]

        for batch in batches:
            df = pd.DataFrame(batch, columns=['FileName'])
            df.to_csv(csv_files[c], index=False)
            c += 1
        return csv_files

    def create_filenames_in_batches(self, input_folder, csv_folder):
        self.input_files = self.get_recursive_files(input_folder)
        self.input_files = [str(i.absolute()) for i in self.input_files]
        self.input_files = [
            i for i in self.input_files if os.path.isdir(i) is False]
        csv_files = self.writeFileNames(self.input_files, csv_folder)
        return csv_files

    def preload_dcm(self, csv_files):
        self.csv_dcm_files = [
            os.path.join(self.csv_dcm_folder, str(c) + '.csv')
            for c in range(0, self.num_workers)]
        if self.num_workers != 1:
            if self.para_method == 'system_call':
                self.systemCallPara(csv_files)

            elif self.para_method == 'concurrent_futures':
                with concurrent.futures.ProcessPoolExecutor(
                        max_workers=self.num_workers) as executor:
                    executor.map(self.loader.loadParallelDicom, csv_files,
                                 self.csv_dcm_files)
            else:
                raise ValueError('This paralization method is not implemented:', self.para_method)
        else:
            self.loader.loadSerialDicom(csv_files, self.csv_dcm_files)

    def systemCallPara(self, csv_files):
        if self.dicom_reader_backend == 'pydicom':
            backend_script = 'pydicom_loader.py'
        else:
            backend_script = 'sitk_loader.py'
        import subprocess
        for i in range(0, len(self.csv_dcm_files)):
            csv_file = csv_files[i]
            csv_dcm_file = self.csv_dcm_files[i]
            script ='python ' + os.getcwd() + os.sep + 'loaders' + os.sep + \
                backend_script + ' --recursive_folder ' + self.recursive_folder + \
                ' --input_file ' + csv_file  + ' --output_file ' + csv_dcm_file
            subprocess.Popen(script, shell=True,
                stdin=None, stdout=None, stderr=None, close_fds=True)
        while len(os.listdir(self.csv_dcm_folder)) != self.num_workers:
            time.sleep(1)
        return

    def appendDataframes(self):
        li = []
        for filename in self.csv_dcm_files:
            df = pd.read_csv(filename, index_col=None,
                             header=0, dtype=str)
            li.append(df)
        return pd.concat(li, axis=0, ignore_index=True)

    def copy_files_serial(self, df):
        inputList = list(df['DcmPathFlatten'])
        outputList = list(df['RecursiveFilePath'])
        self.mkFolder(os.path.dirname(self.recursive_folder))
        for idx in range(0, len(inputList)):
            if (idx % 10000) == 0:
                if idx != 0:
                    print('nr files copied:', idx)
            self.mkFolder(os.path.dirname(outputList[idx]))
            copyfile(inputList[idx], outputList[idx])

    def compareLists(self, df, names):
        differences = set(
            df.columns.to_list()).difference(names)
        missing_fields = set(df.columns.to_list()).intersection(differences)
        return missing_fields

    def __call__(self):
        self.maybResetFolder(self.csv_folder)
        self.maybResetFolder(self.csv_dcm_folder)
        self.csv_files = self.create_filenames_in_batches(self.input_folder,
                                                          self.csv_folder)

        self.preload_dcm(self.csv_files)


if __name__ == '__main__':
    start_time = time.time()
    args = parser.parse_args()
    input_folder = args.input_folder
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
    num_workers = args.num_workers
    temp_dir = args.temp_dir
    para_method = args.para_method
    dicom_reader_backend = args.dicom_reader_backend
    temp_csv_file = os.path.join(args.temp_dir, 'temp_csv.csv')

    preDcmLoader = PreDcmLoader(
        input_folder, temp_dir, sql_config=sql_config,
        dicom_reader_backend=dicom_reader_backend,
        num_workers=num_workers,
        para_method=para_method)
    preDcmLoader()
    df = preDcmLoader.appendDataframes()
    populater = populateDB(
        sql_config=sql_config,
        allowed_meta_cols_fields=preDcmLoader.allowed_meta_cols_fields
                           )
    populater(df)

    rmtree(temp_dir)
    print('done buiding database')
    end = time.time()
    print('time elapsed:', end - start_time)