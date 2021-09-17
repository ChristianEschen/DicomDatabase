import pandas as pd
import pydicom
import numpy as np
import os
import argparse
import time
from pathlib import Path
import os
import re


parser = argparse.ArgumentParser(
    description='Define inputs for building database.')
parser.add_argument(
    '--input_file', type=str,
    help="The path for the input file")
parser.add_argument(
    '--output_file', type=str,
    help="The path for the output file")



class pydicom_loader():
    def __init__(self, recursive_folder, input_folder):
        self.input_folder = input_folder
        self.recursive_folder = recursive_folder
        self.allowed_meta_cols_fields = [
            'DcmPathFlatten', 'TimeStamp', 'BodyPartExamined', 'DateStamp'
            'Modality', 'PatientID', 'StudyInstanceUID',
            'SeriesInstanceUID', 'SOPInstanceUID', 'Organ',
            'StudyDescription', 'SeriesDescription',
            'AdditionalPatientHistory',
            'Manufacturer', 'ContrastBolusAgent', 'StudyDate',
            'StudyTime', 'SeriesTime', 'SeriesDate', 'InstitutionalName',
            'RecursiveFilePath',
            # KAG
            'NumberOfFrames', 'FrmeIncrementPointer', 'PositionerMotion',
            'DistanceSourceToPatient', 'DistanceSourceToDetector',
            'PositionerPrimaryAngle', 'PositionerSecondaryAngle',
            'CineRate', 'FrameTime']

    def getRelateivePath(self, dcm_file):
        self.base_path = os.path.basename(os.path.normpath(self.input_folder))
        match = re.search(self.base_path, dcm_file)
        start_pos = match.regs[0][0]
        dcm_file = dcm_file[start_pos:]
        return dcm_file

    def loadParallelDicom(self, csv_file, dcm_csv_file):
        df = pd.read_csv(csv_file)
        dcm_list = df['FileName'].tolist()
        self.meta_cols = {col: [] for col in self.allowed_meta_cols_fields}
        for dcm_file_name in dcm_list:
            try:
                dcm = pydicom.dcmread(dcm_file_name)
                dcm_file_name_write = self.getRelateivePath(dcm_file_name)
                setattr(dcm, 'DcmPathFlatten', dcm_file_name_write)
                for col in self.meta_cols:
                    if hasattr(dcm, col):
                        self.meta_cols[col].append(str(getattr(dcm, col)))
                    else:
                        self.meta_cols[col].append(np.nan)
            except Exception as ex:
               print('Exception occured:', ex, 'in dcm-file:', dcm_file_name)
        df_data = pd.DataFrame.from_dict(self.meta_cols)
        df_data = self.create_recursive_paths_pydicom(df_data)

        df_data = self.setTimeStamp(df_data)
        df_data = df_data.astype(str)
        # save data
        df_data.to_csv(dcm_csv_file, index=False)
        return None

    def loadSerialDicom(self, csv_list, csv_dcm_list):
        for i in range(0, len(csv_list)):
            df = pd.read_csv(csv_list[i])
            dcm_list = df['FileName'].tolist()
            self.meta_cols = {col: [] for col in self.allowed_meta_cols_fields}
            for dcm_file_name in dcm_list:
                df_data = pd.DataFrame.from_dict(self.meta_cols)
                df_data = self.create_recursive_paths_pydicom(df_data)
                df_data = self.setTimeStamp(df_data)
                df_data = df_data.astype(str)
                # save data
                df_data.to_csv(csv_dcm_list[i], index=False)
            return df_data

    def create_recursive_paths_pydicom(self, df):
        df['RecursiveFilePath'] = os.sep \
            + df['PatientID'] + os.sep + \
            df['StudyInstanceUID'] + os.sep + \
            df['SeriesInstanceUID'] + \
            os.sep + df['SOPInstanceUID'] + '.dcm'
        # PatientID, StudyInstanceUID, SeriesInstanceUID, SOPInstanceUID
        return df

    def setTimeStamp(self, df):
        df['TimeStamp'] = df['SeriesTime']
        df.replace({'TimeStamp': {np.nan: 0}}, inplace=True)
        df.replace({'TimeStamp': {0: ''}}, inplace=True)
        df.replace({'TimeStamp': {'': 0}}, inplace=True)

        df['DateStamp'] = df['SeriesDate']
        df.replace({'DateStamp': {np.nan: 0}}, inplace=True)
        df.replace({'DateStamp': {0: ''}}, inplace=True)
        df.replace({'DateStamp': {'': 0}}, inplace=True)

        df['TimeStamp'] = \
            pd.to_datetime(
                df['DateStamp'].astype(float).astype(int).
                astype(str).apply(
                    lambda x: '{0:0>8}'.format(x)) +
                df['TimeStamp'].astype(float).astype(int).
                astype(str).apply(
                    lambda x: '{0:0>6}'.format(x)), format='%Y%m%d%H%M%S',
                errors='coerce')
        return df

    def writeDcmFiles(self, df):
            for i in range(0, len(df)):
                row = df.iloc[i]
                dcm = pydicom.dcmread(row['DcmPathFlatten'])
                dcm['PatientID'].value = row['bth_pid']
                dcm.save_as(row['OutputPath'])

    def setOutputPath(self, df, output_path):
        df['OutputPath'] = df['DcmPathFlatten'].apply(lambda x:  os.path.join(output_path, Path(x).name))
        return df



if __name__ == '__main__':
    start_time = time.time()
    args = parser.parse_args()
    input_file = args.input_file
    output_file = args.output_file
    recursive_folder = args.recursive_folder
    dicom_load  =pydicom_loader(recursive_folder)
    dicom_load.loadParallelDicom(input_file, output_file)
