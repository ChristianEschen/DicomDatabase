import pandas as pd
import SimpleITK as sitk
import numpy as np
import os
import argparse

parser = argparse.ArgumentParser(
    description='Define inputs for building database.')
parser.add_argument(
    '--input_file', type=str,
    help="The path for the input file")
parser.add_argument(
    '--output_file', type=str,
    help="The path for the output file")
parser.add_argument(
    '--recursive_folder', type=str,
    help="The path for recursive folder")


class sitk_loader():
    def __init__(self, recursive_folder):
        self.recursive_folder = recursive_folder
        self.allowed_meta_cols_fields = {
            '0010|0020': 'PatientID',
            '0018|0015': 'BodyPartExamined',
            '0008|0060': 'Modality',
            '0020|000D': 'StudyInstanceUID',
            '0020|000E': 'SeriesInstanceUID',
            }

    def setTimeStampByTag(self, df):
        df['TimeStamp'] = df['0008|0031']
        df.replace({'TimeStamp': {'': 0}}, inplace=True)
        df.replace({'TimeStamp': {np.nan: 0}}, inplace=True)
        df['DateStamp'] = df['0008|0021']
        df.replace({'DateStamp': {'': 0}}, inplace=True)
        df.replace({'DateStamp': {np.nan: 0}}, inplace=True)
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

    def loadParallelDicom(self, csv_file, dcm_csv_file):
        df = pd.read_csv(csv_file)
        dcm_list = df['FileName'].tolist()
        key_list = []
        value_list = []
        df = pd.DataFrame(columns=[])

        for dcm_file_name in dcm_list:
            try:
                reader = sitk.ImageFileReader()
                reader.LoadPrivateTagsOn()
                reader.SetFileName(dcm_file_name)
                reader.ReadImageInformation()
                for k in reader.GetMetaDataKeys():
                    key_list.append(k)
                    key_list.append('DcmPathFlatten')
                    v = reader.GetMetaData(k)
                    value_list.append(v)
                    value_list.append(dcm_file_name)
                dicts = dict(zip(key_list, value_list))
                df_data = pd.DataFrame(dicts, index=[0])
                df = df.append(df_data)
            except Exception as ex:
                print('Exception occured:', ex, 'in dcm-file:', dcm_file_name)
        df = self.setTimeStampByTag(df)
        df = df.astype(str)
        df = self.remove_surrogates(df)
        df = self.create_recursive_paths(df)
        # save data
        df.to_csv(dcm_csv_file, index=False)
        return df

    def loadSerialDicom(self, csv_list, csv_dcm_list):
        for i in range(0, len(csv_list)):
            df = pd.read_csv(csv_list[i])
            dcm_list = df['FileName'].tolist()
            key_list = []
            value_list = []
            df = pd.DataFrame(columns=[])

            for dcm_file_name in dcm_list:
                try:
                    reader = sitk.ImageFileReader()
                    reader.LoadPrivateTagsOn()
                    reader.SetFileName(dcm_file_name)
                    reader.ReadImageInformation()
                    for k in reader.GetMetaDataKeys():
                        key_list.append(k)
                        key_list.append('DcmPathFlatten')
                        v = reader.GetMetaData(k)
                        value_list.append(v)
                        value_list.append(dcm_file_name)
                    dicts = dict(zip(key_list, value_list))
                    df_data = pd.DataFrame(dicts, index=[0])
                    df = df.append(df_data)
                except Exception as ex:
                    print('Exception occured:',
                          ex, 'in dcm-file:', dcm_file_name)
            df = self.setTimeStampByTag(df)
            df = df.astype(str)
            df = self.remove_surrogates(df)
            df = self.create_recursive_paths(df)
            # save data
            df.to_csv(csv_dcm_list[i], index=False)
            return df

    def create_recursive_paths(self, df):
        df['RecursiveFilePath'] = self.recursive_folder + os.sep \
            + df['0010|0020'] + os.sep + \
            df['0020|000d'] + os.sep + \
            df['0020|000e'] + \
            os.sep + df['0008|0018'] + '.dcm'
        # PatientID, StudyInstanceUID, SeriesInstanceUID, SOPInstanceUID
        return df

    def remove_surrogates(self, df):
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].apply(
                    lambda x: np.nan if x == np.nan else str(x).encode(
                        'utf-8', 'replace').decode('utf-8'))
        return df

if __name__ == '__main__':
    args = parser.parse_args()
    input_file = args.input_file
    output_file = args.output_file
    recursive_folder = args.recursive_folder
    dicom_load  = sitk_loader(recursive_folder)
    dicom_load.loadParallelDicom(input_file, output_file)