import os
from pathlib import Path
import pydicom
from shutil import copyfile
import shutil
import pydicom
from pydicom.errors import InvalidDicomError
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
#from test_for_monai_transformable import get_transform, test_transform_of_path
from monai.transforms import Compose, LoadImaged, EnsureChannelFirstD, Resized, DeleteItemsd, SpatialPadd, RepeatChanneld, ScaleIntensityd, NormalizeIntensityd, EnsureTyped, ToDeviced, RandSpatialCropd, ConcatItemsd, Identityd
import os
import numpy as np
import time
import shelve
import csv
import pandas as pd
def is_dicom_file(filepath):
    try:
        # Attempt to read the file with pydicom, only the metadata
        pydicom.dcmread(filepath, stop_before_pixels=True)
        return True, filepath
    except InvalidDicomError:
        return False, filepath
    except Exception as e:
        # If there are issues like permission errors or the file does not exist
        print(f"Error reading {filepath}: {e}")
        return False, filepath



class BaseDcmLoader:

    def try_to_process(self, path):
        try:
            self.train_transforms(path)
            return True
        except:
             return False
        
    def test_transform_of_path(self, path_total):
      #  path_total = str(path_total)
        images = [path_total,]
        labels = np.array([0], dtype=np.int64)
        train_files = [
            {"DcmPathFlatten": img, "label": label} for img, label in zip(images, labels)]
        maybeTransform= self.try_to_process(train_files)
        return maybeTransform, path_total
  


    def write_cache_csv_list_file(self, filenames_csv_path, filenames):
        df = pd.DataFrame(filenames)
        df.to_csv(filenames_csv_path, header=False, index=False)

    def read_csv_if_exists(self, csv_path):
        data = pd.read_csv(csv_path, header=None, index_col=False)
        return data.iloc[:, 0].tolist()


    def get_recursive_files(self, input_path):
        filenames_can_list = Path(os.path.join(self.temp_dir, "filenames_can_list.csv"))
        filenames_dicom_list = Path(os.path.join(self.temp_dir, "filenames_dicom_list.csv"))
        filenames_monai_list = Path(os.path.join(self.temp_dir, "filenames_monai_list.csv"))



        start = time.time()
        filenames_can = []
        path = Path(input_path)
        if filenames_can_list.exists():
            print('filenaes candiate files already exists')
            filenames_can = self.read_csv_if_exists(filenames_can_list)
        else:
            print('list of candidates does not exists, scanning...')
            for p in path.rglob("*"):
                if os.path.isdir(str(p)) is False:
                    
                    filenames_can.append(p)
        self.write_cache_csv_list_file(filenames_can_list, filenames_can)


        print('nr files that are candidates', len(filenames_can))
        stop = time.time()
        print('time elapsed for scanning files in folder that are not directories', stop-start)
        start = time.time()
        filenames = []
        if filenames_dicom_list.exists():
            print('list of dicom files exists, continue!')

            filenames = self.read_csv_if_exists(filenames_dicom_list)

        else:
            print('list of dicom files does not exists, processing...')

            with ProcessPoolExecutor() as executor:
                results = executor.map(is_dicom_file, filenames_can)
                for is_dicom, filepath in results:
                    if is_dicom:
                        filenames.append(filepath)
        self.write_cache_csv_list_file(filenames_dicom_list, filenames)
        print('nr files that can be loaded with pydicom:', len(filenames))
        stop = time.time()
        print('time elapsed for pydicom reader:', stop-start)
        start = time.time()


        # try without test monai trainsformable
        filenames_transformable = []
        if filenames_monai_list.exists():
            print('transformable list already exists')
            filenames_transformable = self.read_csv_if_exists(filenames_monai_list)
        else:
            print('list of monai transformable files does not exists, processing...')

            with ProcessPoolExecutor() as executor:
          #  with ThreadPoolExecutor() as executor:

                results = executor.map(self.test_transform_of_path, filenames)
                for is_transformable, filepath in results:
                    if is_transformable:
                        filenames_transformable.append(filepath)
        self.write_cache_csv_list_file(filenames_monai_list, filenames_transformable)

        print('nr files that are transformables:', len(filenames_transformable))
        stop = time.time()
        print('time elapsed for monai transformable:', stop-start)

        return filenames_transformable

    def validation_dcm_file(self, dcm_path):
        try:
            pydicom.dcmread(dcm_path)
            isValid = True
        except Exception as ex:
            print('Dicom read exception:', ex)
            print('cannot read dcm file:', dcm_path)
            isValid = False
        return isValid

    def validate_dicom(self, dcm_paths):
        valid_dcm_paths = []
        for dcm_path in dcm_paths:
            if os.path.isdir(dcm_path) is False:
                try:
                    pydicom.dcmread(dcm_path)
                    valid_dcm_paths.append(dcm_path)
                except Exception as ex:
                    print('Dicom read exception:', ex)
                    print('cannot read dcm file:', dcm_path)
        return valid_dcm_paths

    def copy_files(self, input_files, output_path):
        self.maybResetFolder(output_path)
        output_files = []
        i = 0
        for input_file in input_files:
            output_file = os.path.join(output_path, str(i) + input_file.name)
            copyfile(input_file, output_file)
            output_files.append(output_file)
            i += 1
        return output_files

    def flattened_copy(self, input_path, output_path):
        input_files = self.get_recursive_files(input_path)
        input_files = self.validate_dicom(input_files)
        self.output_files = self.copy_files(input_files, output_path)
        return

    def mkFolder(self, dir):
        if not os.path.exists(dir):
            os.makedirs(dir)

    def maybResetFolder(self, dir):
        if os.path.exists(dir):
            shutil.rmtree(dir)
            os.makedirs(dir)
        else:
            os.makedirs(dir)
