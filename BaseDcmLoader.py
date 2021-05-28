import os
from pathlib import Path
import pydicom
from shutil import copyfile
import shutil


class BaseDcmLoader:
    def get_recursive_files(self, input_path):
        filenames = []
        path = Path(input_path)
        for p in path.rglob("*"):
            filenames.append(p)
        return filenames

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
