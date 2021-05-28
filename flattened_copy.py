from BaseDcmLoader import BaseDcmLoader

if __name__ == '__main__':
    input_dir = "/home/sauroman/dicom_database/test_data/dataset"
    output_dir = "/home/sauroman/dicom_database/test_data/dataset_flattened"
    fdf = BaseDcmLoader()
    fdf.flattened_copy(
        input_path=input_dir,
        output_path=output_dir)

    print('done flattened copy')
