from BaseDcmLoader import BaseDcmLoader


def test_flattening_copy():
    input_path = 'test_data/dataset'
    loader = BaseDcmLoader()
    filenames = loader.get_recursive_files(input_path)
    dcm_files = loader.validate_dicom(filenames)

    assert len(dcm_files) == 2864


def test_recursive():
    input_path = 'test_data/dataset_recursive'
    loader = BaseDcmLoader()
    filenames = loader.get_recursive_files(input_path)
    dcm_files = loader.validate_dicom(filenames)

    assert len(dcm_files) == 2864
