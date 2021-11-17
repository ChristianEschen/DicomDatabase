import numpy as np

def get_meta_dicom():
    allowed_meta_cols_fields = {
            'rowid': 'serial PRIMARY KEY',
            'DcmPathFlatten': 'varchar(255)',
            'phase': 'varchar(255)',
            'SeriesDate': 'varchar (255)',
            'StudyDate': 'varchar (255)',
            'TimeStamp': 'TIMESTAMP',
            'DateStamp': 'varchar(255)',
            'StudyTime': 'time',
            'SeriesTime': 'time',
            'BodyPartExamined': 'varchar(255)',
            'Modality': 'varchar(255)',
            'PatientID': 'varchar(255)',
            'PatientSex': 'varchar(255)',
            'StudyInstanceUID': 'varchar(255)',
            'SeriesInstanceUID': 'varchar(255)',
            'SOPInstanceUID': 'varchar(255)',
            'SOPClassUID': 'varchar(255)',
            'StudyDescription': 'varchar(255)',
            'SeriesDescription': 'varchar(255)',
            'AcquisitionDate': 'varchar(255)',
            'ProtocolName': 'varchar(255)',
            'PatientAddress': 'varchar(255)',
            'AccessionNumber': 'varchar(255)',
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
            'labels_original': 'int8',
            'predictions': 'int8',
            'confidences': 'TEXT []'
            }
    return allowed_meta_cols_fields


def get_meta_dicom_pd():
    pandas_fields = {
            'rowid': np.in32,
            'DcmPathFlatten': str,
            'SeriesDate': 'varchar (255)',
            'TimeStamp': 'TIMESTAMP',
            'DateStamp': str,
            'StudyTime': 'time',
            'SeriesTime': 'time',
            'BodyPartExamined': str,
            'Modality': str,
            'PatientID': str,
            'StudyInstanceUID': str,
            'SeriesInstanceUID': str,
            'SOPInstanceUID': str,
            'StudyDescription': str,
            'SeriesDescription': str,
            'AdditionalPatientHistory': str,
            'Manufacturer': str,
            'InstitutionalName': str,
            'NumberOfFrames': 'int8',
            'FrameIncrementPointer': str,
            'FrameTime': 'float8',
            'PositionerMotion': str,
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
    return pandas_fields
