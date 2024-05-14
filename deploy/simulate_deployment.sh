# Simulate deployment of the model


database="mydb"
num_workers=16
input_folder="/data2/angio"
dataset_folder="/data2"

temp_dir="/temp_dir/temp_dir_monai_trans"
para_method="concurrent_futures"
dicom_reader_backend="pydicom"
username="alatar"
password="123qweasd"
host="localhost"
port=5432
table_name="monai_trans_deploy"
master_port=1237
config_path_lca_rca="/deploy/docker_files/lca_rca.yaml"
config_path_rca_sten="/deploy/docker_files/classification_config_angio_SEP_Apr26_12-52-33/classification_config_angio.yaml"
new_output_path="/deploy/docker_files/output_folder"
master_addr="127.0.1.1"
schema_name="cag"
export DB_HOST=my-custom-postgres-instance
host=$DB_HOST

python BuildDatabase.py --database $database \
--num_workers $num_workers \
--input_folder $input_folder \
--temp_dir $temp_dir \
--para_method $para_method \
--dicom_reader_backend $dicom_reader_backend \
--username $username \
--password $password \
--host $host \
--port $port \
--table_name $table_name

UPDATE cag.monai_trans_deploy
SET "PatientID" = reverse(split_part(reverse(substring("DcmPathFlatten" FROM 1 FOR strpos("DcmPathFlatten", '/DICOM') - 1)), '/', 1))
WHERE "DcmPathFlatten" LIKE '%/DICOM%';


SELECT "DcmPathFlatten",
       reverse(split_part(reverse(substring("DcmPathFlatten" FROM 1 FOR strpos("DcmPathFlatten", '/DICOM') - 1)), '/', 1)) AS new_patient_id
FROM cag.test_deploy
WHERE "DcmPathFlatten" LIKE '%/DICOM%'
LIMIT 10;

sed -i "s|^[[:space:]]*output:.*|output: \"$new_output_path\"|" "$config_path_lca_rca"
sed -i "s|^[[:space:]]*DataSetPath:.*|DataSetPath: \"$dataset_folder\"|" "$config_path_lca_rca"

export PYTHONPATH="${PYTHONPATH}:/deploy/docker_files/miacag/miacag"

# TODO maybe normalize data before GridPatchD 
torchrun --nproc_per_node=1 \
--nnodes=1 \
--node_rank=0 \
--master_addr=$master_addr \
--master_port=$master_port \
/deploy/docker_files/miacag/miacag/miacag/scripts/stenosis_regression/submit_angio.py \
--cpu "False" \
--num_workers 1 \
--config_path $config_path_lca_rca \
--table_name_input $table_name


python diagnostic_data_trans.py \
--database $database \
--username $username \
--password $password \
--host $host \
--port $port \
--table_name $table_name \
--schema_name $schema_name

# TODO # merge with data labels
# here we make some fakes:
# add some fake data between 0 and 1 as float
UPDATE cag."test_deploy" SET sten_proc_1_prox_rca_transformed = RANDOM();
UPDATE cag."test_deploy" SET sten_proc_2_midt_rca_transformed = RANDOM();
UPDATE cag."test_deploy" SET sten_proc_3_dist_rca_transformed = RANDOM();
UPDATE cag."test_deploy" SET sten_proc_6_prox_lad_transformed = RANDOM();
UPDATE cag."test_deploy" SET sten_proc_16_pla_rca_transformed = RANDOM();
torchrun --nproc_per_node=1 \
--nnodes=1 \
--node_rank=0 \
--master_addr=$master_addr \
--master_port=$master_port \
/deploy/docker_files/miacag/miacag/scripts/pretraining_downstreams/submit_angio.py \
--cpu "False" \
--num_workers $num_workers \
--config_path $config_path_rca_sten \
--table_name_input $table_name


python /deploy/docker_files/DicomAnnotation/annotate.py \
--database $database \
--username $username \
--host $host \
--table_name $table_name \
--schema_name $schema_name \
--path_dataset $dataset_folder \
--dicom_reader_backend "sitk" \
--sql_query "SELECT * FROM ?schema_name.?table_name" \
--label_name_1 "labels" \
--label_name_2 "labels_transformed" \
--prediction_name "predictions" \
--confidence_name "confidences" \
--password $password

# torchrun --nproc_per_node=1 \
# --nnodes=1 \
# --node_rank=0 \
# --master_addr=$master_addr \
# --master_port=$master_port \
# /deploy/miacag/miacag/scripts/stenosis_regression/submit_angio.py \
# --cpu "False" \
# --num_workers $num_workers \
# --config_path $config_path_rca_sten \
# --table_name_input $table_name

