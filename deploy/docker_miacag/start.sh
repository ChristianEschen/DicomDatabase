sudo docker run --shm-size 20g --gpus all -it --network my_network   -v /home/alatar/miacag:/home/alatar/miacag   -v /home/alatar/DicomDatabase:/home/alatar/DicomDatabase   8500cab633a2

export DB_HOST=my-custom-postgres-instance

psql -h $DB_HOST -p 5432 -U alatar -d mydb
