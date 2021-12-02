#!/bin/bash

mkdir logs ; mkdir minio ; sudo chmod -R 777 minio ; mkdir pgadmin ; sudo chmod -R 777 pgadmin  ; echo -e "AIRFLOW_UID=$(id -u)" > .env

