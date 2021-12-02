# Task 2 - Airflow with Docker-compose

_Provectus-Internship, 2021_

In this project, we implemented MapReduce paradigm in Apache Aiflow to calculate the word count for a given dataset. The input dataset is extracted from `minio`, and the results of the project are stored in `postgres` db. The whole app is dockerized through `docker-compose.yaml`

## Table of contents
1. [ Description and Structure of the code ](#struct)
2. [ Installation and Running ](#install)

<a name="struct"></a>
### 1. Description and Structure of the code

We used the Twitter dataset `tweets.csv`. The solution works for any `.csv` dataset. The data is first uploaded to minio from local file system. We used Custom operators for Minio initializer, Mapper and Reducer, and Postgres uplaoder tasks. This is the graph of the dag we used: 

<p align="center">
<img src="https://i.ibb.co/ZWZCKgB/airflow11.png" width="500" height="300"/>
</p>

As we can see we used one initializer task, 3 mapper tasks, one reducer task and a postgres task. The data was shared between different tasks using cross-communications (XComs). The declaration of the dag is in 
[`mapred.py`](https://github.com/hasankhadra/Provectus-Airflow/blob/master/mapred.py) where all the tasks and their dependencies are defined. The files 
[`init_operator.py`](https://github.com/hasankhadra/Provectus-Airflow/blob/master/init_operator.py), 
[`map_operator.py`](https://github.com/hasankhadra/Provectus-Airflow/blob/master/map_operator.py),  
[`red_operator.py`](https://github.com/hasankhadra/Provectus-Airflow/blob/master/red_operator.py), and
[`postgres_operator.py`](https://github.com/hasankhadra/Provectus-Airflow/blob/dev_2/plugins/postgres_operator.py) contain the implementation of each task (Initializer, Mapper, Reducer, Postgres respectively).

Note that for every run of the dag, the input file on `minio` gets overwritten with the new local copy. The same goes for postgres db. For every run of the dag, the table `frequency` (which contains the results) gets cleared and refilled with the new results.

<a name="install"></a>
### 2. Installation and Running
  1. Clone this repo to your local machine in the same directory you're at now.
  2. `minio` and `pgadmin` require access to some data files inside docker. We made a bash script to make it easier. In the same terminal run:

```
chmod u+x init.sh
sudo ./init.sh
```

  3. Now to run the app, run:

```
docker-compose up airflow-init
docker-compose up --build
```

  You can login through the link: http://localhost:8080/ with the following credentials: `username: airflow`, `password: airflow`. Search for the dag with name `WordCount` and track its process.
  
  5. If you want to run the process again without restarting the docker all over again, you can run the following command:

  ```
  docker-compose up airflow-run-dag
  ```
After running the command, the results of the MapReduce app will be stored in postgres db. You can log in to `pgadmin` at http://localhost:5050/. You need to create a new server with the following credentials: `server name: airflow`,`host: postgres`, `username: airflow`, `password: airflow`. Then you can check the table named `frequency` and its contents.
