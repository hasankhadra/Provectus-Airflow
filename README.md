# Task 1 - Mapreduce with Airflow

_Provectus-Internship, 2021_

In this project, we implemented MapReduce paradigm in Apache Aiflow to calculate the word count for a given dataset. 

## Table of contents
1. [ Structure of the code ](#struct)
2. [ Installation and Running ](#install)

<a name="struct"></a>
### 1. Structure of the code
We used the Twitter dataset `tweets.csv`. The solution works for any `.csv` dataset. We used Custom operators for Initializer, Mapper and Reducer tasks. This is the graph of the dag we used: 

<p align="center">
<img src="https://i.ibb.co/3mkqNMb/airflow.png" width="500" height="400"/>
</p>

As we can see we used one initializer task, 3 mapper tasks and one reducer task. The data was shared between different tasks using cross-communications (XComs). The declaration of the dag is in 
[`mapred.py`](https://github.com/hasankhadra/Provectus-Airflow/blob/dev_1/mapred.py) where all the tasks and their dependencies are defined. The files 
[`init_operator.py`](https://github.com/hasankhadra/Provectus-Airflow/blob/dev_1/init_operator.py), 
[`map_operator.py`](https://github.com/hasankhadra/Provectus-Airflow/blob/dev_1/map_operator.py), and 
[`red_operator.py`](https://github.com/hasankhadra/Provectus-Airflow/blob/dev_1/red_operator.py) contain the implementation of each task (Initializer, Mapper, Reducer respectively). The output of the workflow is stored in 
[`output.json`](https://github.com/hasankhadra/Provectus-Airflow/blob/dev_1/output.json).

<a name="install"></a>
### 2. Installation and Running
  1. First you need to install airflow. Follow the steps in this [tutorial](https://airflow.apache.org/docs/apache-airflow/stable/start/local.html).

  2. `cd` to `AIRFLOW_HOME` (the same one you chose during step 1) and run the following commands:
   
  ```
  mkdir dags
  cd dags
  ```
    
  3. Clone this repo to your local machine in the same directory you're at now.
  4. Open 2 new terminals (Now you have 3 in total). In the first one run:

  ```
  airflow scheduler
  ```
  
  In order to track the execution of the dag, you can use the airflow webserver UI. First you need to create an admin user. In the second terminal run:
  
  ```
  airflow users create \
    --username test \
    --firstname test \
    --lastname test \
    --role Admin \
    --email test@test.test
  ```
  
  You'll be prompted to insert a password, you'll use the same password when loging in to the webserver. 
  
  Now in the same terminal run the webserver:
  
  ```
  airflow webserver
  ```
  
  You can login through the link: http://localhost:8080/. Search for the dag with name `FrequencyCount` and track its process.
  
  5. Now, we're ready to run the dag. In the the 3rd terminal run:

  ```
  airflow dags backfill FrequencyCount --start-date 2015-06-01
  ```
After running the command, the file `output.json` will be overwritten (or created in case it wasn't created before) with the results of the dag. The file will be created in the same directory you are currently in.
