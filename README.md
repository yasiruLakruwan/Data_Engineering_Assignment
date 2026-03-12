How this pipeline would scale to 1 million+ records, scheduling would be
implemented (cron / Airflow concept), partitioning or indexing strategy would
evolve, failures would be handled


- In large data sets we load data in to a storage batch wise, that way we can do the storing step by step.
  In airflow manner we can Extract Transfrom Load the data in a DAG file, we can shedule the DAG daily like, and we initialize connections on the first hand througn the airflow. It can be a http request or it can be a aws s3 like data storage. Airflow provides logs, using that we can identify the folts and status. we can use index for every column for fast retrieval.  