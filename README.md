# create_database_yandex_metrika
get data from yandex metrika and update database (postgreSQL) on web server.

Metrika.py - class for using yandex metrika API to create request, check status and get data. Data is transformated in pandas dataframe.
to_Database.py - class for creating ssh tunnel, connecting to databasae on webserver and saving data to DB.
Main.py - file combine all modules and classes. function could use in airflow dags like a task.
