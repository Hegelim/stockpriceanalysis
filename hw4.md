# Task 1

## Q1.1

### Q1.1 (1) Screenshot of terminal after successfully starting the webserver

![image-20211115114806634](C:\Users\thefi\AppData\Roaming\Typora\typora-user-images\image-20211115114806634.png)

### Q1.1 (1) Screenshot of terminal after starting scheduler

![image-20211115115021774](C:\Users\thefi\AppData\Roaming\Typora\typora-user-images\image-20211115115021774.png)

### Q1.1 (2) Screenshot of web browser after you successfully login and see the DAGs

![image-20211115115223777](C:\Users\thefi\AppData\Roaming\Typora\typora-user-images\image-20211115115223777.png)

## Q1.2

### Q1.2 (1) Screenshots of Tree, Graph, Gantt of each Executor. 

### Tree using SequentialExecutor

![image-20211115153811284](C:\Users\thefi\AppData\Roaming\Typora\typora-user-images\image-20211115153811284.png)

### Graph using Sequential Executor

![image-20211115153854038](C:\Users\thefi\AppData\Roaming\Typora\typora-user-images\image-20211115153854038.png)

### Gantt using Sequential Executor

![image-20211115154012737](C:\Users\thefi\AppData\Roaming\Typora\typora-user-images\image-20211115154012737.png)

### Gantt using Local Executor

![image-20211115155546912](C:\Users\thefi\AppData\Roaming\Typora\typora-user-images\image-20211115155546912.png)

### Q1.2 (2) Explore other features and visualization in the Airflow UI. Choose two features/visualizations, explain their functions and how they help monitor and troubleshoot the pipeline, use helloworld as an example. 

### The Calendar View

The calendar view gives you an overview of your entire DAG’s history over months, or even years. Letting you quickly see trends of the overall success/failure rate of runs over time.

![image-20211115155853165](C:\Users\thefi\AppData\Roaming\Typora\typora-user-images\image-20211115155853165.png)

### The Code View

The code view of helloworld is below. It is a quick way to get to the code that generates the DAG and provide yet more context.

![image-20211115160107539](C:\Users\thefi\AppData\Roaming\Typora\typora-user-images\image-20211115160107539.png)

# Task 2 Build workflows

## Q2.1 Implement the DAG below

### Q2.1 (1)  Screenshots of Tree and Graph

![image-20211115170821546](C:\Users\thefi\AppData\Roaming\Typora\typora-user-images\image-20211115170821546.png)

![image-20211115170900730](C:\Users\thefi\AppData\Roaming\Typora\typora-user-images\image-20211115170900730.png)

### Q2.1 (2) Manually trigger the DAG, and provide screenshots of Gantt

![image-20211115200204716](C:\Users\thefi\AppData\Roaming\Typora\typora-user-images\image-20211115200204716.png)

### Q2.1 (3) Describe how you decide the start date and schedule interval. Provide screenshots of running history after two repeats (first run + 2 repeats). 

I set the start date to be `datetime(2021, 1, 1)` just so that I can run the job immediately. I set the schedule interval to be `timedelta(minutes=30)` because we are required to run the task every 30 minutes. The running history is as follows: 

![image-20211115200520836](C:\Users\thefi\AppData\Roaming\Typora\typora-user-images\image-20211115200520836.png)

![image-20211115200754681](C:\Users\thefi\AppData\Roaming\Typora\typora-user-images\image-20211115200754681.png)

Here you can see that it successfully ran 3 times. The first run happened at 16:39, the second run happened at 17:09 which was exactly 30 minutes after the first run, and the third run happened at 19:27 because I had to stop the VM instance after the second run for dinner to save money, and you can see that I know how to schedule the run in every 30 minutes. 

## Q2.2

#### Code Screenshots

![image-20211126105602345](C:\Users\thefi\AppData\Roaming\Typora\typora-user-images\image-20211126105602345.png)

![image-20211126105637169](C:\Users\thefi\AppData\Roaming\Typora\typora-user-images\image-20211126105637169.png)

![image-20211126105705070](C:\Users\thefi\AppData\Roaming\Typora\typora-user-images\image-20211126105705070.png)

![image-20211126105732914](C:\Users\thefi\AppData\Roaming\Typora\typora-user-images\image-20211126105732914.png)

![image-20211126105756032](C:\Users\thefi\AppData\Roaming\Typora\typora-user-images\image-20211126105756032.png)

![image-20211126105818391](C:\Users\thefi\AppData\Roaming\Typora\typora-user-images\image-20211126105818391.png)

![image-20211126105838735](C:\Users\thefi\AppData\Roaming\Typora\typora-user-images\image-20211126105838735.png)

![image-20211126105859325](C:\Users\thefi\AppData\Roaming\Typora\typora-user-images\image-20211126105859325.png)

#### Design ideas

* DAGs: I used five DAGs for this hw: 

  ![image-20211126104551619](C:\Users\thefi\AppData\Roaming\Typora\typora-user-images\image-20211126104551619.png)

  * get_data_task: retrieve past 10 days' stock data. 
  * get_historical_data_task: retrieve all the historical data up to including yesterday. 
  * train_model_task: train 5 linear regression models.
  * make_prediction_task: make 5 predictions, save predictions to a list, save list to .pkl file. 
  * compute_relative_error_task: compute 5 relative errors. 

* Cross communication:

  * Initially, I planned to use `task_instance.xcom_pull` and `task_instance.xcom_push` to pass my data between each DAG. However, after spending some time exploring stack overflow and looking up Apache documentation, I realized it is a bad idea to transfer big data between DAGs, so I resorted to just save data to files and read the files later. 
  * In my code, you can see a lot of `with open()...` which are basically used to save pickle files to local directory. These files will then be read by downstream DAGs. 

* How I set up the scheduler:

  * basically, I set my `start_date=datetime(2021, 1, 1)` so that the scheduler will run immediately once I start the DAG run. 
  * I then set `schedule_interval='0 7 * * *'` so then the scheduler will run at 7am every day. 

* Workflow

  * The DAGs hierarchy is in the code example.
  * At the first run, I don't have a prediction.pkl and I don't have relative_errors.csv. Run all DAGs except compute_relative_error_task. 
  * On the second run, now I have a prediction file but I don't have a relative_error.csv, so I create the csv file with the first set of relative errors.
  * On all later runs, I have the prediction.pkl and I also have relative_errors.csv, so I can just update the csv. 

#### Results 

![image-20211126111040366](C:\Users\thefi\AppData\Roaming\Typora\typora-user-images\image-20211126111040366.png)

![image-20211126111058169](C:\Users\thefi\AppData\Roaming\Typora\typora-user-images\image-20211126111058169.png)

#### The csv file

![image-20211126111128691](C:\Users\thefi\AppData\Roaming\Typora\typora-user-images\image-20211126111128691.png)

#### Gantt

![image-20211126111202192](C:\Users\thefi\AppData\Roaming\Typora\typora-user-images\image-20211126111202192.png)

# Task 3 Written Parts

## Q3.1 Answer the question (5 pts)
### (1) What are the pros and cons of SequentialExecutor, LocalExecutor, CeleryExecutor, KubernetesExecutor? (10%)

* SequentialExecutor
  * Pros
    * Light weighted.
    * No need to set up. Comes as default.
  * Cons
    * Not ready for production.
    * Not scalable.
    * Not able to perform many tasks at once.
* LocalExecutor
  * Pros
    * Straightforward and easy to set up.
    * Offers parallelism.
  * Cons
    * Not scalable.
    * Dependent on a single point of failure. 
* CeleryExecutor
  * Pros
    * Allows for scalability.
    * Celery is responsible for managing the workers. Celery creates a new one in the case of a failure.
  * Cons
    * Requires RabbitMQ/Redis for task queuing, which is redundant with what Airflow already supports.
    * The setup is complicated due to the above mentioned dependencies.
* KubernetesExecutor
  * Pros
    * Combines the benefits of CeleryExecutor and LocalExecutor in terms of scalability and simplicity. 
    * Fine-grained control over task-allocation resources. At the task level, the amount of CPU/memory needed can be configured.
  * Cons
    * Kubernetes familiarity as a potential barrier to entry.
    * Airflow is newer to Kubernetes, and the documentation is complicated.

## Q3.2 Draw the DAG of your group project (10 pts)
### (1) Formulate it into at least 5 tasks

1. get_weather_task: retrieve the weather data. 
2. get_twitter_task: retrieve the twitter data.
3. data_cleaning_task: clean and combine the weather data and the twitter data.
4. sentiment_task: do sentiment analysis on twitter data.
5. prediction_task: predict sentiment analysis based on the weather data using regression models.

### (2) Task names (functions) and their dependencies

The hierarchy of the tasks could be formulated as follows:

get_weather_task >> get_twitter_task >> data_cleaning_task >> sentiment_task >> prediction_task

### (3) How do you schedule your tasks?

Currently, we plan to schedule it at a certain time everyday (for instance, 10am) so that we could update our models once a day. 

