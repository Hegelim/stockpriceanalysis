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

### Q2.2 (1)

### Q2.2 (2) 

### Q2.2 (3)

### Q2.2 (4)

### Q2.2 (5)

