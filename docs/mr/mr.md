# Intro
[Exam Subject](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)  
[MapReduce Paper](https://pdos.csail.mit.edu/6.824/papers/mapreduce.pdf)

![Execution overview](../images/mr%20-%20Execution%20overview.png)

# Core Principle: Consistency of Understanding - Should all map task be done before starting first reduce task?
When one reduce task is ready to start, it needs input. Input of reduce task is the output of map task, which means reduce task requires map task output. So does it require mere some outputs of map tasks or **ALL** outputs? The answer is **ALL**. Every map task can generate data for some few specific reduce tasks by calculating the hash of data and retrieving the id of reduce task. Only when all the map tasks are done, the resources required in one reduce task are ready.

> ### 3.1 Execution Overview  
> The Map invocations are distributed across multiple
machines by automatically partitioning the input data into a set of M splits. The input splits can be processed in parallel by different machines. Reduce invocations are distributed by partitioning the intermediate key
space into R pieces using a partitioning function (e.g.,
*hash(key)* **mod** R). The number of partitions (R) and
the partitioning function are specified by the user.

# Let's start from *Data Structures*
> The master keeps several data structures. For each map
task and reduce task, it stores the state (*idle*, *in-progress*, or *completed*), and the identity of the worker machine (for non-idle tasks).  
> The master is the conduit through which the location
of intermediate file regions is propagated from map tasks
to reduce tasks. Therefore, for each completed map task,
the master stores the locations and sizes of the R intermediate file regions produced by the map task. Updates to this location and size information are received as map tasks are completed. The information is pushed incrementally to workers that have in-progress reduce tasks.

Actually, I did start from the data structure. At that point time when I set about implementation, I had no idea of what to do. I read the paper back and forth and found the upper remarks. I realized I needed to define the data structure first. Then I can do with these data structures next.

## Task Structure
The core concept in MapReduce is map & reduce. Coordinator and workers need to exchange essential information. In programming model, there should be places to carry information. We define a structure called `Task` to store *Id*, *WorkerId*, *Type*, *Input*, *Output*, etc.

### Task ID
Every task needs an Id to represent itself. When worker gets a map task, executes the task and feeds task with some inputs which the task object brings, it is about to tell the coordinator the result of this task. Coordinator has to know which task the worker is responding about. Otherwise, result of one task would probably be computed or cumulated twice or more which would be a terrible thing because there will be many map, reduce tasks in such distributed chaos.

### Worker ID
There are single coordinator and several workers in this map reduce system. At runtime, many workers are wanting to communicate with coordinator. Then every worker needs a Id to let coordinator able to identify who he is.  
This Id should be allocated from coordinator. Coordinator is responsible to maintain these Ids and make sure one Id is exactly assigned to single worker.  
Then in coordinator, we need a `GetWorkerId` method. We maintain a `maxWorkerId` and atomically increase this value on every call so that each call to this method will get an unique Id.  
In worker, we need a method to call `GetWorkerId` of coordinator via *RPC* and store the result locally in order to tell the coordinator *who I am* on next other call.

### Task Type
We unify map and reduce task into one structure. So it's necessary to define a field to distinct map from reduce.

### Input
Map task transfers the file split from coordinator to worker.