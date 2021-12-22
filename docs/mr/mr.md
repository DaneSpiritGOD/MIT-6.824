# Intro
[Exam Subject](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)  
[MapReduce Paper](https://pdos.csail.mit.edu/6.824/papers/mapreduce.pdf)

![Execution overview](../images/mr%20-%20Execution%20overview.png)

# Insight
## Should all map task be done before starting first reduce task?
When one reduce task is ready to start, it needs input. Input of reduce task is the output of map task, which means reduce task requires map task output. So does it require mere some outputs of map tasks or **ALL** outputs? The answer is **ALL**. Every map task can generate data for some few specific reduce tasks by calculating the hash of data and retrieving the id of reduce task. Only when all the map tasks are done, the resources required in one reduce task are ready.

> ### 3.1 Execution Overview  
> The Map invocations are distributed across multiple
machines by automatically partitioning the input data into a set of M splits. The input splits can be processed in parallel by different machines. Reduce invocations are distributed by partitioning the intermediate key
space into R pieces using a partitioning function (e.g.,
*hash(key)* **mod** R). The number of partitions (R) and
the partitioning function are specified by the user.

## Is it possible that one task would be executed successfully twice?
No. We prevent such case from happening by atomic lock of file system. If one intermediate or final output file is created within one task by a worker. Another worker with the same task will fail when it tries to create the same-name file because the file already exists in the file system. There will never be two files with totally same path.

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

### Input & Output
Map/Reduce task transfers the file splits from coordinator to worker. Worker executor processes the file input, generated intermediate outputs by **Map** or **Reduce** function and puts the outputs inside the task structure.  
Note: *input* field is a *slice* type even though *map* task only has one file input, just because we want to make *map* and *reduce* unified together.

### No task status in `Task` structure
A task should have statuses like: *idle*, *in-progress* and *completed*. On coordinator side, they are necessary, but on worker side, there is only one status: *in-progress*. So I don't store any task status in the structure and just maintain it on coordinator side by other measures instead.

# Coordinator (Dispatcher)
## Nature of Correspondence
Workers communicates with coordinator each other through *RPC*. Then we need to expose some essential APIs in coordinator so that workers could retrieve or pass key data.

### Get ready before execution
#### GetWorkerId
As said in [Worker Id](#worker-id), we need to assign an Id to the worker as identification.

#### GetReduceCount
We also should tell the worker the total count of reduce tasks because the worker wants to partition intermediate outputs generated by *Map* task into *R* regions by *hash* function.

### Dispatch
The most significant part in MapReduce system is how to coordinate the workers with tasks. We need to assign task to worker, wait for the result from worker and cancel one task if the time it takes worker to execute the specific task is out of expectation. We also need to maintain the statuses in task pool so that all the tasks could get calculated and we can obtain a correct outcome finally.

##### *Task Pool*
I abstract `taskContainer` structure as *task pool* for *map* and *reduce* respectively. I define `idleTasks`, `inProgressTasks` and `completedTasks` of `chan *Task` type to pass tasks with distinct statuses. At runtime, many workers are applying for or committing tasks. So it's important and necessary to create a mechanism to guarantee the thread safety. In Go world, *channel* is the best practice to get rid of race condition. I add a member named *cancelWaitingForInProgressTimeout* of `*sync.Map` type as well whose function I will illustrate later.

#### Create Map Task
Coordinator is given file spits as input from program host. Each file is needed to be wrapped as `Input` into a `Task` object. Having constructed a *map task*, we put it into `idleTasks` of *map task pool*.

#### AssignTask
Assigning a reduce task should be same as well.
But we need note that time-out task will be reclaimed, it's important to 

#### ReceiveTaskOutput

#### Done