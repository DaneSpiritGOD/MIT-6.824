# Intro
[Exam Subject](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)  
[MapReduce Paper](https://pdos.csail.mit.edu/6.824/papers/mapreduce.pdf)

# Consistency of Understanding: should all map task be done before starting first reduce task?
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
