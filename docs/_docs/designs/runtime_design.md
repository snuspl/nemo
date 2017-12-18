---
title: Runtime Design
permalink: /docs/runtime_design/
---

### Overview

After the compiler goes through a set of passes for optimization, the optimized Onyx IR is translated into into a 
physical form for the execution runtime to execute. This involves translations like expanding an operator annotated 
with parallelism in Onyx IR to the desired number of tasks and connecting the tasks according to the data communication 
patterns annotated on the IR edges. Physical execution plan is also in the form of a DAG, with the same values annotated 
for execution properties as the given IR DAG if necessary. Onyx IR DAG and physical execution plan can be translated 
from one another by sharing the identifiers.

The Onyx runtime consists of a _RuntimeMaster_ and multiple _Executors_.
_RuntimeMaster_ takes the submitted physical execution plan and schedules each _TaskGroup_ to _Executor_ for execution.

### Dictionary
* Stage: A unit of execution the runtime uses for scheduling the job.
* TaskGroup: A computation unit composed of one or more tasks that can be computed in a single executor.
* Block: The unit of data output by a single task.
* Partition: A block consists of one or more partitions, depending on the _Partitioner_ choice.

### Runtime Architecture
The following figure shows the Onyx runtime's overall architecture.
![picture]({{ site.url }}assets/runtime_arch.png)



