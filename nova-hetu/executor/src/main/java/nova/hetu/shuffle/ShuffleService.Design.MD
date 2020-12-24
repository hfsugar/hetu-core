####Shuffle Service

Shuffle service is responsible shuffling data between tasks. It should be easy for the user of Shuffle service to set up 
communication between tasks based on the DAG of stages.

######Requirements
1. A single point responsible for communication between tasks
2. Capable of `1 to 1`, `1 to M`, `M to 1` and `M to M` communication paradigms
3. Transparent back pressure
4. Non-blocking

Shuffle Service is not visible to the SQL kernel, the exposed API is the `PageProducer` and `PageConsumer`.

######API 
`
//creates a PageProducer for a partition and a task, when there is no partition
PageProducer.create(TaskId, PartitionId, Serde)
PageProducer.send(Page)
PageProducer.create(TaskId, PartitionId, Serde)
` 
`
PageConsuder.create(TaskId, PartitionId, Serde)
` 


####Task Input/Output

#####OutputOperator
The operator sends pages to a target channel to be received by down stream operator, potentially running in a different location.

#####ExchangeOperator
The operators receiving pages sent by up stream operator

`LocalExectionPlanner` uses ShuffleService to set up the communication channels by creating an instance of `ShuffleService`. The `ShuffleService` instance is passed to the 
