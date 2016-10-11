# Projects
FastMQ based on zeromq(Jeromq) is aimed to build simple and fast MQServicers that Cloud Platform or Web Service can use.

## Data Persistence
use rocksdb(facebook rocksdb <https://github.com/facebook/rocksdb>) to save the MQ message and keep the message sequenece.

## Cluster Information registration and discovery
use zookeeper(apache zookeeper<https://github.com/apache/zookeeper>) and zkClient(zkclient - A simple and effective Java
client for zookeeper (both support zookeeper 3.3.x/3.4.x) <https://github.com/adyliu/zkclient>)
to save information of MQ clusters and do some operation with the information.

## Service Export and Refer
use dubbo (alibaba dubbo <https://github.com/alibaba/dubbo>) to do the job of RPC.it can export and refer services.
### planning another RPC framework by designing autonomously.
plan to discovery
