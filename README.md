ZService
=======

ZService 是一个基于ZeroMQ的分布式的服务框架，它易扩展，高可用。

它提供透明化的RPC远程调用，可支持多协议（暂未实现），并提供软负载均衡和并提供软负载均衡和容错机制的集群支持。
还提供简单的服务、集群管理功能



简单的测试说明
=======
依次运行 manager.py、broker1.py、broker2.py、worker1.py、worker2.py
因为现在只是一些核心功能的实现，有些地方还不完善，不按顺序执行可能会出现一些问题

client1.py 是会向 manager.py 执行注册并取得取得broker uri等配置流程
client2.py 则是为了方便测试，写死只会连接到broker2.py

运行client2.py 会看到调用RPC服务后返回的信息。如 9: hello world from worker 1
... from worker 1 表示调用的worker1.py,  ... from worker 2 表示调用的worker2.py

ctrl+c 停止worker1.py，以模拟broker负载能力不足。
等一会儿，以便broker感知到worker1超时不能用了。再次运行 client2.py, 则会看到 9: hello world from worker 2，表示由于 broker2 负载能力不足，请求被转发到broker1上了

ctrl+c 停止worker2.py连接到的borker，等一会儿会看到 worker2 连接到另一个broker上了
