# TDengine-Test-3.0.7.0

本版本的主要内容为紧急的 bug fix。
1. 问题：在三节点三副本的集群中，如果其中一个 dnode 的磁盘损坏，集群写入失败
  1. 
    TS-3598

  测试结果：当出现上述异常时，集群能够继续正常工作，能够写入和查询。测试结果符合预期，详情见 JIRA TS-3598
1. 问题：不兼容的客户端导致服务端crash。在升级服务端 taosd 时使用低版本客户端的应用程序并未停止，会导致升级后的服务端 taosd 在重启后 crash
  1. 
    TS-3617

  1. 
    TS-3603

  1. 
    TS-3635

  测试结果：当出现上述情况时，客户端请求会收到错误 "Permission denied"；如果重启低版本客户端，会收到错误 "incompatible client version, connection refused"
