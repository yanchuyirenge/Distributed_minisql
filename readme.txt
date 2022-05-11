1.zookeeper主机ip、port位于master中（调试值为本机127.0.0.1:2181）
2.client节点在client_path下注册，命名原则client+编号（即按注册顺序cilent0，client1，直到client+MAX_client）
3.读请求在read_path下注册（调试值zookeeper Read）下注册，命名原则read+编号（即按注册顺序read0，read1，直到read+MAX_client）
4.写请求在write_path下注册（调试值zookeeper Write）下注册，命名原则write+编号（即按注册顺序write0，write1，直到write+MAX_write）
5.调试用了自己写的服务端，尚未与region对接，心跳监听也未测试