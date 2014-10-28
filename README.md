rabbitmq-demo
=============

V02. 多个接收者（Worker）
---------------
1、测试：先运行3个Worker，然后运行NewTask：	
>	可以看到3个Worker协同工作，分别接收了很多消息  

V01. Simple Demo
---------------
1、测试，先运行Receive,再运行Send两次，看到如下消费者(Receive的Console面板）能够接受到2条消息了，表明RabbitMQ的环境OK了。	
```	
[*] Waiting for messages. To exit press CTRL+C
[x] Received 'Hello World!'
[x] Received 'Hello World!'
```

2、cmd进到sbin目录，键入rabbitmq-plugins enable rabbitmq_management启用监控管理，然后重启RabbitMQ服务器。 
*	打开网址http://localhost:15672，用户名和密码都是guest。
*	这次我们关掉Receive，再次运行Send，然后点击管理网页上的Queue，可以看到有两个Message（hello）；
*	点击hello进去，并点击Get Message,可以看到Hello World消息的确已经传送到RabbitMQ的服务器端了。 