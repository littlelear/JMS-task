Developer README
================


How to use it
================
1. Create a task table in producer side DB
	- DROP TABLE IF EXISTS `dev_db`.`jms_b_task`;
    - CREATE TABLE  `dev_db`.`jms_b_task` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `content_id` varchar(45) DEFAULT NULL,
  `type` int(10) unsigned NOT NULL,
  `status` int(10) unsigned NOT NULL,
  `created_time` datetime NOT NULL,
  `operated_time` datetime DEFAULT NULL,
  `message` longtext,
  `retry` int(10) unsigned DEFAULT NULL,
  `last_error` varchar(200) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;
	
2. Use **JMSTaskDAO** to add task
3. Create specific scheduler (implementing **AbstractJMSScheduler**) to execute task (sending message)
4. If using *NO_ACKNOWLEDGE* mode, receive message in consumer side. 
5. If using *CLIENT_ACKNOWLEDGE* mode, receive message in consumer side by implementing **AbstractTaskMessageListener**
6. If using *CLIENT_ACKNOWLEDGE* mode, **ReplyReceiver** will get the acknoledge message

Spring configuration
================
1. Producer
```xml
<!-- JMS Task configuration -->
	<!-- JMS DAO -->
	<bean id="jmsTaskDao" class="com.bleum.canton.jms.dao.impl.JMSTaskDao">
		<property name="dataSource" ref="estoreDataSource" />
	</bean>

	<!-- Acknowledge reply queue -->
	<bean id="replyQueueEstore" class="org.apache.activemq.command.ActiveMQQueue">
		<constructor-arg index="0" value="estore.reply.queue"></constructor-arg>
	</bean>

	<!-- Task Scheduler -->
	<bean id="inventoryScheduler" class="com.bleum.canton.oms.jms.scheduler.InventoryScheduler">
		<property name="initialDelay" value="10000" />
		<property name="fixedDelay" value="30000" />
		<property name="threads" value="1" />
		<property name="maxTasksPerThread" value="10" />
		<property name="jmsSender" ref="inventoryUpdateSender" />
	</bean>

	<!-- Task Scheduler using client acknowledge mode -->
	<bean id="orderCreateScheduler" class="com.bleum.canton.oms.jms.scheduler.OrderCreateScheduler">
		<property name="initialDelay" value="10000" />
		<property name="fixedDelay" value="30000" />
		<property name="threads" value="1" />
		<property name="maxTasksPerThread" value="10" />
		<property name="jmsSender" ref="orderCreateSender" />
		<property name="clientAck" value="2" />
		<property name="replyQueue" ref="replyQueueEstore" />
	</bean>

	<!-- Acknowledge reply receiver, needed if using client acknowledge mode -->
	<bean id="replyReceiver"
		class="org.springframework.jms.listener.DefaultMessageListenerContainer">
		<property name="connectionFactory" ref="connectionFactory" />
		<property name="destination" ref="replyQueueEstore" />
		<property name="messageListener" ref="replyMessageListener" />
		<property name="concurrentConsumers" value="1" />
		<property name="sessionTransacted" value="true" />
		<property name="receiveTimeout" value="10000" />
	</bean>
```
2. Consumer
```xml
<!-- JMS Task configuration -->
	<!-- JMS DAO, this is only needed if using client acknowledge mode  -->
	<bean id="jmsTaskDao" class="com.bleum.canton.jms.dao.impl.JMSTaskDao">
		<property name="dataSource" ref="estoreDataSource" />
	</bean>
```




