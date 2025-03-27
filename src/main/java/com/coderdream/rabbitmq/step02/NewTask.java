package com.coderdream.rabbitmq.step02;

import com.coderdream.rabbitmq.util.CdConstants;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class NewTask {
	private static final String TASK_QUEUE_NAME = "task_queue";

	public static void main(String[] argv) throws Exception {
		for (int i = 1; i <= 50; i++) {
			sendMessage(i);
		}

	}

	private static void sendMessage(int index) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(CdConstants.RABBITMQ_SERVER_IP);
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();
		// 声明此队列并且持久化
		channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);

		String message = getMessage(index);

		channel.basicPublish("", TASK_QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());// 持久化消息
		System.out.println(" [x] Sent '" + message + "'");

		channel.close();
		connection.close();
	}

	private static String getMessage(int index) {
		Date date = Calendar.getInstance().getTime();
		SimpleDateFormat f_timestamp = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
		String logTimestampStr = f_timestamp.format(date);
		return "Hello World " + index + " at " + logTimestampStr;
	}

}
