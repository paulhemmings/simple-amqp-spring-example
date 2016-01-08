package com.razor.springRabbit.core;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.QueueingConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Rabbit MQ implementation
 *
 * References Articles:
 *  http://www.rabbitmq.com/tutorials/tutorial-one-java.html
 *  http://projects.spring.io/spring-amqp/
 *
 */
public class QueueCore<T> {

    private final static Logger LOG = LoggerFactory.getLogger(QueueCore.class);

    /**
     * Interface the listener client has to implement
     */

    public interface QueueListener {
        public boolean keepListening();
        public void addMessageItem(DatedMessageItem datedMessageItem);
    }

    /**
     * Publish a message to a queue
     * @param queueName
     * @param message
     * @throws IOException
     */
    public void publishToMessageQueue(
            String host,
            String queueName,
            T message,
            QueueMessageAdapter<T> messsageAdapter) throws IOException {

        Connection connection = this.createConnection(host);
        Channel channel = connection.createChannel();

        channel.queueDeclare(queueName, false, false, false, null);
        channel.basicPublish("", queueName, null, messsageAdapter.toQueueItem(message));

        getLogger().info(" [x] Sent '" + message + "'");

        channel.close();
        connection.close();
    }

    /**
     * Retrieve a message off the queue
     * @param host
     * @param queueName
     * @param messageAdapter
     * @return
     * @throws IOException
     * @throws InterruptedException
     */

    public T retrieveQueuedMessage(
            String host,
            String queueName,
            QueueMessageAdapter<T> messageAdapter) throws IOException, InterruptedException {

        Connection connection = this.createConnection(host);
        Channel channel = connection.createChannel();

        channel.queueDeclare(queueName, false, false, false, null);

        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(queueName, true, consumer);

        QueueingConsumer.Delivery delivery = consumer.nextDelivery();
        return messageAdapter.toMessage(delivery.getBody());
    }

    /**
     * Get a message from the queue
     * @param host
     * @param queueName
     * @return
     * @throws IOException
     * @throws InterruptedException
     */

    public void listenForQueueMessages(
            String host,
            String queueName,
            QueueListener queueListener) throws IOException, InterruptedException {

        Connection connection = this.createConnection(host);
        Channel channel = connection.createChannel();

        channel.queueDeclare(queueName, false, false, false, null);

        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(queueName, true, consumer);

        while (queueListener.keepListening()) {
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            String message = new String(delivery.getBody());
            queueListener.addMessageItem(new DatedMessageItem(message));
            getLogger().info(" [x] Received '" + message + "'");
        }
    }

    protected Logger getLogger() {
        return LOG;
    }

    /**
     * Create connection to the queue
     * @param host
     * @return
     * @throws IOException
     */

    public Connection createConnection(String host) throws IOException {
        com.rabbitmq.client.ConnectionFactory factory = new com.rabbitmq.client.ConnectionFactory();
        factory.setHost(host);
        return factory.newConnection();
    }
}
