package com.razor.springRabbit.core;

import com.razor.springRabbit.adapters.SimpleStringMessageAdapter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class QueueCoreTest {

    @Spy
    QueueCore<String> queueCore;

    QueueMessageAdapter<String> queueMessageAdapter = new SimpleStringMessageAdapter();

    @Mock
    private com.rabbitmq.client.ConnectionFactory connectionFactory;

    @Mock
    private com.rabbitmq.client.Connection connection;

    @Mock
    private com.rabbitmq.client.Channel channel;

    @Before
    public void setup() throws Exception {
    }

    @Test
    public void publishToMessageQueue() throws IOException {

        String host = "host";
        String queueName = "queue";
        String message = "message";

        doReturn(connection).when(queueCore).createConnection(host);
        doReturn(channel).when(connection).createChannel();

        queueCore.publishToMessageQueue(host, queueName, message, queueMessageAdapter);

        verify(channel, times(1)).queueDeclare(queueName, false, false, false,null);
        verify(channel, times(1)).basicPublish("", queueName, null, message.getBytes());
        verify(channel, times(1)).close();
        verify(connection, times(1)).close();
    }

    @Test(expected = IOException.class)
    public void publishToMessageQueueHandlesExceptionWhenCreatingChannel() throws IOException {

        String host = "host";
        String queueName = "queue";
        String message = "message";

        doThrow(IOException.class).when(queueCore).createConnection(host);


        queueCore.publishToMessageQueue(host, queueName, message, queueMessageAdapter);

        verify(connection, never()).createChannel();
        verify(channel, never()).queueDeclare(queueName, false, false, false,null);
        verify(channel, never()).basicPublish("", queueName, null, message.getBytes());
        verify(channel, never()).close();
        verify(connection, never()).close();
    }
}
