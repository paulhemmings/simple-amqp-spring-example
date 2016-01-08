package com.razor.springRabbit.core;

public interface QueueMessageAdapter<T> {
    byte[] toQueueItem(T message);
    T toMessage(byte[] queueItem);
}
