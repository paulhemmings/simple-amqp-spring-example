package com.razor.springRabbit.adapters;

import com.razor.springRabbit.core.QueueMessageAdapter;

public class SimpleStringMessageAdapter implements QueueMessageAdapter<String> {
    @Override
    public byte[] toQueueItem(String message) {
        return message.getBytes();
    }
    @Override
    public String toMessage(byte[] queueItem) {
        return new String((queueItem));
    }
}
