package com.razor.springRabbit.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

@Component
public class MessageListener implements Runnable, QueueCore.QueueListener {

    private final static Logger LOG = LoggerFactory.getLogger(MessageListener.class);

    private List<DatedMessageItem> messageList;
    private QueueCore<String> queueCore;

    private String host = "";
    private String queueName = "";
    private boolean keepListening = false;

    /**
     * Constructor
     */

    public MessageListener() {
        this.queueCore = new QueueCore();
        this.messageList = new LinkedList<DatedMessageItem>();
    }

    /**
     * Accessors
     */

    protected List<DatedMessageItem> getMessageList() {
        return this.messageList;
    }

    protected Logger getLogger() {
        return LOG;
    }

    /**
     * Start listening
     */

    public void startListening(String host, String queueName) {
        if (!this.keepListening) {
            if (!this.host.equals(host) || !this.queueName.equals(queueName)) {
                this.host = host;
                this.queueName = queueName;
                this.getMessageList().clear();
            }
            this.keepListening = true;

            this.getLogger().debug("starting listening thread");
            new Thread(this).start();
        }
    }

    /**
     * Stop listening
     */

    public void stopListening() {
        this.getLogger().debug("stopping listening thread");
        this.keepListening = false;
    }

    /**
     * Get the list of message items that were added since the last check time
     * Return those.
     * Remove them from the internal list.
     *
     * @param lastCheckTime
     * @return
     */

    public List<DatedMessageItem> getMessageSince(Date lastCheckTime) {
        List<DatedMessageItem> messageContent = new ArrayList<DatedMessageItem>();
        synchronized (this.getMessageList()) {
            int lastUnreadMessageLocation = this.getLastUnreadMessage(lastCheckTime);
            if (lastUnreadMessageLocation != -1) {
                while (lastUnreadMessageLocation < this.getMessageList().size()) {
                    this.getLogger().debug("adding message at location {}", lastUnreadMessageLocation);
                    messageContent.add(this.getMessageList().get(lastUnreadMessageLocation++));
                }
            }
        }

        this.getLogger().info("returning {} messages from list", messageContent.size());
        return messageContent;
    }

    /**
     * Get the unread messages from the queue
     * Binary search coming up (but only if queue is big enough to warrant it) !!
     */

    public int getLastUnreadMessage(Date lastCheckTime) {

        this.getLogger().info("queue {} has {} messages", this.queueName, this.messageList.size());

        if (getMessageList().size() < 1) {
            return -1;
        } else if (getMessageList().size() < 2) {
            return 0;
        } else if (getMessageList().size() < 100) {
            return this.getLastReadMessageLocation(lastCheckTime);
        }

        int lowerPointer = 0;
        int upperPointer = this.getMessageList().size()-1;
        return this.recurseMessageListForLastReadMessage(lastCheckTime, lowerPointer, upperPointer);
    }

    protected int getLastReadMessageLocation(Date lastCheckTime) {
        for(int index = 0; index < getMessageList().size()-1; index++ ) {
            if (this.getMessageList().get(index).getAddedDate().compareTo(lastCheckTime) > 0) {
                this.getLogger().info("returning last message index of {}", index);
                return index;
            }
        }
        return -1;
    }

    protected int recurseMessageListForLastReadMessage(Date lastCheckTime, int lower, int upper) {
        int current = lower + ((upper - lower) / 2);
        Date currentDate = this.getMessageList().get(current).getAddedDate();
        Date lowerDate = this.getMessageList().get(current - 1).getAddedDate();
        Date upperDate = this.getMessageList().get(current+1).getAddedDate();

        // lower and currentDate < check date, go forward
        // lower and currentDate > check date, go back further
        // lower < check date and currentDate > check date, bingo!
        // current < check date and upperDate > check date, bingo!

        if (currentDate.compareTo(lastCheckTime) == 0) {
            this.getLogger().debug("found exact match");
            return current;
        } else if (current > this.getMessageList().size()-1) {
            this.getLogger().debug("reached upper end of list");
            return -1;
        } else if (current < 0) {
            this.getLogger().debug("went below lower limit of list");
            return -1;
        } else if (currentDate.compareTo(lastCheckTime) < 1 &&
                upperDate.compareTo(lastCheckTime) > 0) {
            this.getLogger().debug("returning last message index of {} + 1", current);
            return current+1;
        } else if (lowerDate.compareTo(lastCheckTime) < 1 &&
                currentDate.compareTo(lastCheckTime) < 1) {
            this.recurseMessageListForLastReadMessage(lastCheckTime, current, upper);
        } else if (lowerDate.compareTo(lastCheckTime) > 0 &&
                currentDate.compareTo(lastCheckTime) > 0) {
            this.recurseMessageListForLastReadMessage(lastCheckTime, lower, current);
        } else {
            this.getLogger().debug("returning last message index of {}", current);
            return current;
        }

        this.getLogger().debug("dropped out of recursive search with none found");
        return -1;
    }

    /*
     * Entry point for the thread
     */

    @Override
    public void run() {
        this.handleThread();
    }

    /**
     * Handles the thread. No real reason I have this in a separate method.
     */

    protected void handleThread() {
        try {
            this.queueCore.listenForQueueMessages(host, queueName, this);
        } catch(IOException ioException) {
            this.keepListening = false;
        } catch(InterruptedException ex) {
            this.keepListening = false;
        }
    }

    /**
     * Tell the listener if it should stop
     * @return
     */

    @Override
    public boolean keepListening() {
        return this.keepListening;
    }

    /**
     * Let the listener add a message
     * @param datedMessageItem
     */

    @Override
    public void addMessageItem(DatedMessageItem datedMessageItem) {
        synchronized (this.getMessageList()) {
            this.getLogger().debug("adding a message {}", datedMessageItem.getMessage());
            this.getMessageList().add(datedMessageItem);
        }
    }
}
