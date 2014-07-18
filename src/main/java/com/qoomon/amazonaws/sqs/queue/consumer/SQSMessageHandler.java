package com.qoomon.amazonaws.sqs.queue.consumer;

import com.qoomon.amazonaws.sqs.queue.ObjectMessage;
import com.qoomon.amazonaws.sqs.queue.SQSQueue;

public interface SQSMessageHandler<T>
{

    public void receivedMessage(SQSQueue<T> queue, ObjectMessage<T> message);
}
