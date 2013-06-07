package com.mytaxi.amazonaws.sqs.queue.consumer;

import com.mytaxi.amazonaws.sqs.queue.ObjectMessage;
import com.mytaxi.amazonaws.sqs.queue.SQSQueue;

public interface SQSMessageHandler<T>
{

    public void receivedMessage(SQSQueue<T> queue, ObjectMessage<T> message);
}
