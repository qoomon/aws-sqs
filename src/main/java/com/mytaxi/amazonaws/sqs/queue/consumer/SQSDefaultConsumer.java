package com.mytaxi.amazonaws.sqs.queue.consumer;

import java.util.concurrent.ExecutorService;

import com.mytaxi.amazonaws.sqs.queue.SQSQueue;

/**
 * UtilClass to continuously poll from amazon message queue
 *
 * @author bengtbrodersen
 *
 */
public class SQSDefaultConsumer extends SQSConsumer<String>
{

    public SQSDefaultConsumer(final SQSQueue<String> queue, final ExecutorService executorService, final SQSMessageHandler<String> handler)
    {
        super(queue, executorService, handler);
    }


}
