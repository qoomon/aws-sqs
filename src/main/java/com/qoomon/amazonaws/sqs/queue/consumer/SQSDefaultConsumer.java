package com.qoomon.amazonaws.sqs.queue.consumer;

import java.util.concurrent.ExecutorService;

import com.qoomon.amazonaws.sqs.queue.SQSQueue;

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
