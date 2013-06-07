package com.mytaxi.amazonaws.sqs.queue.consumer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.mytaxi.amazonaws.sqs.queue.ObjectMessage;
import com.mytaxi.amazonaws.sqs.queue.SQSQueue;

/**
 * UtilClass to continuously poll from amazon message queue
 *
 * @author bengtbrodersen
 *
 */
public class SQSConsumer<T>
{

    static final Logger           LOG = LoggerFactory.getLogger(SQSConsumer.class);

    private final SQSQueue<T>     queue;
    private final ExecutorService executorService;
    private int                   minWorkerCount = 1;
    private int                   maxWorkerCount = 1;
    private final SQSMessageHandler<T> handler;

    private final Runnable         worker;




    public SQSConsumer(final SQSQueue<T> queue, final ExecutorService executorService, final SQSMessageHandler<T> handler)
    {
        super();
        this.queue = queue;
        this.executorService = executorService;
        this.handler = handler;

        this.worker = new Runnable()
        {

            @Override
            public void run()
            {
                while (!SQSConsumer.this.executorService.isShutdown())
                {
                    try
                    {
                        final ObjectMessage<T> receiveMessage = SQSConsumer.this.queue.receiveMessage();
                        if (receiveMessage != null)
                        {
                            SQSConsumer.this.handler.receivedMessage(queue, receiveMessage);
                            // TODO increase worker
                        }
                        else
                        {
                            // TODO decrease worker
                        }
                    }
                    catch (final Throwable e)
                    {
                        LOG.error("uncought exception", e);
                    }
                }
            }
        };
    }





    public void start()
    {
        // initialize workers
        for (int i = 0; i < this.minWorkerCount; i++)
        {
            this.executorService.submit(this.worker);
        }
    }




    public void releaseExternalResources() throws InterruptedException
    {
        this.executorService.shutdown();
        this.executorService.awaitTermination(30, TimeUnit.SECONDS);
    }




    public void setMaxWorkerCount(final int maxWorkerCount)
    {
        Preconditions.checkArgument(maxWorkerCount > 0, "maxWorkerCount <= 0");
        Preconditions.checkState(maxWorkerCount >= this.minWorkerCount, "maxWorkerCount < minWorkerCount");
        this.maxWorkerCount = maxWorkerCount;
    }




    public SQSConsumer<T> withMaxWorkerCount(final int maxWorkerCount)
    {
        this.setMaxWorkerCount(maxWorkerCount);
        return this;
    }




    public void setMinWorkerCount(final int minWorkerCount)
    {
        Preconditions.checkArgument(minWorkerCount > 0, "minWorkerCount <= 0");
        Preconditions.checkState(minWorkerCount <= this.maxWorkerCount, "minWorkerCount > maxWorkerCount");
        this.minWorkerCount = minWorkerCount;
    }




    public SQSConsumer<T> withMinWorkerCount(final int minWorkerCount)
    {
        this.setMinWorkerCount(minWorkerCount);
        return this;
    }


}
