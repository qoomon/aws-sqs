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

    static final Logger                LOG                          = LoggerFactory.getLogger(SQSConsumer.class);

    protected static final int         MAX_MESSAGE_RECEIVE_COUNT    = 5;
    protected static final int         MESSAGE_HANDLE_RETRY_SECONDS = 5;

    private final SQSQueue<T>          queue;
    private final ExecutorService      executorService;
    private int                        minWorkerCount               = 1;
    private int                        maxWorkerCount               = 1;
    private final SQSMessageHandler<T> handler;

    private final Runnable             worker;
    private int                        workerCount;

    private boolean                    running                      = false;




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
                boolean loop = true;
                LOG.debug("worker run");
                while (SQSConsumer.this.running && loop)
                {
                    try
                    {
                        final ObjectMessage<T> receiveMessage = SQSConsumer.this.queue.receiveMessage();
                        if (receiveMessage != null)
                        {
                            SQSConsumer.this.increaseWorkerCount();
                            try
                            {
                                SQSConsumer.this.handler.receivedMessage(queue, receiveMessage);
                            }
                            catch (final Throwable e)
                            {
                                final int approximateReceiveCount = receiveMessage.getApproximateReceiveCount();
                                LOG.error("uncought exception while message handling. approximate receive count: " + approximateReceiveCount, e);
                                if (approximateReceiveCount < MAX_MESSAGE_RECEIVE_COUNT)
                                {
                                    queue.changeMessageVisibility(receiveMessage.getReceiptHandle(), MESSAGE_HANDLE_RETRY_SECONDS);
                                }
                                else
                                {
                                    LOG.warn("remove message after " + MESSAGE_HANDLE_RETRY_SECONDS + " tries", e);
                                    queue.deleteMessage(receiveMessage.getReceiptHandle());
                                }
                            }
                        }
                        else
                        {
                            loop = !SQSConsumer.this.decreaseWorkerCount();
                        }
                    }
                    catch (final Throwable e)
                    {
                        LOG.error("uncought exception", e);
                    }
                }
                LOG.debug("worker died");
            }
        };
    }




    public void start()
    {
        this.running = true;
        while (this.increaseWorkerCount())
        {
        }
    }




    private synchronized boolean increaseWorkerCount()
    {
        if (this.workerCount < this.maxWorkerCount)
        {
            this.executorService.submit(this.worker);
            this.workerCount++;
            LOG.debug("increase worker count. actual worker count: " + this.workerCount);
            return true;
        }

        return false;
    }




    private synchronized boolean decreaseWorkerCount()
    {
        if (this.workerCount > this.minWorkerCount)
        {
            this.workerCount--;
            LOG.debug("decrease worker count. actual worker count: " + this.workerCount);
            return true;
        }

        return false;
    }




    public void stop() throws InterruptedException
    {
        this.running = false;
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
