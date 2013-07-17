package com.mytaxi.amazonaws.sqs.queue.consumer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

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

    static final Logger                    LOG                                      = LoggerFactory.getLogger(SQSConsumer.class);
    static final String                    SQS_MESSAGE_MDC_KEY                      = "SQSMessage";

    protected static final int             MESSAGE_HANDLE_RETRY_SECONDS             = 20;

    private final SQSQueue<T>              queue;
    private final ExecutorService          executorService;
    private final ScheduledExecutorService scheduledExecutorService;
    private int                            minWorkerCount                           = 1;
    private int                            maxWorkerCount                           = 1;

    private int                            longRunningChangeVisibilitySeconds       = 10;
    private int                            longRunningChangeVisibilityOffsetSeconds = 3;

    private final SQSMessageHandler<T>     handler;

    private final Runnable                 worker;
    private final AtomicInteger            waitingWorkerCount                       = new AtomicInteger(0);
    private int                            workerCount                              = 0;

    private boolean                        running                                  = false;




    public SQSConsumer(final SQSQueue<T> queue, final ExecutorService executorService, final SQSMessageHandler<T> handler)
    {
        this(queue, executorService, handler, Executors.newScheduledThreadPool(1));
    }




    public SQSConsumer(final SQSQueue<T> queue, final ExecutorService executorService, final SQSMessageHandler<T> handler, final ScheduledExecutorService scheduledExecutorService)
    {
        super();
        Preconditions.checkNotNull(queue);
        this.queue = queue;
        Preconditions.checkArgument(executorService != scheduledExecutorService, "executorService should not be equals to scheduledExecutorService");
        Preconditions.checkNotNull(executorService);
        this.executorService = executorService;
        Preconditions.checkNotNull(scheduledExecutorService);
        this.scheduledExecutorService = scheduledExecutorService;
        Preconditions.checkNotNull(handler);
        this.handler = handler;

        this.worker = this.newWorker(queue);
    }




    private Runnable newWorker(final SQSQueue<T> queue)
    {
        return new Runnable()
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
                        SQSConsumer.this.waitingWorkerCount.incrementAndGet();
                        final ObjectMessage<T> receiveMessage = SQSConsumer.this.queue.receiveMessage();
                        SQSConsumer.this.waitingWorkerCount.decrementAndGet();
                        if (receiveMessage != null)
                        {
                            if (SQSConsumer.this.waitingWorkerCount.get() == 0)
                            {
                                SQSConsumer.this.increaseWorkerCount();
                            }

                            MDC.put(SQS_MESSAGE_MDC_KEY, receiveMessage.getId());
                            try
                            {
                                LOG.trace("receive message");
                                final Runnable visibilityChangeTask = this.newVisibilityChangeTask(queue, receiveMessage);

                                // start watcher for set new visibility timeout
                                final int delaySeconds = queue.getVisibilityTimeoutSeconds() - SQSConsumer.this.longRunningChangeVisibilityOffsetSeconds;
                                final int periodSeconds = SQSConsumer.this.longRunningChangeVisibilitySeconds - SQSConsumer.this.longRunningChangeVisibilityOffsetSeconds;
                                final ScheduledFuture<?> futureTask = SQSConsumer.this.scheduledExecutorService.scheduleAtFixedRate(visibilityChangeTask,
                                        delaySeconds, periodSeconds, TimeUnit.SECONDS);

                                SQSConsumer.this.handler.receivedMessage(queue, receiveMessage);

                                // stop watcher for set new visibility timeout
                                futureTask.cancel(true);
                            }
                            catch (final Throwable e)
                            {
                                final int approximateReceiveCount = receiveMessage.getApproximateReceiveCount();
                                LOG.error("uncought exception while message handling. approximate receive count: " + approximateReceiveCount, e);
                                queue.changeMessageVisibility(receiveMessage.getReceiptHandle(), MESSAGE_HANDLE_RETRY_SECONDS);
                            }
                            finally
                            {
                                MDC.remove(SQS_MESSAGE_MDC_KEY);
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




            private Runnable newVisibilityChangeTask(final SQSQueue<T> queue, final ObjectMessage<T> receiveMessage)
            {
                return new Runnable()
                {

                    @Override
                    public void run()
                    {
                        MDC.put(SQS_MESSAGE_MDC_KEY, receiveMessage.getId());
                        try
                        {
                            LOG.error("change message visibility to " + SQSConsumer.this.longRunningChangeVisibilitySeconds + " seconds.");
                            queue.changeMessageVisibility(receiveMessage.getReceiptHandle(), SQSConsumer.this.longRunningChangeVisibilitySeconds);
                        }
                        finally
                        {
                            MDC.remove(SQS_MESSAGE_MDC_KEY);
                        }
                    }
                };
            }
        };
    }




    public void start()
    {
        this.running = true;
        while (this.workerCount < this.minWorkerCount)
        {
            final boolean increaseWorkerCount = this.increaseWorkerCount();
            if (!increaseWorkerCount)
            {
                throw new IllegalStateException("could not increase worker count");
            }
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




    public int getLongRunningChangeVisibilitySeconds()
    {
        return this.longRunningChangeVisibilitySeconds;
    }




    public void setLongRunningChangeVisibilitySeconds(final int longRunningChangeVisibilitySeconds)
    {
        Preconditions.checkArgument(longRunningChangeVisibilitySeconds > 0, "longRunningChangeVisibilitySeconds should be greater than 0");
        Preconditions.checkArgument(longRunningChangeVisibilitySeconds > this.longRunningChangeVisibilityOffsetSeconds, "longRunningChangeVisibilitySeconds should be greater than longRunningChangeVisibilityOffsetSeconds");

        this.longRunningChangeVisibilitySeconds = longRunningChangeVisibilitySeconds;
    }




    public SQSConsumer<T> withLongRunningChangeVisibilitySeconds(final int longRunningChangeVisibilitySeconds)
    {
        this.setLongRunningChangeVisibilitySeconds(longRunningChangeVisibilitySeconds);
        return this;
    }




    public int getLongRunningChangeVisibilityOffsetSeconds()
    {
        return this.longRunningChangeVisibilityOffsetSeconds;
    }




    public void setLongRunningChangeVisibilityOffsetSeconds(final int longRunningChangeVisibilityOffsetSeconds)
    {
        Preconditions.checkArgument(longRunningChangeVisibilityOffsetSeconds > 0, "longRunningChangeVisibilityOffsetSeconds should be greater than 0");
        Preconditions.checkArgument(longRunningChangeVisibilityOffsetSeconds < this.longRunningChangeVisibilitySeconds, "longRunningChangeVisibilityOffsetSeconds should be less than longRunningChangeVisibilitySeconds");
        this.longRunningChangeVisibilityOffsetSeconds = longRunningChangeVisibilityOffsetSeconds;
    }




    public SQSConsumer<T> withLongRunningChangeVisibilityOffsetSeconds(final int longRunningChangeVisibilityOffsetSeconds)
    {
        this.setLongRunningChangeVisibilityOffsetSeconds(longRunningChangeVisibilityOffsetSeconds);
        return this;
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
