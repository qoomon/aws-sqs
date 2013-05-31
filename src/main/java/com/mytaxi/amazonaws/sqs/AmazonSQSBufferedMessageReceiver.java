package com.mytaxi.amazonaws.sqs;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.sqs.model.Message;

/**
 * UtilClass to continuously poll from amazon message queue
 * 
 * @author bengtbrodersen
 * 
 */
public class AmazonSQSBufferedMessageReceiver
{

    static final Logger          LOG                     = LoggerFactory.getLogger(AmazonSQSBufferedMessageReceiver.class);

    private final AmazonSQSQueue queue;
    private ExecutorService      receiverPool            = null;
    private ExecutorService      workerPool              = null;
    private final MessageHandler messageHandler;

    private final AtomicInteger  runningReceiverCount    = new AtomicInteger(0);
    private final int            maxRunningReceiverCount = 10;
    private final AtomicInteger  runningWorkerCount      = new AtomicInteger(0);
    private final int            mxRunningWorkerCount    = 10;




    public AmazonSQSBufferedMessageReceiver(final AmazonSQSQueue queue, final MessageHandler messageHandler)
    {
        super();
        this.queue = queue;
        this.messageHandler = messageHandler;
        throw new NotImplementedException("not implementet yet");
    }




    public void start()
    {
        this.workerPool = Executors.newCachedThreadPool();

        this.receiverPool = Executors.newCachedThreadPool();

        this.incrementReceiverCount();

    }




    private void incrementReceiverCount()
    {
        AmazonSQSBufferedMessageReceiver.this.runningReceiverCount.incrementAndGet();
        this.receiverPool.submit(new Runnable()
        {

            @Override
            public void run()
            {
                while (!AmazonSQSBufferedMessageReceiver.this.receiverPool.isShutdown())
                {
                    try
                    {
                        final List<Message> messages = AmazonSQSBufferedMessageReceiver.this.queue.receiveMessageBatch(1);

                        for (final Message message : messages)
                        {
                            AmazonSQSBufferedMessageReceiver.this.runningWorkerCount.incrementAndGet();
                            AmazonSQSBufferedMessageReceiver.this.workerPool.submit(new Runnable()
                            {

                                @Override
                                public void run()
                                {
                                    try
                                    {
                                        AmazonSQSBufferedMessageReceiver.this.messageHandler.receivedMessage(new MessageTask(message, AmazonSQSBufferedMessageReceiver.this.queue));
                                    }
                                    catch (final Throwable e)
                                    {
                                        LOG.error("uncought exception", e);
                                    }
                                    AmazonSQSBufferedMessageReceiver.this.runningWorkerCount.decrementAndGet();
                                }
                            });
                        }
                    }
                    catch (final Throwable e)
                    {
                        LOG.error("uncought exception", e);
                    }

                }

                AmazonSQSBufferedMessageReceiver.this.runningReceiverCount.decrementAndGet();
            }
        });
    }




    public void shutdown() throws InterruptedException
    {
        this.receiverPool.shutdown();
        this.receiverPool.awaitTermination(30, TimeUnit.SECONDS);
        this.workerPool.shutdown();
        this.workerPool.awaitTermination(30, TimeUnit.SECONDS);
    }

}
