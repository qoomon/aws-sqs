package com.mytaxi.amazonaws.sqs;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.sqs.model.Message;

/**
 * UtilClass to continuously poll from amazon message queue
 * 
 * @author bengtbrodersen
 * 
 */
public class AmazonSQSSimpleMessageReceiver
{

    static final Logger          LOG                     = LoggerFactory.getLogger(AmazonSQSSimpleMessageReceiver.class);

    private final AmazonSQSQueue queue;
    private ExecutorService      receiverPool            = null;
    private ExecutorService      workerPool              = null;
    private final MessageHandler messageHandler;

    private final AtomicInteger  runningReceiverCount    = new AtomicInteger(0);
    private final int            maxRunningReceiverCount = 10;
    private final AtomicInteger  runningWorkerCount      = new AtomicInteger(0);
    private final int            mxRunningWorkerCount    = 10;




    public AmazonSQSSimpleMessageReceiver(final AmazonSQSQueue queue, final MessageHandler messageHandler)
    {
        super();
        this.queue = queue;
        this.messageHandler = messageHandler;
    }




    public void start()
    {
        this.workerPool = Executors.newCachedThreadPool();

        this.receiverPool = Executors.newCachedThreadPool();

        this.incrementReceiverCount();

    }




    private void incrementReceiverCount()
    {
        AmazonSQSSimpleMessageReceiver.this.runningReceiverCount.incrementAndGet();
        this.receiverPool.submit(new Runnable()
        {

            @Override
            public void run()
            {
                while (!AmazonSQSSimpleMessageReceiver.this.receiverPool.isShutdown())
                {
                    try
                    {
                        final List<Message> messages = AmazonSQSSimpleMessageReceiver.this.queue.receiveMessageBatch(1);

                        for (final Message message : messages)
                        {
                            AmazonSQSSimpleMessageReceiver.this.runningWorkerCount.incrementAndGet();
                            AmazonSQSSimpleMessageReceiver.this.workerPool.submit(new Runnable()
                            {

                                @Override
                                public void run()
                                {
                                    try
                                    {
                                        AmazonSQSSimpleMessageReceiver.this.messageHandler.receivedMessage(new MessageTask(message, AmazonSQSSimpleMessageReceiver.this.queue));
                                    }
                                    catch (final Throwable e)
                                    {
                                        LOG.error("uncought exception", e);
                                    }
                                    AmazonSQSSimpleMessageReceiver.this.runningWorkerCount.decrementAndGet();
                                }
                            });
                        }
                    }
                    catch (final Throwable e)
                    {
                        LOG.error("uncought exception", e);
                    }

                }

                AmazonSQSSimpleMessageReceiver.this.runningReceiverCount.decrementAndGet();
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
