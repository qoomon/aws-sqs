package com.mytaxi.amazonaws.sqs;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
public class AmazonSQSBuferedMessageReceiver
{

    static final Logger          LOG        = LoggerFactory.getLogger(AmazonSQSBuferedMessageReceiver.class);

    private final AmazonSQSQueue queue;
    private ExecutorService      workerPool = null;
    private final MessageHandler messageHandler;

    private final int            batchSize;

    private final int            workerCount;


    public AmazonSQSBuferedMessageReceiver(final AmazonSQSQueue queue, final MessageHandler messageHandler, final int workerCount)
    {
        super();
        this.queue = queue;
        this.messageHandler = messageHandler;
        this.workerCount = workerCount;
        this.workerPool = Executors.newFixedThreadPool(this.workerCount);
        throw new NotImplementedException("not implementet yet");
    }




    public void start()
    {
        for (int i = 0; i < this.workerCount; i++)
        {
            this.workerPool.submit(new Runnable()
            {

                @Override
                public void run()
                {
                    while (!AmazonSQSBuferedMessageReceiver.this.workerPool.isShutdown())
                    {
                        try
                        {
                            final Message message = AmazonSQSBuferedMessageReceiver.this.queue.receiveMessage();
                            if (message != null)
                            {
                                try
                                {
                                    AmazonSQSBuferedMessageReceiver.this.messageHandler.receivedMessage(new MessageTask(message, AmazonSQSBuferedMessageReceiver.this.queue));
                                }
                                catch (final Throwable e)
                                {
                                    LOG.error("uncought exception", e);
                                }
                            }
                        }
                        catch (final Throwable e)
                        {
                            LOG.error("uncought exception", e);
                        }
                    }
                }
            });
        }

    }




    public void shutdown() throws InterruptedException
    {
        this.workerPool.shutdown();
        this.workerPool.awaitTermination(30, TimeUnit.SECONDS);
    }

}
