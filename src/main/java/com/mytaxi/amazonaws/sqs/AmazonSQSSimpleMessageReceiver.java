package com.mytaxi.amazonaws.sqs;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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

  static final Logger LOG = LoggerFactory.getLogger(AmazonSQSSimpleMessageReceiver.class);

  private final AmazonSQSQueue queue;
  private ExecutorService workerPool = null;
  private final MessageHandler messageHandler;


  private final int workerCount;


  public AmazonSQSSimpleMessageReceiver(
    final AmazonSQSQueue queue, final MessageHandler messageHandler, final int workerCount)
  {
    super();
    this.queue = queue;
    this.messageHandler = messageHandler;
    this.workerCount = workerCount;
    this.workerPool = Executors.newFixedThreadPool(this.workerCount);
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
          while (!AmazonSQSSimpleMessageReceiver.this.workerPool.isShutdown())
          {
            try
            {
              final Message message = AmazonSQSSimpleMessageReceiver.this.queue.receiveMessage();
              if (message != null)
              {
                try
                {
                  AmazonSQSSimpleMessageReceiver.this.messageHandler.receivedMessage(new MessageTask(
                    message, AmazonSQSSimpleMessageReceiver.this.queue));
                }
                catch (final Throwable e)
                {
                  LOG.error("uncought exception", e);
                }
              } else
              {
                LOG.debug("no message received");
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
