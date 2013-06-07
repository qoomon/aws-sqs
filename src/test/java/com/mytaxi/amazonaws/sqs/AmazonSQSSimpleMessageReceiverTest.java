package com.mytaxi.amazonaws.sqs;

import static org.junit.Assert.assertTrue;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.buffered.AmazonSQSBufferedAsyncClient;
import com.google.common.base.Stopwatch;
import com.mytaxi.amazonaws.sqs.WorkerExecutor.Handler;

public class AmazonSQSSimpleMessageReceiverTest
{

    @Test
    public void test() throws InterruptedException
    {
        // GIVEN
        final AWSCredentials awsCredentials = new AWSCredentials()
        {

            @Override
            public String getAWSSecretKey()
            {
                return "K0xGnasd8xu+RorkyjOUqy/1+144rghYZ0aOeF7e";
            }



            @Override
            public String getAWSAccessKeyId()
            {
                return "AKIAI35ZH66EYCVZ4WXA";
            }
        };
        final AmazonSQSAsync sqsAsync = new AmazonSQSAsyncClient(awsCredentials);
        final AmazonSQSAsync sqs = new AmazonSQSBufferedAsyncClient(sqsAsync);

        final String queueName = "Test-" + UUID.randomUUID();
        final String queueUrl = SQSUtil.create(sqs, queueName);
        final SQSAsyncQueue<String> sqsQueue = new DefaultSQSAsyncQueue(sqs, queueUrl)
                .withVisibilityTimeoutSeconds(10)
                .withWaitTimeSeconds(5);

        final int messagesToSend = 100;
        this.fillQueueWithTestData(sqsQueue, messagesToSend);

        final CountDownLatch countDownLatch = new CountDownLatch(messagesToSend);

        final WorkerExecutor workerExecutor = new WorkerExecutor(Executors.newCachedThreadPool(), 32, new Handler()
        {

            @Override
            public void run()
            {
                final ObjectMessage<String> message = sqsQueue.receiveMessageAsync();
                if (message != null)
                {
                    final String body = message.getBody();
                    countDownLatch.countDown();
                    System.out.println("message received: " + body + " - countDownLatch: " + countDownLatch.getCount());
                    sqsQueue.deleteMessageAsync(message.getReceiptHandle());
                }
                else
                {
                    System.out.println("no message received");
                }

            }
        });

        // WHEN
        workerExecutor.start();
        final Stopwatch stopwatch = new Stopwatch().start();

        // THEN
        final int maxRuntime = 2000;
        countDownLatch.await(maxRuntime, TimeUnit.MILLISECONDS);
        final long runtime = stopwatch.stop().elapsed(TimeUnit.MILLISECONDS);
        System.out.println("runtime: " + runtime + " waiting for shutdown");
        workerExecutor.releaseExternalResources();
        SQSUtil.deleteByUrl(sqs, queueUrl);
        assertTrue("runtime warning:  " + runtime, runtime < maxRuntime);

    }




    private void fillQueueWithTestData(final SQSAsyncQueue<String> sqsQueue, final int messagesToSend)
    {
        System.out.println("start sending messages...");
        int messageCount = 0;
        while (messageCount < messagesToSend)
        {
            messageCount++;
            sqsQueue.sendMessageAsync("messageCount: " + messageCount + " - " + System.currentTimeMillis(), 0);
            System.out.println("send: messageID: " + messageCount);
        }
        System.out.println("all messages send!");
    }

}