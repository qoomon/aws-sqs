package com.mytaxi.amazonaws.sqs;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.buffered.AmazonSQSBufferedAsyncClient;
import com.google.common.base.Stopwatch;

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
        // Create the buffered client
        final AmazonSQSAsync sqs = new AmazonSQSBufferedAsyncClient(sqsAsync);

        final AmazonSQSQueue amazonSQSQueue = new AmazonSQSQueue(sqs, "AmazonSQSTestQueue", 20, 5);
        amazonSQSQueue.init();
        final int messagesToSend = 1000;
        this.fillQueueWithTestData(amazonSQSQueue, messagesToSend);

        final CountDownLatch countDownLatch = new CountDownLatch(messagesToSend);
        final MessageHandler messageHandler = new MessageHandler()
        {

            @Override
            public void receivedMessage(final MessageTask messageTask)
            {
                countDownLatch.countDown();
                System.out.println("countDownLatch: " + countDownLatch.getCount());
                messageTask.delete();
            }
        };
        final AmazonSQSSimpleMessageReceiver amazonSQSSimpleMessageReceiver = new AmazonSQSSimpleMessageReceiver(amazonSQSQueue, messageHandler, 32);

        // WHEN
        amazonSQSSimpleMessageReceiver.start();
        final Stopwatch stopwatch = new Stopwatch().start();

        // THEN
        final int maxRuntime = 20000;
        countDownLatch.await(maxRuntime, TimeUnit.MILLISECONDS);
        final long runtime = stopwatch.stop().elapsed(TimeUnit.MILLISECONDS);
        amazonSQSSimpleMessageReceiver.shutdown();
        System.out.println("runtime: " + runtime);
        assertTrue("runtime warning:  " + runtime, runtime < maxRuntime);

    }




    private void fillQueueWithTestData(final AmazonSQSQueue amazonSqs, final int messagesToSend)
    {
        System.out.println("start sending messages...");
        int messageCount = 0;
        while (messageCount < messagesToSend)
        {
            messageCount++;
            // amazonSqs.sendMessage("messageCount: " + messageCount + " - " + System.currentTimeMillis(), 0);
            System.out.println("send: messageID: " + messageCount);
        }
        System.out.println("all messages send!");
    }

}
