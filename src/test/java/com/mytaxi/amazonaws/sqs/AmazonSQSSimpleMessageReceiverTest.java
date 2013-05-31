package com.mytaxi.amazonaws.sqs;

import static org.junit.Assert.assertTrue;

import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
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
        final AmazonSQS amazonSqs = new AmazonSQSClient(awsCredentials);
        final AmazonSQSQueue amazonSQSQueue = new AmazonSQSQueue(amazonSqs, "AmazonSQSTestQueue", 20, 5);
        amazonSQSQueue.init();
        final int messagesToSend = 100;
        this.fillQueueWithTestData(amazonSQSQueue, messagesToSend);

        final CountDownLatch countDownLatch = new CountDownLatch(messagesToSend);
        final MessageHandler messageHandler = new MessageHandler()
        {

            @Override
            public void receivedMessage(final MessageTask messageTask)
            {
                countDownLatch.countDown();
                messageTask.delete();
            }
        };
        final AmazonSQSSimpleMessageReceiver amazonSQSSimpleMessageReceiver = new AmazonSQSSimpleMessageReceiver(amazonSQSQueue, messageHandler, 32);

        // WHEN

        amazonSQSSimpleMessageReceiver.start();
        final Stopwatch stopwatch = new Stopwatch().start();

        // THEN
        final int maxRuntime = 2000;
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
            final LinkedList<String> messageBodies = new LinkedList<String>();
            for (int j = 0; j < 10; j++)
            {
                messageCount++;
                messageBodies.add("messageID: " + j + " systime: " + System.currentTimeMillis());
            }
            amazonSqs.sendMessage(messageBodies, 0);
            System.out.println("send: messageID: " + messageCount);
        }
        System.out.println("all messages send!");
    }

}
