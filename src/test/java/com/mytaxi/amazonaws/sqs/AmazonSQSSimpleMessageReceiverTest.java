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
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.google.common.base.Stopwatch;
import com.mytaxi.amazonaws.sqs.queue.ObjectMessage;
import com.mytaxi.amazonaws.sqs.queue.SQSDefaultQueue;
import com.mytaxi.amazonaws.sqs.queue.SQSQueue;
import com.mytaxi.amazonaws.sqs.queue.consumer.SQSConsumer;
import com.mytaxi.amazonaws.sqs.queue.consumer.SQSDefaultConsumer;
import com.mytaxi.amazonaws.sqs.queue.consumer.SQSDefaultMessageHandler;
import com.mytaxi.amazonaws.sqs.queue.consumer.SQSMessageHandler;

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
        final SQSQueue<String> sqsQueue = new SQSDefaultQueue(sqs, queueUrl)
                .withVisibilityTimeoutSeconds(10)
                .withWaitTimeSeconds(5);

        final int messagesToSend = 100;
        this.fillQueueWithTestData(sqs, queueUrl, messagesToSend);

        final CountDownLatch countDownLatch = new CountDownLatch(messagesToSend);

        final SQSMessageHandler<String> messageHandler = new SQSDefaultMessageHandler()
        {

            @Override
            public void receivedMessage(final SQSQueue<String> queue, final ObjectMessage<String> message)
            {
                final String body = message.getBody();
                countDownLatch.countDown();
                System.out.println("message received: " + body + " - countDownLatch: " + countDownLatch.getCount());
                sqsQueue.deleteMessage(message.getReceiptHandle());
            }

        };

        final SQSConsumer<String> sqsConsumer = new SQSDefaultConsumer(sqsQueue, Executors.newCachedThreadPool(), messageHandler)
                .withMaxWorkerCount(32)
                .withMinWorkerCount(1);

        // WHEN
        sqsConsumer.start();
        final Stopwatch stopwatch = new Stopwatch().start();

        // THEN
        final int maxRuntime = 2000;
        countDownLatch.await(maxRuntime, TimeUnit.MILLISECONDS);
        final long runtime = stopwatch.stop().elapsed(TimeUnit.MILLISECONDS);
        System.out.println("runtime: " + runtime + " waiting for shutdown");
        sqsConsumer.releaseExternalResources();
        SQSUtil.deleteByUrl(sqs, queueUrl);
        assertTrue("runtime warning:  " + runtime, runtime < maxRuntime);

    }




    private void fillQueueWithTestData(final AmazonSQSAsync sqs, final String queueUrl, final int messagesToSend)
    {
        System.out.println("start sending messages...");
        int messageCount = 0;
        while (messageCount < messagesToSend)
        {
            messageCount++;
            final SendMessageRequest sendMessageRequest = new SendMessageRequest(queueUrl, "messageCount: " + messageCount + " - " + System.currentTimeMillis());
            sqs.sendMessageAsync(sendMessageRequest);
            System.out.println("send: messageID: " + messageCount);

        }
        System.out.println("all messages send!");

    }


}
