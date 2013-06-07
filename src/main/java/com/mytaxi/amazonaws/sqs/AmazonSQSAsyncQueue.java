package com.mytaxi.amazonaws.sqs;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.google.common.base.Preconditions;

/**
 * UtilClass to continuously poll from amazon message queue
 *
 * @author bengtbrodersen
 *
 */
public class AmazonSQSAsyncQueue
{

    public static final int      RECEIVED_MESSAGE_REQUEST_MAX_NUMBER_OF_MESSAGES   = 10;
    public static final int      RECEIVED_MESSAGE_REQUEST_MAX_WAIT_TIMEOUT_SECONDS = 20;

    static final Logger          LOG                                               = LoggerFactory.getLogger(AmazonSQSAsyncQueue.class);

    private final AmazonSQSAsync amazonSqs;
    private final String         queueName;
    private int                  waitTimeSeconds                                   = RECEIVED_MESSAGE_REQUEST_MAX_WAIT_TIMEOUT_SECONDS;
    private int                  visibilityTimeoutSeconds                          = 10;

    private String               queueUrl                                          = null;




    @Autowired
    public AmazonSQSAsyncQueue(final AmazonSQSAsync amazonSqs, final String queueName)
    {
        Preconditions.checkNotNull(amazonSqs);
        this.amazonSqs = amazonSqs;
        Preconditions.checkNotNull(queueName);
        this.queueName = queueName;

    }




    public void setVisibilityTimeoutSeconds(final int visibilityTimeoutSeconds)
    {
        Preconditions.checkArgument(visibilityTimeoutSeconds > 0);
        this.visibilityTimeoutSeconds = visibilityTimeoutSeconds;
    }




    public AmazonSQSAsyncQueue withVisibilityTimeoutSeconds(final int visibilityTimeoutSeconds)
    {
        this.setVisibilityTimeoutSeconds(visibilityTimeoutSeconds);
        return this;
    }




    public void setWaitTimeSeconds(final int waitTimeSeconds)
    {
        Preconditions.checkArgument(waitTimeSeconds > 0);
        Preconditions.checkArgument(waitTimeSeconds <= RECEIVED_MESSAGE_REQUEST_MAX_WAIT_TIMEOUT_SECONDS);
        this.waitTimeSeconds = waitTimeSeconds;
    }




    public AmazonSQSAsyncQueue withWaitTimeSeconds(final int waitTimeSeconds)
    {
        this.setWaitTimeSeconds(waitTimeSeconds);
        return this;
    }




    public void create()
    {
        LOG.info("creating sqs queue... name: " + this.queueName);
        final CreateQueueRequest createQueueRequest = new CreateQueueRequest().withQueueName(this.queueName);
        this.queueUrl = this.amazonSqs.createQueue(createQueueRequest).getQueueUrl();
        LOG.info("creating sqs queue done! name: " + this.queueName + " url: " + this.queueUrl);
    }




    public void delete()
    {
        Preconditions.checkState(this.isInit(), "init() first");
        LOG.info("delete sqs queue... name: " + this.queueName);
        final DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest(this.queueUrl);
        this.amazonSqs.deleteQueue(deleteQueueRequest);
        LOG.info("delete sqs queue done! name: " + this.queueName + " url: " + this.queueUrl);
        this.queueUrl = null;
    }




    public void sendMessageAsync(final String messageBody, final int delaySeconds)
    {
        Preconditions.checkNotNull(messageBody, "messageBody is null");
        Preconditions.checkState(this.isInit(), "init() first");
        final SendMessageRequest sendMessageRequest = new SendMessageRequest(this.queueUrl, messageBody)
                .withDelaySeconds(delaySeconds);

        this.amazonSqs.sendMessageAsync(sendMessageRequest);
    }




    public void sendMessageBatchAsync(final Collection<String> messageBodys, final int delaySeconds)
    {
        Preconditions.checkNotNull(messageBodys, "messageBodys is null");
        Preconditions.checkState(this.isInit(), "init() first");

        if (messageBodys.isEmpty())
        {
            return;
        }

        final List<SendMessageBatchRequestEntry> entries = new LinkedList<SendMessageBatchRequestEntry>();
        Integer id = 0;
        for (final String messageBody : messageBodys)
        {
            entries.add(new SendMessageBatchRequestEntry(id.toString(), messageBody)
                    .withDelaySeconds(delaySeconds));
            id++;
        }
        final SendMessageBatchRequest sendMessageBatchRequest = new SendMessageBatchRequest(this.queueUrl, entries);
        this.amazonSqs.sendMessageBatchAsync(sendMessageBatchRequest);

    }




    public Message receiveMessageAsync()
    {
        final List<Message> receiveMessageBatch = this.receiveMessageBatchAsync(1);
        return receiveMessageBatch.isEmpty() ? null : receiveMessageBatch.get(0);
    }




    public List<Message> receiveMessageBatchAsync(final int maxNumberOfMessages)
    {
        Preconditions.checkState(this.isInit(), "init() first");
        final ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(this.queueUrl)
                // http://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/Query_QueryReceiveMessage.html
                .withAttributeNames("ApproximateReceiveCount")
                .withWaitTimeSeconds(this.waitTimeSeconds)
                .withVisibilityTimeout(this.visibilityTimeoutSeconds)
                .withMaxNumberOfMessages(Math.min(maxNumberOfMessages, RECEIVED_MESSAGE_REQUEST_MAX_NUMBER_OF_MESSAGES));
        final Future<ReceiveMessageResult> receiveMessage = this.amazonSqs.receiveMessageAsync(receiveMessageRequest);
        try
        {
            return receiveMessage.get().getMessages();
        }
        catch (final InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        catch (final ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }




    public void deleteMessageAsync(final String receiptHandle)
    {
        Preconditions.checkNotNull(receiptHandle, "receiptHandle is null");
        Preconditions.checkState(this.isInit(), "init() first");
        final DeleteMessageRequest deleteMessageRequest =
                new DeleteMessageRequest(this.queueUrl, receiptHandle);
        this.amazonSqs.deleteMessageAsync(deleteMessageRequest);
    }




    public void deleteMessageBatchAsync(final Collection<String> receiptHandles)
    {
        Preconditions.checkNotNull(receiptHandles, "receiptHandles is null");
        Preconditions.checkState(this.isInit(), "init() first");

        if (receiptHandles.isEmpty())
        {
            return;
        }

        final List<DeleteMessageBatchRequestEntry> entries = new LinkedList<DeleteMessageBatchRequestEntry>();
        int messageIndex = 0;
        for (final String receiptHandle : receiptHandles)
        {
            entries.add(new DeleteMessageBatchRequestEntry(Integer.toString(messageIndex), receiptHandle));
            messageIndex++;
        }
        final DeleteMessageBatchRequest deleteMessageBatchRequest = new DeleteMessageBatchRequest(this.queueUrl, entries);
        this.amazonSqs.deleteMessageBatchAsync(deleteMessageBatchRequest);
    }




    public void changeMessageVisibilityAsync(final String receiptHandle, final int visibilityTimeoutSeconds)
    {
        Preconditions.checkNotNull(receiptHandle, "receiptHandle is null");
        Preconditions.checkState(this.isInit(), "init() first");

        final ChangeMessageVisibilityRequest changeMessageVisibilityRequest =
                new ChangeMessageVisibilityRequest(this.queueUrl, receiptHandle, visibilityTimeoutSeconds);
        this.amazonSqs.changeMessageVisibilityAsync(changeMessageVisibilityRequest);
    }




    public void changeMessageVisibilityBatchAsync(final Collection<String> receiptHandles, final int visibilityTimeoutSeconds)
    {
        Preconditions.checkNotNull(receiptHandles, "receiptHandles is null");
        Preconditions.checkState(this.isInit(), "init() first");

        if (receiptHandles.isEmpty())
        {
            return;
        }

        final List<ChangeMessageVisibilityBatchRequestEntry> entries =
                new LinkedList<ChangeMessageVisibilityBatchRequestEntry>();
        int messageIndex = 0;
        for (final String receiptHandle : receiptHandles)
        {
            entries.add(new ChangeMessageVisibilityBatchRequestEntry(Integer.toString(messageIndex), receiptHandle)
                    .withVisibilityTimeout(visibilityTimeoutSeconds));
            messageIndex++;
        }

        final ChangeMessageVisibilityBatchRequest changeMessageVisibilityBatchRequest =
                new ChangeMessageVisibilityBatchRequest(this.queueUrl, entries);
        this.amazonSqs.changeMessageVisibilityBatchAsync(changeMessageVisibilityBatchRequest);
    }




    public AmazonSQS getAmazonSqs()
    {
        return this.amazonSqs;
    }




    public String getQueueUrl()
    {
        return this.queueUrl;
    }




    public String getQueueName()
    {
        return this.queueName;
    }




    public int getWaitTimeSeconds()
    {
        return this.waitTimeSeconds;
    }




    public int getVisibilityTimeoutSeconds()
    {
        return this.visibilityTimeoutSeconds;
    }




    public boolean isInit()
    {
        return this.queueUrl != null;
    }

}
