package com.mytaxi.amazonaws.sqs;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
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
public class AmazonSQSQueue
{

    public static final int RECEIVED_MESSAGE_REQUEST_MAX_NUMBER_OF_MESSAGES   = 10;
    public static final int RECEIVED_MESSAGE_REQUEST_MAX_WAIT_TIMEOUT_SECONDS = 20;

    static final Logger     LOG                                               = LoggerFactory.getLogger(AmazonSQSQueue.class);

    private final AmazonSQS amazonSqs;
    private final String    queueName;
    private final int       waitTimeSeconds;
    private final int       visibilityTimeoutSeconds;

    private String          queueUrl                                          = null;




    @Autowired
    public AmazonSQSQueue(
            final AmazonSQS amazonSqs, final String queueName, final int waitTimeSeconds, final int visibilityTimeoutSeconds)
    {
        Preconditions.checkNotNull(amazonSqs);
        this.amazonSqs = amazonSqs;
        Preconditions.checkNotNull(queueName);
        this.queueName = queueName;
        Preconditions.checkArgument(waitTimeSeconds > 0);
        Preconditions.checkArgument(waitTimeSeconds <= RECEIVED_MESSAGE_REQUEST_MAX_WAIT_TIMEOUT_SECONDS);
        this.waitTimeSeconds = waitTimeSeconds;
        Preconditions.checkArgument(visibilityTimeoutSeconds > 0);
        this.visibilityTimeoutSeconds = visibilityTimeoutSeconds;
    }




    public void init()
    {
        LOG.info("creating sqs queue... name: " + this.queueName);
        final CreateQueueRequest createQueueRequest = new CreateQueueRequest().withQueueName(this.queueName);
        this.queueUrl = this.amazonSqs.createQueue(createQueueRequest).getQueueUrl();
        LOG.info("creating sqs queue done! name: " + this.queueName + " url: " + this.queueUrl);
    }




    public void sendMessage(final String messageBody, final int delaySeconds)
    {
        Preconditions.checkNotNull(messageBody, "messageBody is null");
        Preconditions.checkState(this.isInit(), "init() first");
        final SendMessageRequest sendMessageRequest = new SendMessageRequest(this.queueUrl, messageBody)
        .withDelaySeconds(delaySeconds);
        this.amazonSqs.sendMessage(sendMessageRequest);

    }




    public void sendMessage(final Collection<String> messageBodys, final int delaySeconds)
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
        this.amazonSqs.sendMessageBatch(sendMessageBatchRequest);

    }




    public Message receiveMessage()
    {
        final List<Message> receiveMessageBatch = this.receiveMessageBatch(1);
        return receiveMessageBatch.isEmpty() ? null : receiveMessageBatch.get(0);
    }




    public List<Message> receiveMessageBatch(final int maxNumberOfMessages)
    {
        Preconditions.checkState(this.isInit(), "init() first");
        final ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(this.queueUrl)
        .withWaitTimeSeconds(this.waitTimeSeconds)
        .withVisibilityTimeout(this.visibilityTimeoutSeconds)
        .withMaxNumberOfMessages(Math.min(maxNumberOfMessages, RECEIVED_MESSAGE_REQUEST_MAX_NUMBER_OF_MESSAGES));
        final ReceiveMessageResult receiveMessage = this.amazonSqs.receiveMessage(receiveMessageRequest);
        return receiveMessage.getMessages();
    }




    public void delete(final Message message)
    {
        Preconditions.checkNotNull(message, "message is null");
        Preconditions.checkState(this.isInit(), "init() first");
        final DeleteMessageRequest deleteMessageRequest =
                new DeleteMessageRequest(this.queueUrl, message.getReceiptHandle());
        this.amazonSqs.deleteMessage(deleteMessageRequest);
    }




    protected void delete(final Collection<Message> messages)
    {
        Preconditions.checkNotNull(messages, "messages is null");
        Preconditions.checkState(this.isInit(), "init() first");

        if (messages.isEmpty())
        {
            return;
        }

        final List<DeleteMessageBatchRequestEntry> entries = new LinkedList<DeleteMessageBatchRequestEntry>();
        for (final Message message : messages)
        {
            entries.add(new DeleteMessageBatchRequestEntry(message.getMessageId(), message.getReceiptHandle()));
        }
        final DeleteMessageBatchRequest deleteMessageBatchRequest = new DeleteMessageBatchRequest(this.queueUrl, entries);
        this.amazonSqs.deleteMessageBatch(deleteMessageBatchRequest);
    }




    public void changeVisibility(final Message message, final int visibilityTimeoutSeconds)
    {
        Preconditions.checkNotNull(message, "message is null");
        Preconditions.checkState(this.isInit(), "init() first");
        final ChangeMessageVisibilityRequest changeMessageVisibilityRequest =
                new ChangeMessageVisibilityRequest(this.queueUrl, message.getReceiptHandle(), visibilityTimeoutSeconds);
        this.amazonSqs.changeMessageVisibility(changeMessageVisibilityRequest);
    }




    protected void changeVisibility(final Collection<Message> messages, final int visibilityTimeoutSeconds)
    {
        Preconditions.checkNotNull(messages, "messages is null");
        Preconditions.checkState(this.isInit(), "init() first");

        if (messages.isEmpty())
        {
            return;
        }

        final List<ChangeMessageVisibilityBatchRequestEntry> entries = new LinkedList<ChangeMessageVisibilityBatchRequestEntry>();
        for (final Message message : messages)
        {
            entries.add(new ChangeMessageVisibilityBatchRequestEntry(message.getMessageId(), message.getReceiptHandle())
            .withVisibilityTimeout(visibilityTimeoutSeconds));
        }

        final ChangeMessageVisibilityBatchRequest changeMessageVisibilityBatchRequest =
                new ChangeMessageVisibilityBatchRequest(this.queueUrl, entries);
        this.amazonSqs.changeMessageVisibilityBatch(changeMessageVisibilityBatchRequest);
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
