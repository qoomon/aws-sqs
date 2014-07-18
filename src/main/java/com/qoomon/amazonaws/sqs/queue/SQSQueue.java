package com.qoomon.amazonaws.sqs.queue;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;

/**
 * UtilClass to continuously poll from amazon message queue
 *
 * @author bengtbrodersen
 *
 */
public class SQSQueue<T>
{

    static final Logger               LOG                                                      = LoggerFactory.getLogger(SQSQueue.class);

    public static final int           RECEIVED_MESSAGE_REQUEST_MAX_INVISIBILITY_TIMEOUT_SECONDS = 1000 * 60 * 60 * 12;
    public static final int           RECEIVED_MESSAGE_REQUEST_MAX_NUMBER_OF_MESSAGES          = 10;
    public static final int           RECEIVED_MESSAGE_REQUEST_MAX_WAIT_TIMEOUT_SECONDS        = 20;

    private final AmazonSQS           sqs;
    private final Function<T, String> encoder;
    private final Function<String, T> decoder;

    private final String              queueUrl;
    private int                       waitTimeSeconds                                          = RECEIVED_MESSAGE_REQUEST_MAX_WAIT_TIMEOUT_SECONDS;
    private int                       visibilityTimeoutSeconds                                 = 10;




    public SQSQueue(final AmazonSQS sqs, final String queueUrl,
            final Function<T, String> encoder,
            final Function<String, T> decoder)
    {
        this.sqs = Preconditions.checkNotNull(sqs);
        this.queueUrl = queueUrl;
        this.encoder = Preconditions.checkNotNull(encoder);
        this.decoder = Preconditions.checkNotNull(decoder);
    }


    public void sendMessage(final T messageBody, final int delaySeconds)
    {
        Preconditions.checkNotNull(messageBody, "messageBody is null");

        final String encodedMessageBody = this.encoder.apply(messageBody);
        final SendMessageRequest sendMessageRequest = new SendMessageRequest(this.queueUrl, encodedMessageBody)
                .withDelaySeconds(delaySeconds);

        this.sqs.sendMessage(sendMessageRequest);
    }




    public void sendMessageBatch(final Collection<T> messageBodys, final int delaySeconds)
    {
        Preconditions.checkNotNull(messageBodys, "messageBodys is null");

        if (messageBodys.isEmpty())
        {
            return;
        }

        final List<SendMessageBatchRequestEntry> entries = new LinkedList<SendMessageBatchRequestEntry>();
        int index = 0;
        for (final T messageBody : messageBodys)
        {
            final String encodedMessageBody = this.encoder.apply(messageBody);
            entries.add(new SendMessageBatchRequestEntry(Integer.toString(index), encodedMessageBody)
                    .withDelaySeconds(delaySeconds));
            index++;
        }
        final SendMessageBatchRequest sendMessageBatchRequest = new SendMessageBatchRequest(this.queueUrl, entries);
        this.sqs.sendMessageBatch(sendMessageBatchRequest);

    }




    public ObjectMessage<T> receiveMessage()
    {
        final List<ObjectMessage<T>> receiveMessages = this.receiveMessageBatch(1);
        if (!receiveMessages.isEmpty())
        {
            return receiveMessages.get(0);
        }
        return null;
    }




    public List<ObjectMessage<T>> receiveMessageBatch(final int maxNumberOfMessages)
    {
        Preconditions.checkArgument(maxNumberOfMessages > 0, "maxNumberOfMessages <= 0");

        final ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(this.queueUrl)
                // http://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/Query_QueryReceiveMessage.html
                .withAttributeNames("ApproximateReceiveCount")
                .withWaitTimeSeconds(this.waitTimeSeconds)
                .withVisibilityTimeout(this.visibilityTimeoutSeconds)
                .withMaxNumberOfMessages(Math.min(maxNumberOfMessages, RECEIVED_MESSAGE_REQUEST_MAX_NUMBER_OF_MESSAGES));
        final List<Message> receiveMessages = this.sqs.receiveMessage(receiveMessageRequest).getMessages();
        final List<ObjectMessage<T>> convertedMessages = new LinkedList<ObjectMessage<T>>();
        for (final Message receiveMessage : receiveMessages)
        {
            convertedMessages.add(new ObjectMessage<T>(receiveMessage, this.decoder));
        }
        return convertedMessages;
    }




    public void deleteMessage(final String receiptHandle)
    {
        Preconditions.checkNotNull(receiptHandle, "receiptHandle is null");

        final DeleteMessageRequest deleteMessageRequest =
                new DeleteMessageRequest(this.queueUrl, receiptHandle);
        this.sqs.deleteMessage(deleteMessageRequest);
    }




    public void deleteMessageBatch(final Collection<String> receiptHandles)
    {
        Preconditions.checkNotNull(receiptHandles, "receiptHandles is null");
        Preconditions.checkArgument(!receiptHandles.isEmpty(), "receiptHandles is empty");

        final List<DeleteMessageBatchRequestEntry> entries = new LinkedList<DeleteMessageBatchRequestEntry>();
        int messageIndex = 0;
        for (final String receiptHandle : receiptHandles)
        {
            entries.add(new DeleteMessageBatchRequestEntry(Integer.toString(messageIndex), receiptHandle));
            messageIndex++;
        }
        final DeleteMessageBatchRequest deleteMessageBatchRequest = new DeleteMessageBatchRequest(this.queueUrl, entries);
        this.sqs.deleteMessageBatch(deleteMessageBatchRequest);
    }




    public void changeMessageVisibility(final String receiptHandle, final int visibilityTimeoutSeconds)
    {
        Preconditions.checkNotNull(receiptHandle, "receiptHandle is null");

        final ChangeMessageVisibilityRequest changeMessageVisibilityRequest =
                new ChangeMessageVisibilityRequest(this.queueUrl, receiptHandle, visibilityTimeoutSeconds);
        this.sqs.changeMessageVisibility(changeMessageVisibilityRequest);
    }




    public void changeMessageVisibilityBatch(final Collection<String> receiptHandles, final int visibilityTimeoutSeconds)
    {
        Preconditions.checkNotNull(receiptHandles, "receiptHandles is null");
        Preconditions.checkArgument(!receiptHandles.isEmpty(), "receiptHandles is empty");

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
        this.sqs.changeMessageVisibilityBatch(changeMessageVisibilityBatchRequest);
    }




    public void setVisibilityTimeoutSeconds(final int visibilityTimeoutSeconds)
    {
        Preconditions.checkArgument(visibilityTimeoutSeconds > 0);
        Preconditions.checkArgument(visibilityTimeoutSeconds <= RECEIVED_MESSAGE_REQUEST_MAX_INVISIBILITY_TIMEOUT_SECONDS);

        this.visibilityTimeoutSeconds = visibilityTimeoutSeconds;
    }




    public SQSQueue<T> withVisibilityTimeoutSeconds(final int visibilityTimeoutSeconds)
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




    public SQSQueue<T> withWaitTimeSeconds(final int waitTimeSeconds)
    {
        this.setWaitTimeSeconds(waitTimeSeconds);
        return this;
    }




    public AmazonSQS getSqs()
    {
        return this.sqs;
    }




    public String getQueueUrl()
    {
        return this.queueUrl;
    }




    public int getWaitTimeSeconds()
    {
        return this.waitTimeSeconds;
    }




    public int getVisibilityTimeoutSeconds()
    {
        return this.visibilityTimeoutSeconds;
    }

}
