package com.mytaxi.amazonaws.sqs;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.amazonaws.services.sqs.AmazonSQS;
import com.google.common.base.Preconditions;

/**
 * UtilClass to continuously poll from amazon message queue
 *
 * @author bengtbrodersen
 *
 */
public abstract class SQSAsyncObjectQueue<T>
{

    static final Logger               LOG = LoggerFactory.getLogger(SQSAsyncObjectQueue.class);

    private final AmazonSQSAsyncQueue amazonSqsQueue;




    @Autowired
    public SQSAsyncObjectQueue(final AmazonSQSAsyncQueue amazonSqsQueue)
    {
        Preconditions.checkNotNull(this.amazonSqsQueue);
        this.amazonSqsQueue = amazonSqsQueue;
    }




    public void sendMessageBatchAsync(final Collection<T> messageBodys, final int delaySeconds)
    {
        final Collection<String> encodedMessageBodys = new LinkedList<String>();
        for (final T messageBody : messageBodys)
        {
            encodedMessageBodys.add(this.encodeMessageBody(messageBody));
        }

        this.amazonSqsQueue.sendMessageBatchAsync(encodedMessageBodys, delaySeconds);
    }




    public void sendMessageAsync(final T messageBody, final int delaySeconds)
    {
        final String encodedMessageBody = this.encodeMessageBody(messageBody);
        this.amazonSqsQueue.sendMessageAsync(encodedMessageBody, delaySeconds);
    }




    public Message<T> receiveMessageAsync()
    {
        final com.amazonaws.services.sqs.model.Message receiveMessage = this.amazonSqsQueue.receiveMessageAsync();
        return receiveMessage != null ? this.convertMessage(receiveMessage) : null;
    }




    public List<Message<T>> receiveMessageBatchAsync(final int maxNumberOfMessages)
    {

        final List<com.amazonaws.services.sqs.model.Message> receiveMessages = this.amazonSqsQueue.receiveMessageBatchAsync(maxNumberOfMessages);
        final List<Message<T>> convertedMessages = new LinkedList<Message<T>>();
        for (final com.amazonaws.services.sqs.model.Message receiveMessage : receiveMessages)
        {
            convertedMessages.add(this.convertMessage(receiveMessage));
        }

        return convertedMessages;
    }




    public void deleteMessageAsync(final String receiptHandle)
    {
        this.amazonSqsQueue.deleteMessageAsync(receiptHandle);
    }




    public void deleteMessageBatchAsync(final Collection<String> receiptHandles)
    {
        this.amazonSqsQueue.deleteMessageBatchAsync(receiptHandles);
    }




    public void changeMessageVisibilityAsync(final String receiptHandle, final int visibilityTimeoutSeconds)
    {
        this.amazonSqsQueue.changeMessageVisibilityAsync(receiptHandle, visibilityTimeoutSeconds);
    }




    public void changeMessageVisibilityBatchAsync(final Collection<String> receiptHandles, final int visibilityTimeoutSeconds)
    {
        this.amazonSqsQueue.changeMessageVisibilityBatchAsync(receiptHandles, visibilityTimeoutSeconds);
    }




    public Message<T> convertMessage(final com.amazonaws.services.sqs.model.Message message)
    {
        final T decodedMessageBody = this.decodeMessageBody(message.getBody());
        final String approximateReceiveCountString = message.getAttributes().get("ApproximateReceiveCount");
        final int approximateReceiveCount = approximateReceiveCountString != null ? Integer.parseInt(approximateReceiveCountString) : 1;
        final String receiptHandle = message.getReceiptHandle();
        return new Message<T>(decodedMessageBody, approximateReceiveCount, receiptHandle);
    }





    abstract public String encodeMessageBody(T messageBody);




    abstract public T decodeMessageBody(String messageBody);



    public AmazonSQS getAmazonSqs()
    {
        return this.amazonSqsQueue.getAmazonSqs();
    }




    @Override
    public int hashCode()
    {
        return this.amazonSqsQueue.hashCode();
    }




    @Override
    public boolean equals(final Object obj)
    {
        return this.amazonSqsQueue.equals(obj);
    }




    @Override
    public String toString()
    {
        return this.amazonSqsQueue.toString();
    }

}
