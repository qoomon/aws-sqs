package com.mytaxi.amazonaws.sqs;

import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.mytaxi.amazonaws.sqs.converter.IdentityConverter;


/**
 * UtilClass to continuously poll from amazon message queue
 *
 * @author bengtbrodersen
 *
 */
public class DefaultSQSAsyncQueue extends SQSAsyncQueue<String>
{

    public DefaultSQSAsyncQueue(final AmazonSQSAsync sqs, final String queueUrl)
    {
        super(sqs, queueUrl, new IdentityConverter<String>(), new IdentityConverter<String>());
    }
}
