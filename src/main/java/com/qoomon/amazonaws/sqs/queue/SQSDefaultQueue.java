package com.qoomon.amazonaws.sqs.queue;

import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.qoomon.amazonaws.sqs.converter.SQSDefaultDecoder;
import com.qoomon.amazonaws.sqs.converter.SQSDefaultEncoder;


/**
 * UtilClass to continuously poll from amazon message queue
 *
 * @author bengtbrodersen
 *
 */
public class SQSDefaultQueue extends SQSQueue<String>
{

    public SQSDefaultQueue(final AmazonSQSAsync sqs, final String queueUrl)
    {
        super(sqs, queueUrl, new SQSDefaultEncoder(), new SQSDefaultDecoder());
    }
}
