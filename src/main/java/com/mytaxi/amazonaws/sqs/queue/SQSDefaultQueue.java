package com.mytaxi.amazonaws.sqs.queue;

import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.mytaxi.amazonaws.sqs.converter.SQSDefaultDecoder;
import com.mytaxi.amazonaws.sqs.converter.SQSDefaultEncoder;


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
