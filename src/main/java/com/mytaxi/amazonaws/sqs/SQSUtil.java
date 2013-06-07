package com.mytaxi.amazonaws.sqs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.google.common.base.Preconditions;

public final class SQSUtil
{

    private static final Logger LOG = LoggerFactory.getLogger(SQSUtil.class);


    public static String create(final AmazonSQSAsync sqs, final String queueName)
    {
        Preconditions.checkNotNull(sqs);
        Preconditions.checkNotNull(queueName);
        Preconditions.checkArgument(!queueName.isEmpty());

        LOG.info("creating queue " + queueName);
        final CreateQueueRequest createQueueRequest = new CreateQueueRequest()
                .withQueueName(queueName);
        final String queueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
        LOG.info("queue " + queueName + " created url: " + queueUrl);
        return queueUrl;
    }




    public static void delete(final AmazonSQSAsync sqs, final String queueName)
    {
        Preconditions.checkNotNull(sqs);
        Preconditions.checkNotNull(queueName);
        Preconditions.checkArgument(!queueName.isEmpty());
        final String queueUrl = getQueueUrl(sqs, queueName);
        deleteByUrl(sqs, queueUrl);
    }




    public static void deleteByUrl(final AmazonSQSAsync sqs, final String queueUrl)
    {
        Preconditions.checkNotNull(sqs);
        Preconditions.checkNotNull(queueUrl);
        Preconditions.checkArgument(!queueUrl.isEmpty());
        LOG.info("delete queue " + queueUrl);
        final DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest(queueUrl);
        sqs.deleteQueue(deleteQueueRequest);
        LOG.info("queue " + queueUrl + " deleted");
    }




    public static String getQueueUrl(final AmazonSQSAsync sqs, final String queueName)
    {
        Preconditions.checkNotNull(sqs);
        Preconditions.checkNotNull(queueName);
        Preconditions.checkArgument(!queueName.isEmpty());
        final GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(queueName);
        return sqs.getQueueUrl(getQueueUrlRequest).getQueueUrl();
    }

}
