package com.mytaxi.amazonaws.sqs;

import com.amazonaws.services.sqs.model.Message;

public class MessageTask
{

    private final Message        message;
    private final AmazonSQSQueue queue;




    public MessageTask(final Message message, final AmazonSQSQueue queue)
    {
        super();
        this.message = message;
        this.queue = queue;
    }




    public String getBody()
    {
        return this.message.getBody();
    }




    public void changeVisibility(final int visibilityTimeoutSeconds)
    {
        this.queue.changeVisibility(this.message, visibilityTimeoutSeconds);
    }




    public void delete()
    {
        this.queue.delete(this.message);
    }

}
