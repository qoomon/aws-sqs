package com.qoomon.amazonaws.sqs.queue;

import java.util.Map;

import com.amazonaws.services.sqs.model.Message;
import com.google.common.base.Function;

public class ObjectMessage<T>
{

    private final Message             message;
    private final Function<String, T> decoder;
    private T                         body                    = null;
    private int                       approximateReceiveCount = -1;




    public ObjectMessage(final Message message, final Function<String, T> decoder)
    {
        super();
        this.message = message;
        this.decoder = decoder;
    }




    public Message getMessage()
    {
        return this.message;
    }




    public String getId()
    {
        return this.getMessage().getMessageId();
    }




    public String getMD5OfBody()
    {
        return this.getMessage().getMD5OfBody();
    }




    public String getReceiptHandle()
    {
        return this.getMessage().getReceiptHandle();
    }




    public Map<String, String> getAttributes()
    {
        return this.getMessage().getAttributes();
    }




    public T getBody()
    {

        if (this.body == null)
        {
            this.body = this.decoder.apply(this.getMessage().getBody());
        }

        return this.body;
    }




    public int getApproximateReceiveCount()
    {
        if (this.approximateReceiveCount < 0)
        {
            final String approximateReceiveCountString = this.getAttributes().get("ApproximateReceiveCount");
            if (approximateReceiveCountString != null)
            {
                this.approximateReceiveCount = Integer.parseInt(approximateReceiveCountString);
            }
            else
            {
                this.approximateReceiveCount = 1;
            }
        }

        return this.approximateReceiveCount;
    }





}
