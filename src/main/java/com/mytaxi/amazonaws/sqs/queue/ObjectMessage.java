package com.mytaxi.amazonaws.sqs.queue;

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




    public String getReceiptHandle()
    {
        return this.message.getReceiptHandle();
    }




    public T getBody()
    {

        if (this.body == null)
        {
            this.body = this.decoder.apply(this.message.getBody());
        }

        return this.body;
    }




    public int getApproximateReceiveCount()
    {
        if (this.approximateReceiveCount < 0)
        {
            final String approximateReceiveCountString = this.message.getAttributes().get("ApproximateReceiveCount");
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




    public Message getMessage()
    {
        return this.message;
    }

}
