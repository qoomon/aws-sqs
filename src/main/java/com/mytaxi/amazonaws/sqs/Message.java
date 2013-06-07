package com.mytaxi.amazonaws.sqs;


public class Message<T>
{
 private final T body;
    private final int approximateReceiveCount;
    private final String receiptHandle;




    public Message(final T body, final int approximateReceiveCount, final String receiptHandle)
    {
        super();
        this.body = body;
        this.approximateReceiveCount = approximateReceiveCount;
        this.receiptHandle = receiptHandle;
    }




    public String getReceiptHandle()
    {
        return this.receiptHandle;
    }


    public T getBody()
    {
        return this.body;
    }




    public int getApproximateReceiveCount()
    {
        return this.approximateReceiveCount;
    }

}
