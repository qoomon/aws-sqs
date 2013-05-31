package com.mytaxi.amazonaws.sqs;


public interface MessageHandler
{

    public void receivedMessage(MessageTask messageTask);
}
