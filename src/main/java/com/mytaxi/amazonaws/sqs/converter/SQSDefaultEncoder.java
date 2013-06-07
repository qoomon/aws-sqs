package com.mytaxi.amazonaws.sqs.converter;



public class SQSDefaultEncoder implements Encoder<String>
{
    @Override
    public String apply(final String input)
    {
        return input;
    }
}
