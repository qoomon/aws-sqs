package com.qoomon.amazonaws.sqs.converter;



public class SQSDefaultDecoder implements Decoder<String>
{
    @Override
    public String apply(final String input)
    {
        return input;
    }
}
