package com.mytaxi.amazonaws.sqs.converter;

import com.google.common.base.Function;


public class SQSDefaultDecoder implements Function<String, String>
{
    @Override
    public String apply(final String input)
    {
        return input;
    }
}
