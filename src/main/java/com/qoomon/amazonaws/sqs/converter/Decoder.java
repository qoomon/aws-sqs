package com.qoomon.amazonaws.sqs.converter;

import com.google.common.base.Function;

public interface Decoder<T> extends Function<String, T>
{

    @Override
    public T apply(String input);
}
