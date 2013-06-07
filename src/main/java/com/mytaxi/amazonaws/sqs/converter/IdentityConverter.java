package com.mytaxi.amazonaws.sqs.converter;

import com.google.common.base.Function;


public class IdentityConverter<T> implements Function<T, T>
{
    @Override
    public T apply(final T input)
    {
        return input;
    }
}
