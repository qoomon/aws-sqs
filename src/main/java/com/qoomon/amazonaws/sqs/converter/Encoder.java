package com.qoomon.amazonaws.sqs.converter;

import com.google.common.base.Function;


public interface Encoder<F> extends Function<F, String>
{

    @Override
    public String apply(F input);
}
