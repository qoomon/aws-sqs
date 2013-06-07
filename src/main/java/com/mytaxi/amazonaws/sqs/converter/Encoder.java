package com.mytaxi.amazonaws.sqs.converter;

import com.google.common.base.Function;


public interface Encoder<F> extends Function<F, String>
{

}
