package com.bull.eurocontrol.csst.poc.utils;

import org.apache.commons.collections.Transformer;

public interface ITransformer<V,T> {
    public T transform(int i, V val);
}
