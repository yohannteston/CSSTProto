package com.bull.eurocontrol.csst.poc.utils;

public interface IJTransformer<F,T> {

    T transform(int i, int j, F val);
    
}
