package com.bull.eurocontrol.csst.poc.utils;

public interface Converter<F, T> {

    
    
    T convert(F from);
}
