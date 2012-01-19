package com.bull.eurocontrol.csst.poc.utils;

public class ITransformerUtils {

    private static final ITransformer<?, ?> NOP = new ITransformer() {
        public final Object transform(int i, Object val) { return val; };
    };
    
    
    
    public static final <V> ITransformer<V, V> nop() {
        return (ITransformer<V, V>) NOP;
    }
}
