package com.bull.aurocontrol.csst.poc.index;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

import org.apache.commons.lang3.mutable.MutableInt;

import com.bull.eurocontrol.csst.poc.utils.Combiner;
import com.bull.eurocontrol.csst.poc.utils.SMatrix;
import com.bull.eurocontrol.csst.poc.utils.SVector;
import com.bull.eurocontrol.csst.poc.utils.MapReduceTask.Merger;

public final class MutableIntMatrixAdder implements Merger<SMatrix<Integer>> {
    @Override
    public SMatrix<Integer> merge(SMatrix<Integer> first, SMatrix<Integer> rest) {
        if (first == null) return rest;
        if (rest == null) return first;
//        System.out.println("first");
//        System.out.println(first);
//        System.out.println("rest");
//        System.out.println(rest);
        
        SMatrix<Integer> result = SMatrix.combine(first,rest, new Combiner<Integer>() {

            @Override
            public Integer combine(Integer a, Integer b) {
               
                return a + b;
            }
        });
//        System.out.println("result");
//        System.out.println(result);
        return result;
        
    }
}