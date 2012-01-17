package com.bull.eurocontrol.csst.poc.utils;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.lang3.mutable.MutableInt;

import com.bull.eurocontrol.csst.poc.utils.MapReduceTask.AtomicTaskFactory;

import jsr166y.ForkJoinTask;
import jsr166y.RecursiveTask;

public final class MapReduceTask<A,R,P> extends RecursiveTask<R> {
    public interface Merger<R> {
        R merge(R first, R rest);
    }
    
    public interface AtomicTaskFactory<A,R,P> {
        RecursiveTask<R> create(A item, int index, P parameters);
    }

    private A[] list;
    private P parameters;

    private int from;
    private int len;
    
    private Merger<R> merger;
    private AtomicTaskFactory<A,R,P> atomicTaskFactory;
    

    
    
    
    
    
    public MapReduceTask(A[] list, P parameters, Merger<R> merger, AtomicTaskFactory<A,R,P> atomicTaskFactory) {
        super();
        this.list = list;
        this.from = 0;
        this.len = list.length;
        this.parameters = parameters;
        this.merger = merger;
        this.atomicTaskFactory = atomicTaskFactory;
    }

    
    
    
    private MapReduceTask(A[] list, P parameters, int from, int len, Merger<R> merger, AtomicTaskFactory<A,R,P> atomicTaskFactory) {
        super();
        this.list = list;
        this.parameters = parameters;
        this.from = from;
        this.len = len;
        this.merger = merger;
        this.atomicTaskFactory = atomicTaskFactory;
    }




    public MapReduceTask(
            Collection<A> tasks,
            P index,
            Merger<R> merger,
            AtomicTaskFactory<A,R,P> factory) {
        this((A[]) tasks.toArray(), index, merger, factory);
    }




    protected R merge(R first, R rest) {
        return merger.merge(first, rest);
    }

    protected RecursiveTask<R> createAtomicTask(int index) {
        return atomicTaskFactory.create(list[index], index, parameters);
    }

    
    @Override
    protected R compute() {
    
        if (len == 1) {
            // compute direct and return
            RecursiveTask<R> task = createAtomicTask(from);
            return task.invoke();
        } else {
            ForkJoinTask<R>[] tasks = new ForkJoinTask[len];
            for (int i = 0; i < len; i++) {
                ForkJoinTask<R> task = createAtomicTask(from+i).fork();
                tasks[i] = task;                
            }
            R result = tasks[0].join(); 
            tasks[0] = null;
            for (int i = 1; i < len; i++) {
                R next = tasks[i].join();
                tasks[i] = null;
                result = merge(result, next);
            }
            return result;
        }
//            int mid = len / 2;
//            if (mid == 1) {
//                // compute direct first and fork rest
//                ForkJoinTask<R> fork = new MapReduceTask<A, R, P>(list, parameters, from+1, len - 1, merger, atomicTaskFactory).fork();
//    
//                // compute first
//                R first = createAtomicTask(from).invoke();
//    
//                // join
//                R rest = fork.join();
//    
//                return merge(first, rest);
//    
//            } else {
//                // divide list in 2 fork both and merge
//                ForkJoinTask<R> fork1 = new MapReduceTask<A, R, P>(list, parameters,from, mid, merger, atomicTaskFactory).fork();
//                ForkJoinTask<R> fork2 = new MapReduceTask<A, R, P>(list, parameters,from + mid, len - mid, merger, atomicTaskFactory).fork();
//                
//                R rest1 = fork1.join();
//                R rest2 = fork2.join();
//                
//                return merge(rest1, rest2);
//            }
//        }
    
    }

}