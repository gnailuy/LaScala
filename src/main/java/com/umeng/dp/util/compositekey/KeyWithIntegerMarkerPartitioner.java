package com.umeng.dp.util.compositekey;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created with IntelliJ IDEA.
 * User: yuliang
 * Date: 8/13/13
 * Time: 11:18 AM
 * Partitioner for KeyWithIntegerMarker
 */
public class KeyWithIntegerMarkerPartitioner extends Partitioner<KeyWithIntegerMarker, Writable> {

    @Override
    public int getPartition(KeyWithIntegerMarker key, Writable val, int numReduceTasks) {
        return (StringHelper.hash32(key.getNaturalKey()) & Integer.MAX_VALUE) % numReduceTasks;
    }
}
