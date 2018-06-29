package com.umeng.dp.util.compositekey;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by gnailuy on 12/3/14.
 */
public class KeyWithStringMarkerPartitioner extends Partitioner<KeyWithStringMarker, Writable> {

    @Override
    public int getPartition(KeyWithStringMarker key, Writable value, int numPartitions) {
        return (key.getKey().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
