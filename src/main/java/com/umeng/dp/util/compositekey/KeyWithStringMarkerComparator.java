package com.umeng.dp.util.compositekey;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by gnailuy on 12/3/14.
 */

public class KeyWithStringMarkerComparator extends WritableComparator {

    public KeyWithStringMarkerComparator() {
        super(KeyWithStringMarker.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        KeyWithStringMarker keyA = (KeyWithStringMarker) a;
        KeyWithStringMarker keyB = (KeyWithStringMarker) b;
        return keyA.compareTo(keyB);
    }
}
