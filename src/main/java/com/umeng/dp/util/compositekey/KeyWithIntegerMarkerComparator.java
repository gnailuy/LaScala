package com.umeng.dp.util.compositekey;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created with IntelliJ IDEA.
 * User: yuliang
 * Date: 8/13/13
 * Time: 10:55 AM
 * Comparator of KeyWithIntegerMarker
 */
public class KeyWithIntegerMarkerComparator extends WritableComparator {
    public KeyWithIntegerMarkerComparator() {
        super(KeyWithIntegerMarker.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        KeyWithIntegerMarker k1 = (KeyWithIntegerMarker)w1;
        KeyWithIntegerMarker k2 = (KeyWithIntegerMarker)w2;

        int result = k1.getNaturalKey().compareTo(k2.getNaturalKey());
        if(0 == result) {
            result = k1.getMarker().compareTo(k2.getMarker());
        }
        return result;
    }
}
