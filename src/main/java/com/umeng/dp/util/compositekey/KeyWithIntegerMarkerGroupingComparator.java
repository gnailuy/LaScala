package com.umeng.dp.util.compositekey;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created with IntelliJ IDEA.
 * User: yuliang
 * Date: 8/13/13
 * Time: 11:14 AM
 * Natural key Grouping Comparator for KeyWithIntegerMarker
 */
public class KeyWithIntegerMarkerGroupingComparator extends WritableComparator {
    public KeyWithIntegerMarkerGroupingComparator() {
        super(KeyWithIntegerMarker.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        KeyWithIntegerMarker k1 = (KeyWithIntegerMarker)w1;
        KeyWithIntegerMarker k2 = (KeyWithIntegerMarker)w2;

        return k1.getNaturalKey().compareTo(k2.getNaturalKey());
    }
}
