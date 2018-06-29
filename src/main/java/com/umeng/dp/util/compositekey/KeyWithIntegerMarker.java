package com.umeng.dp.util.compositekey;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yuliang
 * Date: 8/13/13
 * Time: 10:55 AM
 * Key With an Integer Marker for secondary sort
 */
public class KeyWithIntegerMarker implements WritableComparable<KeyWithIntegerMarker> {
    private String nkey; // Natural Key
    private Integer marker; // A mark code to mark different kinds of keys, for secondary sort of keys

    public KeyWithIntegerMarker() {}

    public KeyWithIntegerMarker(String nkey, Integer marker) {
        this.nkey = nkey;
        this.marker = marker;
    }

    @Override
    public String toString() {
        return (new StringBuilder())
                .append('{').append(nkey).append(',')
                .append(marker.toString()).append('}').toString();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        nkey = WritableUtils.readString(in);
        marker = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, nkey);
        out.writeInt(marker);
    }

    @Override
    public int compareTo(KeyWithIntegerMarker o) {
        int result = nkey.compareTo(o.nkey);
        if(0 == result) {
            result = marker.compareTo(o.marker);
        }
        return result;
    }

    @Override
    public int hashCode() {
        return this.nkey.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return this.toString().equals(o.toString());
    }

    public String getNaturalKey() {
        return nkey;
    }

    public void setNaturalKey(String nkey) {
        this.nkey = nkey;
    }

    public Integer getMarker() {
        return marker;
    }

    public void setMarker(Integer marker) {
        this.marker = marker;
    }
}
