package com.umeng.dp.util.compositekey;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by gnailuy on 12/3/14.
 */
public class KeyWithStringMarker implements WritableComparable<KeyWithStringMarker> {

    private Text key = null;
    private Text marker = null;

    public KeyWithStringMarker() {
        this.key = new Text();
        this.marker = new Text();
    }

    public KeyWithStringMarker(String key, String marker) {
        this.key = new Text(key);
        this.marker = new Text(marker);
    }

    public KeyWithStringMarker(Text key, Text marker) {
        this.key = key;
        this.marker = marker;
    }

    public Text getKey() {
        return key;
    }

    public String getKeyStr() {
        return key.toString();
    }

    public Text getMarker() {
        return marker;
    }

    public String getMarkerStr() {
        return marker.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        key.write(out);
        marker.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        key.readFields(in);
        marker.readFields(in);
    }

    @Override
    public int compareTo(KeyWithStringMarker o) {
        int code = this.key.compareTo(o.key);
        if (code == 0) {
            return this.marker.compareTo(o.marker);
        }
        return code;
    }

    @Override
    public boolean equals(Object o) {
        return this.compareTo((KeyWithStringMarker) o) == 0;
    }

    public boolean keyEquals(Object o) {
        return this.getKeyStr().equals(((KeyWithStringMarker) o).getKeyStr());
    }

    @Override
    public String toString() {
        return key.toString() + " : " + marker.toString();
    }
}
