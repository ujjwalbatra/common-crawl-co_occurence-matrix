package main.java.assignment.bdp.rmit.util;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import java.util.Set;

public class PairMapWritable extends MapWritable {
    @Override
    public String toString() {
        StringBuilder s = new StringBuilder("{ ");
        Set<Writable> keys = this.keySet();

        for (Writable key : keys) {
            LongWritable count = (LongWritable) this.get(key);
            s.append(key.toString()).append("=").append(count.toString()).append(",");
        }

        s.append(" }");
        return s.toString();
    }
}