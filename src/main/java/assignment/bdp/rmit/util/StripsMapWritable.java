package main.java.assignment.bdp.rmit.util;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import java.util.Set;

public class StripsMapWritable extends MapWritable {
    @Override
    public String toString() {
        StringBuilder s = new StringBuilder("{ ");
        Set<Writable> keys = this.keySet();

        for (Writable key : keys) {
            IntWritable count = (IntWritable) this.get(key);
            s.append(key.toString()).append("=").append(count.toString()).append(",");
        }

        s.append(" }");
        return s.toString();
    }
}