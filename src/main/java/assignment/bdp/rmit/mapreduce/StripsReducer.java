package main.java.assignment.bdp.rmit.mapreduce;

import main.java.assignment.bdp.rmit.util.StripsMapWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Set;

public class StripsReducer extends Reducer<Text, StripsMapWritable, Text, StripsMapWritable> {
    private StripsMapWritable stripsMapWritable = new StripsMapWritable();

    @Override
    protected void reduce(Text key, Iterable<StripsMapWritable> values, Context context) throws IOException, InterruptedException {
        stripsMapWritable.clear();
        for (StripsMapWritable value : values) {
            computeAll(value);
        }
        context.write(key, stripsMapWritable);
    }

    private void computeAll(StripsMapWritable StripsMapWritable) {
        Set<Writable> keys = StripsMapWritable.keySet();
        for (Writable key : keys) {
            IntWritable fromCount = (IntWritable) StripsMapWritable.get(key);
            if (stripsMapWritable.containsKey(key)) {
                IntWritable count = (IntWritable) stripsMapWritable.get(key);
                count.set(count.get() + fromCount.get());
            } else {
                stripsMapWritable.put(key, fromCount);
            }
        }
    }
}
