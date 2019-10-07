package main.java.assignment.bdp.rmit.mapreduce;

import main.java.assignment.bdp.rmit.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PairReducer extends Reducer<Pair, IntWritable, Pair, IntWritable> {
    @Override
    protected void reduce(Pair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int value = 0;

        for (IntWritable val : values) {
            value += val.get();
        }

        context.write(key, new IntWritable(value));
    }
}