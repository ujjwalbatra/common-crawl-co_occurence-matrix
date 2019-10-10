package main.java.assignment.bdp.rmit.mapreduce;

import main.java.assignment.bdp.rmit.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

public class PairMapperText extends Mapper<LongWritable, Text, Pair, IntWritable> {
    private static final Logger LOG = Logger.getLogger(PairMapperWarc.class);
    private IntWritable ONE = new IntWritable(1);
    private Pair pair = new Pair();

    protected enum MAPPERCOUNTER {
        RECORDS_IN,
        EMPTY_PAGE_TEXT,
        EXCEPTIONS,
        NON_PLAIN_TEXT
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int neighbors = context.getConfiguration().getInt("neighbors", 5);
        String[] tokens = value.toString().split("\\s+|\\n+|\\t+");

        try {
            if (tokens.length > 1) {
                for (int i = 0; i < tokens.length; i++) {
                    pair.setWord1(tokens[i]);

                    int start = Math.max(i - neighbors, 0);
                    int end = (i + neighbors >= tokens.length) ? tokens.length - 1 : i + neighbors;
                    for (int j = start; j <= end; j++) {
                        if (j == i) continue;
                        pair.setWord2(tokens[j]);
                        context.write(pair, ONE);
                    }
                }
            }
        } catch (Exception ex) {
            LOG.error("Caught Exception", ex);
            context.getCounter(MAPPERCOUNTER.EXCEPTIONS).increment(1);
        }
    }
}
