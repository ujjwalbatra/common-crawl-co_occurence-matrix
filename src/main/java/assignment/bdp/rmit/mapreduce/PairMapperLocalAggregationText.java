package main.java.assignment.bdp.rmit.mapreduce;

import main.java.assignment.bdp.rmit.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;

public class PairMapperLocalAggregationText extends Mapper<LongWritable, Text, Pair, IntWritable> {
    private static final Logger LOG = Logger.getLogger(PairMapperWarc.class);

    protected enum MAPPERCOUNTER {
        RECORDS_IN,
        EMPTY_PAGE_TEXT,
        EXCEPTIONS,
        NON_PLAIN_TEXT
    }


    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException {

        try {
            int neighbors = context.getConfiguration().getInt("neighbors", 5);
            context.getCounter(MAPPERCOUNTER.RECORDS_IN).increment(1);
            String[] tokens = value.toString().split("\\s+|\\n+|\\t+");

            HashMap<Pair, Integer> map = new HashMap<>();

            for (int i = 0; i < tokens.length; i++) {
                Pair pair = new Pair();
                if (tokens[i].length() == 0) continue;

                pair.setWord1(tokens[i]);
                int start = Math.max(i - neighbors, 0);
                int end = (i + neighbors >= tokens.length) ? tokens.length - 1 : i + neighbors;

                for (int j = start; j <= end; j++) {
                    if (j == i || tokens[j].length() == 0) continue;

                    pair.setWord2(tokens[j]);
                    if (map.containsKey(pair)) {
                        map.replace(pair, map.get(pair) + 1);
                    } else {
                        map.put(pair, 1);
                    }
                }

                for (Pair wordPair : map.keySet()) {
                    context.write(wordPair, new IntWritable(map.get(wordPair)));
                }

            }
        } catch (Exception ex) {
            LOG.error("Caught Exception", ex);
            context.getCounter(MAPPERCOUNTER.EXCEPTIONS).increment(1);
        }
    }
}
