package main.java.assignment.bdp.rmit.mapreduce;

import main.java.assignment.bdp.rmit.util.StripsMapWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

public class StripsMapperText extends Mapper<LongWritable, Text, Text, StripsMapWritable> {
    private static final Logger LOG = Logger.getLogger(PairMapperWarc.class);
    StripsMapWritable stripsMapWritable = new StripsMapWritable();

    private Text word = new Text();

    protected enum MAPPERCOUNTER {
        RECORDS_IN,
        EMPTY_PAGE_TEXT,
        EXCEPTIONS,
        NON_PLAIN_TEXT
    }

    // @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int neighbors = context.getConfiguration().getInt("neighbors", 5);
        String[] tokens = value.toString().split("\\s+|\\n+|\\t+");

        context.getCounter(MAPPERCOUNTER.RECORDS_IN).increment(1);

        if (tokens.length > 1) {
            for (int i = 0; i < tokens.length; i++) {
                word.set(tokens[i]);
                stripsMapWritable.clear();

                int start = Math.max(i - neighbors, 0);
                int end = (i + neighbors >= tokens.length) ? tokens.length - 1 : i + neighbors;

                for (int j = start; j <= end; j++) {
                    if (j == i) continue;
                    Text neighbor = new Text(tokens[j]);
                    if (stripsMapWritable.containsKey(neighbor)) {
                        IntWritable count = (IntWritable) stripsMapWritable.get(neighbor);
                        count.set(count.get() + 1);
                    } else {
                        stripsMapWritable.put(neighbor, new IntWritable(1));
                    }
                }
                context.write(word, stripsMapWritable);
            }
        }
    }
}
