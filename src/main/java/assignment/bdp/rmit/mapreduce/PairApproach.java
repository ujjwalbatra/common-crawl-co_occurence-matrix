package main.java.assignment.bdp.rmit.mapreduce;

import main.java.assignment.bdp.rmit.util.Pair;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Map implementation for Pair approach for calculation of Co-occurence matrix
 */
public class PairApproach {
    private static final Logger LOG = Logger.getLogger(PairApproach.class);

    protected enum MAPPERCOUNTER {
        RECORDS_IN,
        EMPTY_PAGE_TEXT,
        EXCEPTIONS,
        NON_PLAIN_TEXT
    }

    public static class PairMapper extends Mapper<Text, ArchiveReader, Pair, IntWritable> {
        private IntWritable one = new IntWritable(1);
        private Pair pair = new Pair();


        @Override
        public void map(Text key, ArchiveReader value, Context context) throws IOException {
            for (ArchiveRecord r : value) {
                try {
                    // check if it is a plain text file
                    if (r.getHeader().getMimetype().equals("text/plain")) {
                        int neighbors = context.getConfiguration().getInt("neighbors", 2);
                        context.getCounter(MAPPERCOUNTER.RECORDS_IN).increment(1);
                        LOG.debug(r.getHeader().getUrl() + " -- " + r.available());

                        // Convenience function that reads the full message into a raw byte array
                        byte[] rawData = IOUtils.toByteArray(r, r.available());
                        String content = new String(rawData);

                        String[] tokens = content.replaceAll("\\p{Punct}+", "").split("\\s+|\\n+|\\t+");
                        System.out.println("tokens length ============================= " + tokens.length);
                        System.out.println("words-------------------------------------------------------------------------");
                        for (String t: tokens) {
                            System.out.print(t + "-------------");
                        }
                        System.out.println("words-----------------------end--------------------------------------------------");
                        if (tokens.length > 1) {
                            for (int i = 0; i < tokens.length; i++) {
                                this.pair.setWord1(tokens[i]);

                                System.out.println("[MAP]---neigh--------------------" + neighbors);

                                int start = Math.max(i - neighbors, 0);
                                int end = (i + neighbors >= tokens.length) ? tokens.length - 1 : i + neighbors;
                                for (int j = start; j <= end; j++) {
                                    System.out.println("[MAP]---pa ir--------------------" + pair);

                                    if (j == i) continue;
                                    this.pair.setWord2(tokens[j]);
                                    context.write(this.pair, this.one);
                                }
                            }
                        }
                    } else {
                        context.getCounter(MAPPERCOUNTER.NON_PLAIN_TEXT).increment(1);
                    }

                } catch (Exception ex) {
                    LOG.error("Caught Exception", ex);
                    context.getCounter(MAPPERCOUNTER.EXCEPTIONS).increment(1);
                }
            }
        }

    }


    public static class PairReducer extends Reducer<Pair, IntWritable, Pair, IntWritable> {
        @Override
        protected void reduce(Pair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int value = 0;

            for (IntWritable val : values) {
                value += val.get();
            }

            System.out.println("[RED]: " + key + value);
            context.write(key, new IntWritable(value));
        }
    }


}
