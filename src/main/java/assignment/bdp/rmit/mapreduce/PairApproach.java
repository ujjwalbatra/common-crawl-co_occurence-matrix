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

    public static class PairMapper extends Mapper<Text, ArchiveReader, Pair, LongWritable> {
        private StringTokenizer lineTokenizer;
        private LongWritable outVal = new LongWritable(1);
        private Pair pair = new Pair();


        @Override
        public void map(Text key, ArchiveReader value, Context context) throws IOException {
            for (ArchiveRecord r : value) {
                try {
                    // check if it is a plain text file
                    if (r.getHeader().getMimetype().equals("text/plain")) {
                        context.getCounter(MAPPERCOUNTER.RECORDS_IN).increment(1);
                        LOG.debug(r.getHeader().getUrl() + " -- " + r.available());

                        // Convenience function that reads the full message into a raw byte array
                        byte[] rawData = IOUtils.toByteArray(r, r.available());
                        String content = new String(rawData);

                        // Grab each line from the document
                        this.lineTokenizer = new StringTokenizer(content, "\n");

                        if (!this.lineTokenizer.hasMoreTokens()) {
                            context.getCounter(MAPPERCOUNTER.EMPTY_PAGE_TEXT).increment(1);
                        } else {
                            while (this.lineTokenizer.hasMoreTokens()) {
                                // grab each word from the line
                                String[] words = this.lineTokenizer.nextToken().split("\\s|\\n|\\t|\\r|\\f");

                                for (int i = 0; i < words.length; i++) {
                                    this.pair.setWord1(words[i]);
                                    for (int j = i + 1; j < words.length - 1; j++) {
                                        pair.setWord2(words[j]);
                                        context.write(this.pair, this.outVal);
                                    }
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
                value = val.get();
            }

            context.write(key, new IntWritable(value));
        }
    }


}
