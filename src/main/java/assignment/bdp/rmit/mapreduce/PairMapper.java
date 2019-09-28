package main.java.assignment.bdp.rmit.mapreduce;

import main.java.assignment.bdp.rmit.util.Pair;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;

import java.io.IOException;


public class PairMapper extends Mapper<Text, ArchiveReader, Pair, IntWritable> {
    private static final Logger LOG = Logger.getLogger(PairMapper.class);
    private IntWritable ONE = new IntWritable(1);
    private Pair pair = new Pair();

    protected enum MAPPERCOUNTER {
        RECORDS_IN,
        EMPTY_PAGE_TEXT,
        EXCEPTIONS,
        NON_PLAIN_TEXT
    }


    @Override
    public void map(Text key, ArchiveReader value, Context context) throws IOException {
        for (ArchiveRecord r : value) {
            try {
                // check if it is a plain text file
                if (r.getHeader().getMimetype().equals("text/plain")) {
                    int neighbors = context.getConfiguration().getInt("neighbors", 5);
                    context.getCounter(MAPPERCOUNTER.RECORDS_IN).increment(1);
                    LOG.debug(r.getHeader().getUrl() + " -- " + r.available());

                    // Convenience function that reads the full message into a raw byte array
                    byte[] rawData = IOUtils.toByteArray(r, r.available());
                    String content = new String(rawData);

                    String[] tokens = content.split("\\p{Punct}+|\\s+|\\n+|\\t+");

                    for (int i = 0; i < tokens.length; i++) {
                        if (tokens[i].length() == 0) continue;

                        this.pair.setWord1(tokens[i]);
                        int start = Math.max(i - neighbors, 0);
                        int end = (i + neighbors >= tokens.length) ? tokens.length - 1 : i + neighbors;

                        for (int j = start; j <= end; j++) {
                            if (j == i || tokens[j].length() == 0) continue;

                            this.pair.setWord2(tokens[j]);
                            context.write(this.pair, this.ONE);
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
