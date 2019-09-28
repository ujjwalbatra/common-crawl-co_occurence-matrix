package main.java.assignment.bdp.rmit.mapreduce;

import main.java.assignment.bdp.rmit.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class PairPartitioner extends Partitioner<Pair, IntWritable> {
    private static final Logger LOG = Logger.getLogger(PairPartitioner.class);

    @Override
    public int getPartition(Pair key, IntWritable value, int numReduceTasks) {
        LOG.setLevel(Level.DEBUG);

        if (numReduceTasks == 0) {
            LOG.debug("No partitioning - only ONE reducer");
            return 0;
        }

        if (key.hashCode() % numReduceTasks == 0) {
            LOG.debug("Partitioning to reducer 0: " + key);
            return 0;
        } else if (key.hashCode() % numReduceTasks == 0) {
            LOG.debug("Partitioning to reducer 1: " + key);
            return 1;
        } else {
            LOG.debug("Partitioning to reducer 1: " + key);
            return 2;
        }
    }
}
