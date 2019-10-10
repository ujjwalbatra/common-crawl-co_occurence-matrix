package main.java.assignment.bdp.rmit.mapreduce;

import main.java.assignment.bdp.rmit.util.StripsMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class StripsPartitioner extends Partitioner<Text, StripsMapWritable> {
    private static final Logger LOG = Logger.getLogger(PairPartitioner.class);

    @Override
    public int getPartition(Text key, StripsMapWritable stripsMapWritable, int numReduceTasks) {
        LOG.setLevel(Level.DEBUG);

        if (numReduceTasks == 0) {
            LOG.debug("No partitioning - only ONE reducer");
            return 0;
        }

        int reducerNumber = key.hashCode() % numReduceTasks;
        LOG.debug("Partitioning to reducer " + reducerNumber + " :" + key);
        return reducerNumber;
    }
}
