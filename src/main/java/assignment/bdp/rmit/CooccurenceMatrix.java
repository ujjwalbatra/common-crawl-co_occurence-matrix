package main.java.assignment.bdp.rmit;

import main.java.assignment.bdp.rmit.mapreduce.*;
import main.java.assignment.bdp.rmit.util.Pair;
import main.java.assignment.bdp.rmit.util.WARCFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


/**
 * Co-occurence matrix using the extract test for Common Crawl Dataset
 *
 * @author Ujjwal Batra
 */
public class CooccurenceMatrix extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(CooccurenceMatrix.class);

    /**
     * {@link ToolRunner} is used for running the Hadoop job.
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CooccurenceMatrix(), args);
        System.exit(res);
    }

    /**
     * Builds and runs the Hadoop job.
     *
     * @return 0 if the Hadoop job completes successfully and 1 otherwise.
     */
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        //
        Job job = new Job(conf);
        job.setJarByClass(CooccurenceMatrix.class);

        String input = args[0];
        String output = args[1];
        String approach = args[2];
        String mode = args[3];

        LOG.info("Input path: " + input);
        FileInputFormat.addInputPath(job, new Path(input));

        FileSystem fs = FileSystem.newInstance(conf);
        if (fs.exists(new Path(output))) {
            fs.delete(new Path(output), true);
        }


        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setInputFormatClass(WARCFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Pair.class);
        job.setOutputValueClass(IntWritable.class);

        CombineFileInputFormat.setMaxInputSplitSize(job, 128000000);
        CombineFileInputFormat.setMinInputSplitSize(job, 128000000);

        if (approach.equalsIgnoreCase("pair")) {
            if (mode.equalsIgnoreCase("no")) {

                job.setMapperClass(PairMapper.class);
                job.setReducerClass(PairReducer.class);

            } else if (mode.equalsIgnoreCase("yes")) {

                job.setMapperClass(PairMapper.class);
                job.setCombinerClass(PairReducer.class);
                job.setPartitionerClass(PairPartitioner.class);
                job.setReducerClass(PairReducer.class);

            } else if (mode.equalsIgnoreCase("inmapper")) {

                job.setMapperClass(PairMapperLocalAggregation.class);
                job.setPartitionerClass(PairPartitioner.class);
                job.setReducerClass(PairReducer.class);

            }
        } else if (approach.equalsIgnoreCase("strips")) {
            job.setMapperClass(StripsMapper.class);
            job.setReducerClass(StripsReducer.class);
        }


        if (job.waitForCompletion(true)) {
            return 0;
        } else {
            return 1;
        }
    }
}
