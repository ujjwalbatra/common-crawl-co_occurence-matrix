package main.java.assignment.bdp.rmit;

import main.java.assignment.bdp.rmit.mapreduce.PairApproach;
import main.java.assignment.bdp.rmit.util.WARCFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
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
     * @return	0 if the Hadoop job completes successfully and 1 otherwise.
     */
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        //
        Job job = new Job(conf);
        job.setJarByClass(CooccurenceMatrix.class);
        job.setNumReduceTasks(1);

        String inputPath = "/user/ujjwalbatra/*.warc.wet.gz";
        LOG.info("Input path: " + inputPath);
        FileInputFormat.addInputPath(job, new Path(inputPath));

        String outputPath = "/user/hadoop/test/";
        FileSystem fs = FileSystem.newInstance(conf);
        if (fs.exists(new Path(outputPath))) {
            fs.delete(new Path(outputPath), true);
        }
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setInputFormatClass(WARCFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(PairApproach.PairMapper.class);
        job.setReducerClass(PairApproach.PairReducer.class);
        job.setCombinerClass(PairApproach.PairReducer.class);

        if (job.waitForCompletion(true)) {
            return 0;
        } else {
            return 1;
        }
    }
}
