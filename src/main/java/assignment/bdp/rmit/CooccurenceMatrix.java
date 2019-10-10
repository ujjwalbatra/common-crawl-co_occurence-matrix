package main.java.assignment.bdp.rmit;

import main.java.assignment.bdp.rmit.mapreduce.*;
import main.java.assignment.bdp.rmit.util.Pair;
import main.java.assignment.bdp.rmit.util.StripsMapWritable;
import main.java.assignment.bdp.rmit.util.WARCFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
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

        String inputPath = args[0];
        String outputPath = args[1];
        String approach = args[2];
        String mode = args[3];
        boolean isWarc = inputPath.toLowerCase().contains("warc");

        LOG.info("Input path: " + inputPath);
        FileInputFormat.addInputPath(job, new Path(inputPath));

        FileSystem fs = FileSystem.newInstance(conf);
        if (fs.exists(new Path(outputPath))) {
            fs.delete(new Path(outputPath), true);
        }

        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        if (isWarc) {
            job.setInputFormatClass(WARCFileInputFormat.class);
        } else {
            job.setInputFormatClass(TextInputFormat.class);
        }

        job.setOutputFormatClass(TextOutputFormat.class);

        CombineFileInputFormat.setMaxInputSplitSize(job, 128000000);
        CombineFileInputFormat.setMinInputSplitSize(job, 128000000);

        if (approach.equalsIgnoreCase("pair")) {

            job.setOutputKeyClass(Pair.class);
            job.setOutputValueClass(IntWritable.class);

            if (mode.equalsIgnoreCase("no")) {

                if (isWarc) job.setMapperClass(PairMapperWarc.class);
                else job.setMapperClass(PairMapperText.class);

                job.setMapperClass(PairMapperWarc.class);
                job.setReducerClass(PairReducer.class);

            } else if (mode.equalsIgnoreCase("yes")) {

                if (isWarc) job.setMapperClass(PairMapperWarc.class);
                else job.setMapperClass(PairMapperText.class);


                job.setCombinerClass(PairReducer.class);
                job.setPartitionerClass(PairPartitioner.class);
                job.setReducerClass(PairReducer.class);

            } else if (mode.equalsIgnoreCase("inmapper")) {

                if (isWarc) job.setMapperClass(PairMapperLocalAggregationWarc.class);
                else job.setMapperClass(StripsMapperText.class);

                job.setPartitionerClass(PairPartitioner.class);
                job.setReducerClass(PairReducer.class);

            }
        } else if (approach.equalsIgnoreCase("strips")) {
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(StripsMapWritable.class);
            job.setReducerClass(StripsReducer.class);

            if (isWarc) job.setMapperClass(StripsMapperWarc.class);
            else job.setMapperClass(StripsMapperText.class);

            if (!mode.equalsIgnoreCase("no")) {
                job.setCombinerClass(StripsReducer.class);
                job.setPartitionerClass(StripsPartitioner.class);
            }

        }



        if (job.waitForCompletion(true)) {
            return 0;
        } else {
            return 1;
        }
    }
}
