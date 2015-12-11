package cs6240;

import edu.cmu.lemurproject.WarcFileInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class PageRank extends Configured implements Tool {

    private static final String MAX_FILES_KEY = "pagerank.max.files";
    private static final int MAX_MAP_TASK_FAILURES_PERCENT = 5;

    public static void main(String[] args) throws Exception {
        int exitCode = 0;
        exitCode = ToolRunner.run(new PageRank(), args);
        System.exit(exitCode);
    }

    /*
     * Runs the Hadoop job which takes in 3 arguments.
     *
     * Input Path: 	S3 location of a Common Crawl segment.
     * Output Path: S3 location for storing the segment results.
     * Max Files: 	Optionally restricts how many Common Crawl files
     * 				are processed.
     */

    public int run(String[] args) throws Exception {
        // Get current configuration.
        Configuration conf = getConf();

        // Parse command line arguments.
        String inputPath = args[0];
        String outputPath = args[1];

        String maxArcFiles = "";
        if (args.length == 3)
            maxArcFiles = args[2];

        // Set the maximum number of arc files to process.
        conf.set(MAX_FILES_KEY, maxArcFiles);

        JobConf job = new JobConf(conf);

        // Set input path.
        if (inputPath.length() > 0) {
            FileInputFormat.addInputPath(job, new Path(inputPath));
        } else {
            System.err.println("No input path found.");
            return 1;
        }

        // Set output path.
        if (outputPath.length() > 0) {
            SequenceFileOutputFormat.setOutputPath(job, new Path(outputPath));
        } else {
            System.err.println("No output path found.");
            return 1;
        }

        // Load other classes from same jar a this class.
        job.setJarByClass(PageRank.class);

        // Input is in WARC file format.
        job.setInputFormat(WarcFileInputFormat.class);

        // Output is Hadoop sequence file format.
        job.setOutputFormat(SequenceFileOutputFormat.class);

        // Set the output data types.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LinkArrayWritable.class);

        // Use custom mapper class.
        job.setMapRunnerClass(PageRankMapper.class);

        // Use custom reducer class.
        job.setReducerClass(PageRankReducer.class);

        // Allow 5 percent of map tasks to fail.
        job.setMaxMapTaskFailuresPercent(MAX_MAP_TASK_FAILURES_PERCENT);

        if (JobClient.runJob(job).isSuccessful())
            return 0;
        else
            return 1;
    }
}