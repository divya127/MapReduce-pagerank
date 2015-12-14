package edu.neu.mapreduce.project;

// Java classes
import java.net.URI;
// Hadoop classes
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
// Apache Project classes
import org.apache.log4j.Logger;

/**
 * Use of Common Crawl 'metadata' files to quickly gather high level information about the corpus'
 * content such as href which resides in links.
 *
 * @author Preety Mishra
 * @author Divya Devaraj
 * @author Christoforus Benvenuto
 */
public class PageRank extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(PageRank.class);
    private static final String MAX_FILES_KEY = "pagerank.max.files";

    public static class FileCountFilter extends Configured implements PathFilter {

        private static final int DEFAULT_MAX_FILES = 9999999;

        private static int fileCount = 0;
        private static int maxFiles = 0;

        // Called once per file to be processed.  Returns true until max files has been reached.
        public boolean accept(Path path) {

            // If max files hasn't been set then set it to the configured value.
            if (FileCountFilter.maxFiles == 0) {
                Configuration conf = getConf();
                String confValue = conf.get(MAX_FILES_KEY);

                if (confValue.length() > 0)
                    FileCountFilter.maxFiles = Integer.parseInt(confValue);
                else
                    FileCountFilter.maxFiles = DEFAULT_MAX_FILES;
            }

            FileCountFilter.fileCount++;

            if (FileCountFilter.fileCount > FileCountFilter.maxFiles)
                return false;

            return true;
        }
    }

    /**
     * Implmentation of Tool.run() method, which builds and runs the Hadoop job.
     *
     * @param args command line parameters, less common Hadoop job parameters stripped out and
     *             interpreted by the Tool class.
     * @return 0 if the Hadoop job completes successfully, 1 if not.
     */
    @Override
    public int run(String[] args) throws Exception {

        String baseInputPath;
        String outputPath;
        String maxFiles = "";

        // Read the command line arguments.
        if (args.length < 1)
            throw new IllegalArgumentException("'run()' must be passed an output path.");

        outputPath = args[0];
        if (args.length > 2)
            maxFiles = args[1];

        // Set the maximum number of metadata files to process.
        this.getConf().set(MAX_FILES_KEY, maxFiles);

        // Put your own AWSAccessKeyId and AWSSecretKey
        this.getConf().set("fs.s3n.awsAccessKeyId", "");
        this.getConf().set("fs.s3n.awsSecretAccessKey", "");
        // Creates a new job configuration for this Hadoop job.
        JobConf job = new JobConf(this.getConf());

        job.setJarByClass(PageRank.class);

        // Set input path directory.
        baseInputPath = "s3n://aws-publicdatasets/common-crawl/parse-output/segment";
        String inputPath = baseInputPath + "/1341690169105/metadata-00112";
        // String inputPath = baseInputPath + "/*/metadata-*";

        LOG.info("adding input path '" + inputPath + "'");
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileInputFormat.setInputPathFilter(job, FileCountFilter.class);

        // Delete the output path directory if it already exists.
        LOG.info("clearing the output path at '" + outputPath + "'");
        FileSystem fs = FileSystem.get(new URI(outputPath), job);
        if (fs.exists(new Path(outputPath)))
            fs.delete(new Path(outputPath), true);

        // Set the path where final output 'part' files will be saved.
        LOG.info("setting output path to '" + outputPath + "'");
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        FileOutputFormat.setCompressOutput(job, false);

        // Set which InputFormat class to use.
        job.setInputFormat(SequenceFileInputFormat.class);

        // Set which OutputFormat class to use.
        job.setOutputFormat(TextOutputFormat.class);

        // Set the output data types.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set which Mapper and Reducer classes to use.
        job.setMapperClass(XMLParser.OutlinksMapper.class);
        job.setReducerClass(XMLParser.OutlinksReducer.class);

        if (JobClient.runJob(job).isSuccessful())
            return 0;
        else
            return 1;
    }

    /**
     * Main entry point that uses the {@link ToolRunner} class to run the example Hadoop job.
     */
    public static void main(String[] args) throws Exception {

        int res = ToolRunner.run(new Configuration(), new PageRank(), args);
        System.exit(res);
    }
}
