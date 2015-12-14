package edu.neu.mapreduce.project;

// Java classes
import java.io.IOException;
import java.net.URI;
// Hadoop classes
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
// Apache Project classes
import org.apache.log4j.Logger;
// GSON classes
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

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

    /**
     * Mapping class that produces statistics about the Common Crawl corpus.
     */
    public static class OutlinksMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> {

        // create a counter group for Mapper-specific statistics
        private final String _counterGroup = "Custom Mapper Counters";

        // implement the main "map" function
        public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

            // key & value are "Text" right now ...
            String url = key.toString();
            String json = value.toString();

            try {
                // See if the page has a successful HTTP code
                JsonParser jsonParser = new JsonParser();
                JsonObject jsonObj = jsonParser.parse(json).getAsJsonObject();

                String disposition = "[no status]";

                if (jsonObj.has("disposition")) {
                    disposition = jsonObj.get("disposition").getAsString().trim().toUpperCase();
                    if (disposition.equals("SUCCESS") && jsonObj.has("content")) {
                        JsonObject content = (JsonObject) jsonObj.get("content");
                        if (content.has("type")) {
                            if ("html-doc".equalsIgnoreCase(content.get("type").getAsString().trim()
                                    .toLowerCase())) {
                                int counter = 0;
                                StringBuilder outLinks = new StringBuilder();
                                if (content.has("links")) {
                                    JsonArray allLinks = content.getAsJsonArray("links");
                                    for (int i = 0; i < allLinks.size(); i++) {
                                        JsonObject obj = allLinks.get(i).getAsJsonObject();
                                        if (obj.has("href")) {
                                            String href = obj.get("href").getAsString().trim();
                                            outLinks.append(href).append(",");
                                            counter++;
                                        }
                                    }
                                }
                                output.collect(new Text(url), new Text(outLinks.toString()));
                                // output.collect(new Text(url), new Text(String.valueOf(counter)));
                            }
                        }
                    }
                }
            }

            catch (Exception ex) {
                LOG.error("Caught Exception", ex);
                reporter.incrCounter(this._counterGroup, "Exceptions", 1);
            }
        }
    }

    public static class FileCountFilter extends Configured implements PathFilter {

        private static final int DEFAULT_MAX_FILES = 9999999;

        private static int fileCount = 0;
        private static int maxFiles = 0;

        /*
         * Called once per file to be processed.  Returns true until max files
         * has been reached.
         */
        public boolean accept(Path path) {

            // If max files hasn't been set then set it to the
            // configured value.
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

        // Put your own AWSAccessKeyId and AWSSecretKey
        this.getConf().set("fs.s3n.awsAccessKeyId", "");
        this.getConf().set("fs.s3n.awsSecretAccessKey", "");
        // Creates a new job configuration for this Hadoop job.
        JobConf job = new JobConf(this.getConf());

        job.setJarByClass(PageRank.class);

        // Add input path directory.
        baseInputPath = "s3n://aws-publicdatasets/common-crawl/parse-output/segment";
        String inputPath = baseInputPath + "/1341690169105/metadata-00112";
        // String inputPath = baseInputPath + "/*/metadata-*";

        LOG.info("adding input path '" + inputPath + "'");
        FileInputFormat.addInputPath(job, new Path(inputPath));

        // Set the maximum number of metadata files to process.
        this.getConf().set(MAX_FILES_KEY, maxFiles);
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
        job.setMapperClass(OutlinksMapper.class);
        // job.setCombinerClass(LongSumReducer.class);
        // job.setReducerClass(LongSumReducer.class);

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
