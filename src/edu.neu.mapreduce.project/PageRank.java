package edu.neu.mapreduce.project;

// Java classes
import java.io.IOException;
import java.net.URI;
import java.text.DecimalFormat;
import java.text.NumberFormat;
// Hadoop classes
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
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
public class PageRank extends Configured {

    private static final Logger LOG = Logger.getLogger(PageRank.class);
    private static final String MAX_FILES_KEY = "pagerank.max.files";
    private static NumberFormat nf = new DecimalFormat("00");

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

    public void runMetadataParserParsing(String outputPath, String maxFiles) throws IOException {

        JobConf conf = new JobConf(PageRank.class);

        conf.set("fs.s3n.awsAccessKeyId", "");
        conf.set("fs.s3n.awsSecretAccessKey", "");

        conf.set(MAX_FILES_KEY, maxFiles);

        String baseInputPath = "s3n://aws-publicdatasets/common-crawl/parse-output/segment";
        String inputPath = baseInputPath + "/1341690169105/metadata-00112";
        // inputPath = baseInputPath + "/*/metadata-*";

        FileInputFormat.addInputPath(conf, new Path(inputPath));
        conf.setInputFormat(SequenceFileInputFormat.class);
        FileInputFormat.setInputPathFilter(conf, FileCountFilter.class);
        conf.setMapperClass(MetadataParser.OutlinksMapper.class);

        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        conf.setOutputFormat(TextOutputFormat.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setReducerClass(MetadataParser.OutlinksReducer.class);

        JobClient.runJob(conf);
    }

    private void runRankCalculation(String inputPath, String outputPath) throws IOException {

        JobConf conf = new JobConf(PageRank.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));

        conf.setMapperClass(RankCalculator.RankMapper.class);
        conf.setReducerClass(RankCalculator.RankReducer.class);

        JobClient.runJob(conf);
    }

    private void runRankOrdering(String inputPath, String outputPath) throws IOException {

        JobConf conf = new JobConf(PageRank.class);

        conf.setOutputKeyClass(FloatWritable.class);
        conf.setOutputValueClass(Text.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));

        conf.setMapperClass(RankOrderer.OrderMapper.class);

        JobClient.runJob(conf);
    }

    public static void main(String[] args) throws Exception {

        String outputPath;
        String maxFiles = "";

        if (args.length < 1)
            throw new IllegalArgumentException("'run()' must be passed an output path.");

        outputPath = args[0];
        if (args.length > 2)
            maxFiles = args[1];

        PageRank pagerank = new PageRank();
        pagerank.runMetadataParserParsing("crawl/ranking/iter00", maxFiles);

        int runs = 0;
        for (; runs < 5; runs++) {
            pagerank.runRankCalculation("crawl/ranking/iter" + nf.format(runs), "crawl/ranking/iter" + nf.format(runs + 1));
        }

        pagerank.runRankOrdering("crawl/ranking/iter" + nf.format(runs), outputPath);
    }
}
