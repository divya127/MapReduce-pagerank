package edu.neu.mapreduce.project;

// Java classes
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
// Hadoop classes
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

/**
 * Use of Common Crawl 'metadata' files to quickly gather high level information about the corpus'
 * content such as href which resides in links.
 *
 * @author Preety Mishra
 * @author Divya Devaraj
 * @author Christoforus Benvenuto
 */
public class PageRank {

    private static final String AWS_ACCESS_KEY_ID = "";
    private static final String AWS_SECRET_ACCESS_KEY = "";
    private static final String AWS_S3_BUCKET_NAME = "";

    public void runMetadataParsing(String outputPath, int maxFiles) throws IOException, URISyntaxException {

        JobConf conf = new JobConf(PageRank.class);

        conf.set("fs.s3n.awsAccessKeyId", AWS_ACCESS_KEY_ID);
        conf.set("fs.s3n.awsSecretAccessKey", AWS_SECRET_ACCESS_KEY);

        String baseInputPath = "s3n://aws-publicdatasets/common-crawl/parse-output/segment";
        // String inputPath = baseInputPath + "/1341690169105/metadata-00112";
        // String inputPath = baseInputPath + "/*/metadata-*";
        // FileInputFormat.addInputPath(conf, new Path(inputPath));

        String inputPath;
        NumberFormat numberFormat = new DecimalFormat("00000");
        for (int i = 0; i < maxFiles; i++) {
            inputPath = baseInputPath + "/1346823845675/metadata-" + numberFormat.format(i);
            FileInputFormat.addInputPath(conf, new Path(inputPath));
        }

        conf.setInputFormat(SequenceFileInputFormat.class);
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
        String maxFiles = "0";

        if (args.length < 1)
            throw new IllegalArgumentException("'run()' must be passed an output path.");

        outputPath = args[0];
        if (args.length > 1)
            maxFiles = args[1];

        PageRank pagerank = new PageRank();
        pagerank.runMetadataParsing("s3://" + AWS_S3_BUCKET_NAME + "/crawl/ranking/iter00", Integer.parseInt(maxFiles));

        NumberFormat nf = new DecimalFormat("00");
        int runs = 0;
        for (; runs < 5; runs++) {
            pagerank.runRankCalculation("s3://" + AWS_S3_BUCKET_NAME + "/crawl/ranking/iter" + nf.format(runs), "s3://" + AWS_S3_BUCKET_NAME + "/crawl/ranking/iter" + nf.format(runs + 1));
        }

        pagerank.runRankOrdering("s3://" + AWS_S3_BUCKET_NAME + "/crawl/ranking/iter" + nf.format(runs), outputPath);
    }
}
