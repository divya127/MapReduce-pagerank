package cs6240;

import edu.cmu.lemurproject.WritableWarcRecord;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class PageRankMapper extends MapReduceBase implements MapRunnable<LongWritable, WritableWarcRecord, Text, LinkArrayWritable> {

        @Override
        public void run(RecordReader<LongWritable, WritableWarcRecord> recordReader, OutputCollector<Text, LinkArrayWritable> outputCollector, Reporter reporter) throws IOException {

        }
}
