package cs6240;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class PageRankReducer extends MapReduceBase implements Reducer<Text, LinkArrayWritable, Text, LinkArrayWritable> {

    @Override
    public void reduce(Text text, Iterator<LinkArrayWritable> iterator, OutputCollector<Text, LinkArrayWritable> outputCollector, Reporter reporter) throws IOException {

    }
}
