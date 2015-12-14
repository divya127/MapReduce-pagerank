package edu.neu.mapreduce.project;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;

public class XMLParser {

    private static final Logger LOG = Logger.getLogger(PageRank.class);

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

    public static class OutlinksReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String pagerank = "1.0\t";

            boolean first = true;
            while(values.hasNext()){
                if(!first) pagerank += ",";

                pagerank += values.next().toString();
                first = false;
            }

            output.collect(key, new Text(pagerank));
        }
    }
}
