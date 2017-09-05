/**
 * Created by ice on 2017-07-15.
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class LanguageModel {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        int threshold;
        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            threshold = conf.getInt("threshold", 20);
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String line = value.toString().trim();
            String[] wordsCount = line.split("\t");

            if (wordsCount.length < 2) {
                return;
            }
            String[] words = wordsCount[0].split("\\s++");
            int count = Integer.valueOf(wordsCount[1]);

            if (count < threshold) {
                return;
            }

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < words.length - 1; i++) {
                sb.append(words[i]).append(" ");
            }
            String outputKey = sb.toString().trim();
            String outputValue = words[words.length - 1];

            if (!((outputKey == null) || (outputKey.length() <1))) {
                context.write(new Text(outputKey),
                        new Text(outputValue + "=" + count));
            }
        }
    }

    public static class Reduce extends Reducer <Text, Text, DBOutputWritable, NullWritable> {

        int n;
        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            n = conf.getInt("n", 5);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
            TreeMap<Integer, List<String>> top = new TreeMap<Integer, List<String>> (
                    Collections.reverseOrder()
            );
            for (Text val : values) {
                String curValue = val.toString().trim();
                String word = curValue.split("=")[0].trim();
                int count = Integer.parseInt(curValue.split("=")[1].trim());
                if (top.containsKey(count)) {
                    top.get(count).add(word);
                } else {
                    List<String> list = new ArrayList<String>();
                    list.add(word);
                    top.put(count, list);
                }
            }
            Iterator<Integer> iter = top.keySet().iterator();
            for (int i = 0; iter.hasNext() && i < n; i++) {
                int keyCount = iter.next();
                List<String> words = top.get(keyCount);
                for (String word : words) {
                    context.write(new DBOutputWritable(key.toString(), word, keyCount),
                            NullWritable.get());
                    i++;
                }
            }
        }
    }
}
