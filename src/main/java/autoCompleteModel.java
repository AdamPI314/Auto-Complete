import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class autoCompleteModel {
    public static class AcmMapper extends Mapper<LongWritable, Text, Text, Text> {
        int threshold;
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration configuration = context.getConfiguration();
            this.threshold = configuration.getInt("threshold", 10);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            super.map(key, value, context);
            // cool\t20
            String line = value.toString().trim();
            String[] phrases_count = line.split("\t");
            if (phrases_count.length != 2)
                return;

            String[] words = phrases_count[0].split("\\s+");
            int count = Integer.valueOf(phrases_count[1]);

            if (count < this.threshold)
                return;

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < words.length - 1; ++i) {
                sb.append(words[i]);
                if (i != words.length - 1)
                     sb.append(" ");
            }

            String outputKey = sb.toString().trim();
            String outputValue = words[words.length - 1];

            if (outputKey != null && outputKey.length() >= 1) {
                context.write(new Text(outputKey), new Text(outputValue + "=" + count));
            }

        }
    }

    public static class AcmReducer extends Reducer<Text, Text, DBOutputWritable, NullWritable> {
        int n;
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration configuration = context.getConfiguration();
            this.n = configuration.getInt("n", 5);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            super.reduce(key, values, context);
            TreeMap<Integer, List<String>> tm = new TreeMap<Integer, List<String>>(Collections.<Integer>reverseOrder());
            for (Text text : values) {
                String phrase_value = text.toString().trim();
                String[] phrase_count = phrase_value.split("=");
                String phrase = phrase_count[0].trim();
                int count = Integer.parseInt(phrase_count[1].trim());
                if (tm.containsKey(count))
                    tm.get(count).add(phrase);
                else
                    tm.put(count, new ArrayList<String>(Arrays.asList(phrase)));
            }

            Iterator<Integer> iterator = tm.keySet().iterator();
            for (int j = 0; iterator.hasNext() && j <n ;) {
               int keyCount = iterator.next();
               List<String> words = tm.get(keyCount);
               for (String word : words) {
                   context.write(new DBOutputWritable(key.toString(), word, keyCount), NullWritable.get());
                   ++j;

                   if (j >= n)
                       break;
               }
            }
        }

    }
}
