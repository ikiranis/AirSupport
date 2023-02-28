package eu.apps4net;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AirSupport {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, FloatWritable> {

        private final static FloatWritable wordLength = new FloatWritable();
        private final Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            // Αφαίρεση σημείων στίξης και μετατροπή σε lower case
            line = line.replaceAll("\\p{Punct}", " ").toLowerCase();

            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                // Reads each word and removes (strips) the white space
                String token = itr.nextToken().strip();

                // Αν δεν αρχίζει από αριθμό
                if(!token.matches("^\\d.*")) {
                    word.set(String.valueOf(token.charAt(0)));

                    wordLength.set(token.length());

                    context.write(word, wordLength);
                }

            }
        }
    }

    public static class AvgReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        private final FloatWritable result = new FloatWritable();

        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;

//            System.out.print(key + " ");

            for (FloatWritable val : values) {
                sum += val.get();
                count++;
            }

            float average = (float) sum/count;
//            System.out.println("Sum: " + sum + " count: " + count + " average: "  + average);

            // Format the average value as a string with two decimal places
            String formattedAverage = String.format("%.1f", average);

            // Parse the formatted string back into a float
            result.set(Float.parseFloat(formattedAverage));

            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average words length");
        job.setJarByClass(AirSupport.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(AvgReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
