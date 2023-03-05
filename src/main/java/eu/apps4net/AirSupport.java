package eu.apps4net;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AirSupport {

    /**
     * This method uses a regular expression to split each line to a list of strings,
     * each one representing one column
     */
    private static String[] processLine(String line) {
        // Create a regular expression for proper split of each line

        // The regex for characters other than quote (")
        String otherThanQuote = " [^\"] ";

        // The regex for a quoted string. e.g "whatever1 whatever2"
        String quotedString = String.format(" \" %s* \" ", otherThanQuote);

        // The regex to split the line using comma (,) but taking into consideration the quoted strings
        // This means that is a comma is in a quoted string, it should be ignored.
        String regex = String.format("(?x) " + // enable comments, ignore white spaces
                        ",                         " + // match a comma
                        "(?=                       " + // start positive look ahead
                        "  (?:                     " + //   start non-capturing group 1
                        "    %s*                   " + //     match 'otherThanQuote' zero or more times
                        "    %s                    " + //     match 'quotedString'
                        "  )*                      " + //   end group 1 and repeat it zero or more times
                        "  %s*                     " + //   match 'otherThanQuote'
                        "  $                       " + // match the end of the string
                        ")                         ", // stop positive look ahead
                otherThanQuote, quotedString, otherThanQuote);
        String[] tokens = line.split(regex, -1);

        // check for the proper number of columns
        if (tokens.length == 10) {
            return tokens;
        } else {
            System.err.println("Wrong number of columns for line: " + line);
            return null;
        }
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, LongWritable> {

        private final static LongWritable tweetId = new LongWritable();
        private final Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Tweet tweet = null;

            String line = value.toString();
            String tweetText = "";

            // Σπάει την γραμμή σε στοιχεία
            String[] tweetArray = processLine(line);

            if(tweetArray != null) {
                // Δημιουργία αντικειμένου Tweet
                tweet = new Tweet(tweetArray);

                // Παίρνει καθαρό κείμενο από το Tweet
                tweetText = tweet.getClearedText();
            }

            StringTokenizer itr = new StringTokenizer(tweetText);
            while (itr.hasMoreTokens()) {
                // Reads each word and removes (strips) the white space
                String token = itr.nextToken().strip();

//                System.out.println(token);
                word.set(String.valueOf(token));

                try {
                    tweetId.set((long) Double.parseDouble(tweet.getTweetId()));

                    context.write(word, tweetId);
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            }
        }
    }

    public static class TweetsReducer extends Reducer<Text, LongWritable, Text, Text> {
        private final Text result = new Text();

        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            StringBuilder text = new StringBuilder();

            for (LongWritable val : values) {
                text.append(String.valueOf(val)).append(" ");
            }

            result.set(String.valueOf(text));

            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Airline tweets");
        job.setJarByClass(AirSupport.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(TweetsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    /**
     * Βοηθητική κλάση για την καταχώρηση του Tweet
     */
    public static class Tweet {
        private final String tweetId;
        private final String airlineSentiment;
        private final String airlineSentimentConfidence;
        private final String negativeReason;
        private final String negativeReasonConfidence;
        private final String airline;
        private final String name;
        private final String text;
        private final String tweetCreated;
        private final String userTimezone;

        public Tweet(String[] tweetArray) {
            this.tweetId = tweetArray[0];
            this.airlineSentiment = tweetArray[1];
            this.airlineSentimentConfidence = tweetArray[2];
            this.negativeReason = tweetArray[3];
            this.negativeReasonConfidence = tweetArray[4];
            this.airline = tweetArray[5];
            this.name = tweetArray[6];
            this.text = tweetArray[7];
            this.tweetCreated = tweetArray[8];
            this.userTimezone = tweetArray[9];
        }

        public String getTweetId() {
            return tweetId;
        }

        public String getAirlineSentiment() {
            return airlineSentiment;
        }

        public String getAirlineSentimentConfidence() {
            return airlineSentimentConfidence;
        }

        public String getNegativeReason() {
            return negativeReason;
        }

        public String getNegativeReasonConfidence() {
            return negativeReasonConfidence;
        }

        public String getAirline() {
            return airline;
        }

        public String getName() {
            return name;
        }

        public String getText() {
            return text;
        }

        /**
         * Επιστρέφει καθαρισμένο το κείμενο, αφήνοντας μόνο λέξεις, mentions και hashtags
         *
         * @return String
         */
        public String getClearedText() {
            return text.replaceAll("^[0-9]+", "")
                    .replaceAll("http\\S+", "")
                    .replaceAll("[^\\p{L}\\p{Nd}\\s@#]", "")
                    .replaceAll("\\p{C}", "")
                    .replaceAll("\\s+", " ")
                    .toLowerCase();
        }

        public String getTweetCreated() {
            return tweetCreated;
        }

        public String getUserTimezone() {
            return userTimezone;
        }

        @Override
        public String toString() {
            return "Tweet{" +
                    "tweetId='" + tweetId + '\'' +
                    ", airlineSentiment='" + airlineSentiment + '\'' +
                    ", airlineSentimentConfidence='" + airlineSentimentConfidence + '\'' +
                    ", negativeReason='" + negativeReason + '\'' +
                    ", negativeReasonConfidence='" + negativeReasonConfidence + '\'' +
                    ", airline='" + airline + '\'' +
                    ", name='" + name + '\'' +
                    ", text='" + text + '\'' +
                    ", tweetCreated='" + tweetCreated + '\'' +
                    ", userTimezone='" + userTimezone + '\'' +
                    '}';
        }
    }
}
