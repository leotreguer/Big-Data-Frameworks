package ltreguer.hadoop;

/**
 * Created by Léo on 14/10/2016.
 */

import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static java.lang.Math.toIntExact;

/** Here, we fail to compute the frequency of the gender of the prenom
 * We manage the number of female prenoms, male prenoms, and the total
 * number of prenoms. We need to compute the division to get the frequency
 * but I did not manage to do it through a MAP/reduce program.
 * I think we could use the TaskCount function from Hadoop, which is given
 * once the program runs in Hadoop
 * "Map-Reduce Framework
 Map input records=11526 -> Same as number of rows
"
 */
public class mapreducec {

    public static class TokenizerMapperc
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        /** We create a new word "total_key", which is counted
         * every time we switch row.
         * * Total_key is the total number of prenoms
         * It is the denominator in the computation of the frequency
         *
         */
        private static final Text TOTAL_KEY = new Text("TOTAL_KEY");

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] result = value.toString().split(";");

            StringTokenizer itr = new StringTokenizer(result[1],",");
            /** We take the same mapper as mapper b, but we return two results
             * instead of one for every occurence of f or m            */

            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);

            }
            context.write(TOTAL_KEY,one);
            /**We return (gender,1) and (Total_Key,1)

             *
             */
        }
    }

    public static int safeLongToInt(long l) {
        if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
            throw new IllegalArgumentException
                    (l + " cannot be cast to int without changing its value.");
        }
        return (int) l;
    }

    public static class SumReducerc
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
                        int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        int inputRecordCounter;
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separatorText", ",");
        Job job = Job.getInstance(conf, "namebynumberoforigins");
        job.setJarByClass(mapreducec.class);
        job.setMapperClass(TokenizerMapperc.class);
        job.setCombinerClass(SumReducerc.class);
        job.setReducerClass(SumReducerc.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


