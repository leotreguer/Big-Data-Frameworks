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

/** Ici, on ne parvient pas à calculer la fréquence
 * On renvoie le nombre de prénoms féminins, le nombre de prénoms masculins
 * et le nombre total de prénoms. Reste à effectuer la division pour
 * obtenir la fréquence, mais je ne suis pas parvenu à l'
 */
public class mapreducec {

    public static class TokenizerMapperc
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        /** on cree un nouveau mot Total_Key qui est compté à chaque fois
         * que l'on change de ligne
         * Total_key correspond au nombre total de prénoms
         * C'est le dénominateur dans le calcul de la fréquence
         *
         */
        private static final Text TOTAL_KEY = new Text("TOTAL_KEY");

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] result = value.toString().split(";");

            StringTokenizer itr = new StringTokenizer(result[1],",");
            /** on reprend le meme mapper que pour la question 2,
             * sauf que l'on renvoie deux résultats au lieu d'un seul
             * pour chaque occurence de f ou m.
             */

            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);

            }
            context.write(TOTAL_KEY,one);
            /**On renvoie (gender,1) et (Total_Key,1)

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
        inputRecordCounter=counters.findCounter("org.apache.hadoop.mapred.Task$Counter","MAP_INPUT_RECORDS").getvalue();
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


