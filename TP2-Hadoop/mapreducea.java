/**added comments
*/

package ltreguer.hadoop;
/**
 * Created by Léo on 13/10/2016.
 */
import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class mapreducea {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            /** The CSV file is read row by row, with ";" being the delimiter in a row             *
             */
            String[] result = value.toString().split(";");
            /** We read the content of the third column (the one with the prenoms's origins)
             * We define "," as delimiter to assess whether a prenom has multiple origins.
             * Every token itr corresponds to a different origin
             */
            StringTokenizer itr = new StringTokenizer(result[2],",");
            /** for every prenom in the liste, we return (origin,1)
             * for every origin there is.
             */
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    /** The reducer function is classical here.
     *
     */
    public static class SumReducera
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
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separatorText", ",");
        Job job = Job.getInstance(conf, "namebyorigin");
        job.setJarByClass(mapreducea.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(SumReducera.class);
        job.setReducerClass(SumReducera.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


