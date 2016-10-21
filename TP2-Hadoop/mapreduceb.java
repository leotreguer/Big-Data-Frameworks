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
        import org.apache.hadoop.io.Text;
        import org.apache.hadoop.mapreduce.Job;
        import org.apache.hadoop.mapreduce.Mapper;
        import org.apache.hadoop.mapreduce.Reducer;
        import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
        import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class mapreduceb {

    public static class TokenizerMapperb
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] result = value.toString().split(";");
            /**Here we count the number of tokens separated by comma ","
             * in the third colum to count the number of origins of a prenom
             */
            StringTokenizer itr = new StringTokenizer(result[2],",");
            System.out.println(itr.countTokens());
            /** For every prenom, we return (number of origins,1)
             * Here the while loop is not necessary like in the mapper a
             * since we care only about the number of origins, and not
             * about the origins themselves
             */
            word.set(String.valueOf(itr.countTokens())+"origins");
            context.write(word, one);

        }
    }

    /** We reuse the same reducer as in mapreducea
     *
     */

    public static class SumReducerb
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
        Job job = Job.getInstance(conf, "namebynumberoforigins");
        job.setJarByClass(mapreduceb.class);
        job.setMapperClass(TokenizerMapperb.class);
        job.setCombinerClass(SumReducerb.class);
        job.setReducerClass(SumReducerb.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


