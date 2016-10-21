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
            /** on parcourt le CVS ligne par ligne en définissant ";" comme délimiteur
             *
             */
            String[] result = value.toString().split(";");
            /** on lit le contenu de la troisième colonne, celle des origines, en définissant ","
             * comme délimiteur pour les cas où un prénom a plusieurs origines.
             * Chaque token itr correspond à une origine différente
             */
            StringTokenizer itr = new StringTokenizer(result[2],",");
            /** pour chaque prénom de la liste on renvoie (origin,1) autant de fois
             * qu'il y a d'origine
             */
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    /** le reducer est classique ici
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


