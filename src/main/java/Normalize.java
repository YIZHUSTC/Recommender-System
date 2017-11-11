import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Normalize {

    public static class NormalizeMapper extends Mapper<LongWritable, Text, Text, Text> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //input: movieA:movieB \t relation
            //collect the relationship list for movieA
            // output: key = movieA, value = movieB:relation

            String[] moviepair_relation = value.toString().trim().split("\t");
            String movies[] = moviepair_relation[0].split(":");
            context.write(new Text(movies[0]), new Text(movies[1] + ":" + moviepair_relation[1]));
        }
    }

    public static class NormalizeReducer extends Reducer<Text, Text, Text, Text> {
        // reduce method
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //input: key = movieA, value=<movieB:relation, movieC:relation...>
            //normalize each unit of co-occurrence matrix
            //output: key = movieB, value = movieA=relative_relation_to_movieB
            //        key = movieC, value = movieA=relative_relation_to_movieC
            //          ...

            long sum = (long)0;
            List<String> movies = new ArrayList<String>();
            List<Integer> relations = new ArrayList<Integer>();
            for(Text value : values) {
                String[] movie_relation = value.toString().trim().split(":");
                movies.add(movie_relation[0]);
                relations.add(Integer.parseInt(movie_relation[1]));
                sum += Integer.parseInt(movie_relation[1]);
            }

            // matrix multiplication, each row get a different key
            for(int i = 0; i < movies.size(); i++) {
                context.write(new Text(movies.get(i)), new Text(key.toString() + "=" + (double)relations.get(i)/sum));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);

        job.setJarByClass(Normalize.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
