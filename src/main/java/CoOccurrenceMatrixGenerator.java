import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CoOccurrenceMatrixGenerator {
	public static class MatrixGeneratorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//input: value = userid \t movie1:rating, movie2:rating...
			//output: key = movie1: movie2 value = 1
			//calculate each user rating list: <movieA, movieB>

			//[movie1:rating, movie2:rating..]
			String[] movie_rating_set = value.toString().trim().split("\t")[1].split(",");

			// [movie1, movie2 ...]
			List<String> movies = new ArrayList<String>();
			for(String movie_rating : movie_rating_set) {
				movies.add(movie_rating.split(":")[0]);
			}

			for(int i = 0; i < movies.size(); i++) {
				for (int j = 0; j < movies.size(); j++) {
					context.write(new Text(movies.get(i) + ":" + movies.get(j)), new IntWritable(1));
				}
			}

		}
	}

	public static class MatrixGeneratorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			//key movie1:movie2 value = iterable<1, 1, 1>
			//calculate each two movies have been watched by how many people

			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		job.setMapperClass(MatrixGeneratorMapper.class);
		job.setReducerClass(MatrixGeneratorReducer.class);
		
		job.setJarByClass(CoOccurrenceMatrixGenerator.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
	}
}
