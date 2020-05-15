// adapted from https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.json.JSONObject;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RedditAverage extends Configured implements Tool {

	public static class RedditMapper
	extends Mapper<LongWritable, Text, Text, LongPairWritable>{

		//private final static Long one = new longValue();
		//private final static Text word = new Text();
			

		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {			
			
			String keyvalue = value.toString();
			JSONObject record = new JSONObject(keyvalue);
			Text word = new Text(record.getString("subreddit"));
			long Score = record.getLong("score");			
			long one = 1;
 			LongPairWritable MapperOutput = new LongPairWritable(one, Score);
			context.write(word, MapperOutput);
								

		}
	}
	public static class RedditCombiner
	extends Reducer<Text, LongPairWritable, Text, LongPairWritable> {
		//private DoubleWritable result = new DoubleWritable();

		@Override
		public void reduce(Text key, Iterable<LongPairWritable> values,
				Context context
				) throws IOException, InterruptedException {
			int ScoreSum = 0;
			int CountSum = 0;
			double average = 0;

			for (LongPairWritable val : values) {
				ScoreSum += val.get_1();
				CountSum += val.get_0();
			}
			LongPairWritable CombinerOutput = new LongPairWritable(CountSum, ScoreSum);
			context.write(key, CombinerOutput);
		}
	}
	public static class RedditReducer
	extends Reducer<Text, LongPairWritable, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();

		@Override
		public void reduce(Text key, Iterable<LongPairWritable> values,
				Context context
				) throws IOException, InterruptedException {
			double Reducerscoresum = 0;
			double Reducercountsum = 0;
			//double average = 0;

			for (LongPairWritable val1: values) {
				Reducercountsum += val1.get_0();
				Reducerscoresum += val1.get_1();
			}
			double average =Reducerscoresum/Reducercountsum;
			result.set(average);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "Reddit Average");
		job.setJarByClass(RedditAverage.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(RedditMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongPairWritable.class);
		
		job.setCombinerClass(RedditCombiner.class);

		job.setReducerClass(RedditReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
