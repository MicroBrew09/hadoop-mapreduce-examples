// adapted from https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WikipediaPopular extends Configured implements Tool {

	public static class WikiMapper
	extends Mapper<LongWritable, Text, Text, LongWritable>{

		private final static LongWritable count = new LongWritable();
		private Text datetime = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {			
			String line = value.toString(); 

			//String valuelower = line.toLowerCase();
                        //Pattern word_sep = Pattern.compile("\t");
			String[] result = line.split(" ");
			if(result[1].equals("en")==true && result[2].equals("Main_Page")==false && result[2].startsWith("Special:")==false) {  
     				datetime.set(result[0]);
				count.set(Long.valueOf(result[3]));
			 	context.write(datetime, count);
				}

				
			}

	}

	public static class WikiCombiner
	extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable maxvalue1 = new LongWritable();
			Text maxKey1 = new Text();
		@Override
		public void reduce(Text key, Iterable<LongWritable> values,
				Context context
				) throws IOException, InterruptedException {
			long max1 = 0;
			for (LongWritable val1 : values) {
				if(val1.get() > max1){					
				max1 = val1.get();
				maxKey1 = key;
				}				
			}
			maxvalue1.set(max1);
			context.write(maxKey1, maxvalue1);
	
		}
	}


	public static class WikiReducer
	extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable maxvalue = new LongWritable();
			Text maxKey = new Text();
		@Override
		public void reduce(Text key, Iterable<LongWritable> values,
				Context context
				) throws IOException, InterruptedException {
				long max =0;
				for (LongWritable val : values) {
				if(val.get() > max){					
				max = val.get();
				maxKey = key;
				}				
			}
			maxvalue.set(max);
			context.write(maxKey, maxvalue);
				

		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "wikipopular count");
		job.setJarByClass(WikipediaPopular.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(WikiMapper.class);
		job.setCombinerClass(WikiCombiner.class);
		job.setReducerClass(WikiReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
