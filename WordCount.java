import java.io.IOException;
import java.util.ArrayList;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, MyMapWritable> {

		private final static IntWritable one = new IntWritable(1);
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			MyMapWritable mw = new MyMapWritable();
			
			ArrayList<String> productNameList = new ArrayList<>();
			String[] tokens = value.toString().split(", ");
			for(int i=0; i<tokens.length; i++) {
				mw.put(new Text(tokens[i]), one);
				productNameList.add(tokens[i]);
			}
			for(int i=0; i<tokens.length; i++) {
				Text key_productName = new Text();
				key_productName.set(tokens[i]);
				mw.remove(key_productName);
				context.write(key_productName, mw);
				mw.put(key_productName, one);
			}
		}
	}
	
	public static class IntSumReducer extends Reducer<Text, MyMapWritable, Text, MyMapWritable> {
	//public static class IntSumReducer extends Reducer<Text, MyMapWritable, Text, Text> {
		public void reduce(Text key, Iterable<MyMapWritable> values, Context context)
				throws IOException, InterruptedException {
			MyMapWritable mw = new MyMapWritable();
			for(MyMapWritable value : values) {
				for(Entry<Writable, Writable> entry : value.entrySet()) {
					
					Text entryKey = new Text();
					entryKey.set(entry.getKey().toString());
					IntWritable entryValue = new IntWritable(Integer.parseInt(entry.getValue().toString()));
					if(mw.containsKey(entryKey)){
						int count = Integer.parseInt(mw.get(entryKey).toString());
						count = count + Integer.parseInt(entryValue.toString());
						mw.put(entryKey, new IntWritable(count));
					} else {
						mw.put(entryKey, new IntWritable(1));
					}
				}
			}
			context.write(key, mw);
		}
	}

	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		//Attention: If reduce output class types are not same as input class, we have to declare map output class type (two lines below). 
		//job.setMapOutputKeyClass(Text.class);
		//job.setMapOutputValueClass(MyMapWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MyMapWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
