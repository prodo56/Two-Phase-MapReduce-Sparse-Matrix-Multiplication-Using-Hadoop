// Two phase matrix multiplication in Hadoop MapReduce
//package matrixMultiplicationHadoop;

import java.io.IOException;

// add your import statement here if needed

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TwoPhase {

	// mapper for processing entries of matrix A
	public static class PhaseOneMapperA extends
			Mapper<LongWritable, Text, Text, Text> {

		private Text outKey = new Text();
		private Text outVal = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] valueContents = value.toString().split(",");
			System.out.println(valueContents);
			try {
				outKey.set(valueContents[1]);
				outVal.set("A," + valueContents[0] + "," + valueContents[2]);
				context.write(outKey, outVal);
			} catch (ArrayIndexOutOfBoundsException e) {
				System.out.println("error in Mapper of A: "
						+ e.getLocalizedMessage());
			}
		}

	}

	// mapper for processing entries of matrix B
	public static class PhaseOneMapperB extends
			Mapper<LongWritable, Text, Text, Text> {

		private Text outKey = new Text();
		private Text outVal = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] valueContents = value.toString().split(",");
			try {
				outKey.set(valueContents[0]);
				outVal.set("B," + valueContents[1] + "," + valueContents[2]);
				context.write(outKey, outVal);
			} catch (ArrayIndexOutOfBoundsException e) {
				System.out.println("error in Mapper of B: "
						+ e.getLocalizedMessage());
			}

		}
	}

	public static class PhaseOneReducer extends Reducer<Text, Text, Text, Text> {

		private Text outKey = new Text();
		private Text outVal = new Text();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			List<String> listA = new ArrayList<String>();
			List<String> listB = new ArrayList<String>();
			Iterator<Text> it = values.iterator();
            while (it.hasNext()) {
            	Text value = it.next();
				String[] stringValue = value.toString().split(",",2);
				if (stringValue[0].equalsIgnoreCase("A")) {
					listA.add(stringValue[1]);
				} else {
					listB.add(stringValue[1]);
				}
			}
			//System.out.println("list A: "+listA.toString());
			//System.out.println("list B: "+listB.toString());
			for (int i=0;i<listA.size();i++) {
				String[] valueA = listA.get(i).split(",");
				for (int j = 0; j < listB.size(); j++) {
					String[] valueB = listB.get(j).split(",");
					outKey.set(valueA[0] + "," +valueB[0]);
					outVal.set(String.valueOf(Integer.valueOf(valueA[1])
							* Integer.valueOf(valueB[1])));
					context.write(outKey, outVal);
				}
			}
		}

	}

	public static class PhaseTwoMapper extends Mapper<Text, Text, Text, Text> {

		private Text outKey = new Text();
		private Text outVal = new Text();

		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			outKey.set(key);
			outVal.set(value);
			context.write(outKey, outVal);
		}
	}

	public static class PhaseTwoReducer extends Reducer<Text, Text, Text, Text> {

		private Text outKey = new Text();
		private Text outVal = new Text();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (Text value : values) {
				sum += Integer.valueOf(value.toString());
			}
			outKey.set(key);
			outVal.set(String.valueOf(sum));
			context.write(outKey, outVal);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job jobOne = Job.getInstance(conf, "phase one");

		jobOne.setJarByClass(TwoPhase.class);

		jobOne.setOutputKeyClass(Text.class);
		jobOne.setOutputValueClass(Text.class);

		jobOne.setReducerClass(PhaseOneReducer.class);

		MultipleInputs.addInputPath(jobOne, new Path(args[0]),
				TextInputFormat.class, PhaseOneMapperA.class);

		MultipleInputs.addInputPath(jobOne, new Path(args[1]),
				TextInputFormat.class, PhaseOneMapperB.class);

		Path tempDir = new Path("temp");

		FileOutputFormat.setOutputPath(jobOne, tempDir);
		jobOne.waitForCompletion(true);

		// job two
		Job jobTwo = Job.getInstance(conf, "phase two");

		jobTwo.setJarByClass(TwoPhase.class);

		jobTwo.setOutputKeyClass(Text.class);
		jobTwo.setOutputValueClass(Text.class);

		jobTwo.setMapperClass(PhaseTwoMapper.class);
		jobTwo.setReducerClass(PhaseTwoReducer.class);

		jobTwo.setInputFormatClass(KeyValueTextInputFormat.class);

		FileInputFormat.setInputPaths(jobTwo, tempDir);
		FileOutputFormat.setOutputPath(jobTwo, new Path(args[2]));

		jobTwo.waitForCompletion(true);

		FileSystem.get(conf).delete(tempDir, true);

	}
}
