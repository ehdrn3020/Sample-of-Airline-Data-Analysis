package Airline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class DelayCount_MultiOutput extends Configured implements Tool {
	public static void main(String[] args) throws Exception{
		//Tool �������̽� ����
		int res = ToolRunner.run(new Configuration(), new DelayCount_MultiOutput(),args);
		System.out.println("MR-Job Result : "+res);
	}
	public int run(String[] args) throws Exception {
		String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
		// ����� ������ ��� Ȯ��
		if (otherArgs.length != 2) {
			System.err.println("Usage:DelayCount MultiOutput <input><output>");
			System.exit(2);
		}
		// �� �̸� ����
		Job job = new Job(getConf(), "DelayCount MultipleOutput");

		// ����� ������ ��� ����
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// �� Ŭ���� ����
		job.setJarByClass(DelayCount_MultiOutput.class);
		// ���� Ŭ���� ����
		job.setMapperClass(DelayCountMapper_MultiOutPut.class);
		// ���༭ Ŭ���� ����
		job.setReducerClass(DelayCountReducer_MultiOutput.class);

		// ����� ������ ���� ����
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// ���Ű �� ��� �� ���� ����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		//Multipleoutput ����
		MultipleOutputs.addNamedOutput(
				job, "departure", TextOutputFormat.class, Text.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(
				job, "arrival", TextOutputFormat.class, Text.class, IntWritable.class);
		
		job.waitForCompletion(true);
		return 0;
	}
}
