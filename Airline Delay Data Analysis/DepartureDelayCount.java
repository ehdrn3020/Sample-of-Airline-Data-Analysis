package Airline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DepartureDelayCount {
	public static void main(String[] args) throws Exception{
		//입출력 데이터 경로 확인
		Configuration conf = new Configuration();
		if(args.length!=2){
			System.out.println("Usage:WordCount<input><output>");
			System.exit(2);
		}
		//잡 이름 설정
		Job job = new Job(conf, "DepartureDelayCount");
		
		//입출력 데이터 경로 설정
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//잡 클래스 설정
		job.setJarByClass(DepartureDelayCount.class);
		//매퍼 클래스 설정
		job.setMapperClass(DepartureDelayCountMapper.class);
		//리듀서 클래스 설정
		job.setReducerClass(DelayCountReducer.class);
		
		//입출력 데이터 포맷 설정
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//출력키 및 출력 값 유형 설정
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.waitForCompletion(true);
		
	}
}
