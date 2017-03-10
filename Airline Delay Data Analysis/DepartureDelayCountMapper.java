package Airline;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class DepartureDelayCountMapper extends 
	Mapper<LongWritable, Text, Text, IntWritable> {
	//맵 출력 값  Output Value of Map
	private final static IntWritable outputValue = new IntWritable(1);
	
	//맵 출력 키  Output Value of Key
	private Text outputKey = new Text();
	
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
			AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
			
		//출력키 설정  Output key Settings
		outputKey.set(parser.getYear() + "," + parser.getMonth());
		
		//출력 데이터 생성  Output Data Creation
		if(parser.getDepartureDelayTime() > 0){
			context.write(outputKey, outputValue);
		}
	}
}
