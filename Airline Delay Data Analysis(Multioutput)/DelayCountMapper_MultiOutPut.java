package Airline;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class DelayCountMapper_MultiOutPut extends 
	Mapper<LongWritable, Text, Text, IntWritable> {
	
	//�� ��� ��
	private final static IntWritable outputValue = new IntWritable(1);
	
	//�� ��� Ű
	private Text outputKey = new Text();
	
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
	
		AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
			
		//��� ���� ������ ���
		if(parser.isDepartureDelayAvailable()){
			if(parser.getDepartureDelayTime()>0){
				//��� Ű ����
				outputKey.set("D, "+parser.getYear() + "," + parser.getMonth());
				
				//��� ������ ����
				context.write(outputKey, outputValue);
			}else if (parser.getDepartureDelayTime()==0){
				context.getCounter(DelayCounters.scheduled_departure).increment(1);
			}else if (parser.getDepartureDelayTime()<0) {
				context.getCounter(DelayCounters.early_departure).increment(1);
			}
		}else {
			context.getCounter(DelayCounters.not_available_departure).increment(1);
		}
	
		//���� ���� ������ ���
		if(parser.isArriveDelayAvailable()){
			if(parser.getArriveDelayTime()>0){
				//��� Ű ����
				outputKey.set("A, "+parser.getYear() + "," + parser.getMonth());
				
				// ��� ������ ����
				context.write(outputKey, outputValue);
			} else if (parser.getArriveDelayTime() == 0) {
				context.getCounter(DelayCounters.scheduled_arrival).increment(1);
			} else if (parser.getArriveDelayTime() < 0) {
				context.getCounter(DelayCounters.early_arrival).increment(1);
			}
		} else {
			context.getCounter(DelayCounters.not_available_arrival).increment(1);
		}
	}
}
