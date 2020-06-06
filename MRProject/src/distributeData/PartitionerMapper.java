package distributeData;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;    
import org.apache.hadoop.io.LongWritable;    
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;  


public class PartitionerMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
	throws IOException, InterruptedException{
		
		String inputString = value.toString().trim();
		String[] inputArr = inputString.split(",");
		
		String country = inputArr[0];
		Double countrySal = Double.parseDouble(inputArr[2]);
		
		context.write(new Text(country), new DoubleWritable(countrySal));
		
		
	}

}
