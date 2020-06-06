package distributeData;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class PartitionerReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	
	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values, 
			Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context arg2) throws IOException, InterruptedException {
		
		Double y = 0.0;
		
		for(DoubleWritable x: values)
		{
			y = y + Double.parseDouble(x.toString());
		}
		
		arg2.write(key, new DoubleWritable(y));
		
	}
	

}
