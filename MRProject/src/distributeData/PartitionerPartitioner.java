package distributeData;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;  

public class PartitionerPartitioner extends Partitioner<Text, DoubleWritable>{
	
	@Override
	public int getPartition(Text key, DoubleWritable value, int arg2) {
		
		String inputstr = key.toString().toLowerCase().trim();
		if(inputstr.equals("France"))
		{
			return 0;
		}
		
		if(inputstr.equals("Spain"))
		{
			return 1;
		}
		
		if(inputstr.equals("Germany"))
		{
			return 2;
		}
		
		else
		{
			return 2;
		}
		
	}

	
	
}
