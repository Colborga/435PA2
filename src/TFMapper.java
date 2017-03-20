import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class TFMapper extends Mapper<LongWritable, Text, Text, Text>{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		//String[] Line = value.toString().split("\n");
		String[] lineSplit = value.toString().split("\\s+");
		String Author = lineSplit[0];
		String term = lineSplit[1];
		String count = lineSplit[2];
		
		context.write(new Text(Author), new Text(term + "," +  count));
	}
}
