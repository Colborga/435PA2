/**
 * Created by colborga on 3/19/17.
 */
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OnlineCos2mapper extends Mapper<LongWritable, Text, Text, Text>{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split(":");

        Text authXYX = new Text(split[0]);
        Text values = new Text(split[1]);

        context.write(authXYX , values);

    }

}

