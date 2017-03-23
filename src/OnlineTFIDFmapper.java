/**
 * Created by colborga on 3/19/17.
 */
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OnlineTFIDFmapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split("\\s+");

        if (split.length == 4) {
            String Term = split[1];
            Double IDF = new Double(split[3]);
            context.write(new Text(Term), new DoubleWritable(IDF));

        }
        else{
            //String Author = split[0];
            String Term = split[1];
            Double TF = new Double(split[2]);

            context.write(new Text(Term), new DoubleWritable(TF));

        }

    }

}

