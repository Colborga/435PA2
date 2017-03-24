/**
 * Created by colborga on 3/19/17.
 */
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OnlineTFIDFmapper extends Mapper<LongWritable, Text, Text, Text>{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split("\\s+");

        Text term = new Text();
        Text number = new Text();


        if (split.length == 4) {
            term.set(split[1]);
            number.set(split[3]);
            context.write(term, number);
            //String Author = split[0];


        }
        else{
            term.set(split[1]);
            number.set("#" + split[2]);
            context.write(term ,  number);
        }

    }

}

