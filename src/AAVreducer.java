/**
 * Created by colborga on 3/19/17.
 */

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class AAVreducer extends Reducer<Text,DoubleWritable,Text, DoubleWritable>{

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        //for(DoubleWritable d : values){
        context.write(key,values.iterator().next());
        //}

    }
}