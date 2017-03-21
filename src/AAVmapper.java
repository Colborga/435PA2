/**
 * Created by colborga on 3/19/17.
 */

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AAVmapper extends Mapper<LongWritable, Text, Text, Text>{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{


        String[] lineSplit = value.toString().split("\\s+");
        String Author = lineSplit[0];
        String Term = lineSplit[1];
        Double TFIDF = Double.parseDouble(lineSplit[2]);




        context.write(new Text(Author + " " + Term), new Text(TFIDF.toString()));

    }
}

