/**
 * Created by colborga on 3/19/17.
 */
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class IDFmapper extends Mapper<LongWritable, Text, Text, Text>{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        //String[] Line = value.toString().split("\n");


        String[] lineSplit = value.toString().split("\\s+");
        String Author = lineSplit[0];
        String Term = lineSplit[1];
        double TFval = Double.parseDouble(lineSplit[2]);
        int AuthorCount = Integer.parseInt(lineSplit[3]);




        context.write(new Text(Term), new Text(Author + " " + TFval + " " + AuthorCount));
    }
}

