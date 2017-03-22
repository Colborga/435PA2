/**
 * Created by colborga on 3/19/17.
 */
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class IDFmapper extends Mapper<LongWritable, Text, Text, Text>{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

        String Term = "";
        String Authors = "";

        String[] firstSplit = value.toString().split(":");

        String[] lineSplit = firstSplit[0].split("\\s+");
        String Author = lineSplit[0];
        Term = lineSplit[1];
        double TFval = Double.parseDouble(lineSplit[2]);
        int AuthorCount = Integer.parseInt(lineSplit[3]);
        context.write(new Text(Term), new Text(Author + " " + TFval + " " + AuthorCount));

        Authors = firstSplit[1];
        context.write(new Text(Term), new Text("&"+ Authors));







    }

}
