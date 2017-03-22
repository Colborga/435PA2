/**
 * Created by colborga on 3/19/17.
 */
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TFIDFmapper extends Mapper<LongWritable, Text, Text, Text>{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

        String Term = "";


        String[] lineSplit = value.toString().split("\\s+");
        String Author = lineSplit[0];
        Term = lineSplit[1];
        Double TF = new Double(lineSplit[2]);
        Double IDF = new Double(lineSplit[3]);
        context.write(new Text(Author + " " + Term), new Text( TF.toString() + " " + IDF.toString()));

    }

}

