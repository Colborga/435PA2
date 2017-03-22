/**
 * Created by colborga on 3/19/17.
 */

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AuthorCountMapper extends Mapper<LongWritable, Text, Text, Text>{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{


            String[] lineSplit = value.toString().split("\\s+");
            String Author;
            String Term;
            double TFval;
            if(!lineSplit[0].equals("$")){
                 Author = lineSplit[0];
                 Term = lineSplit[1];
                 TFval = Double.parseDouble(lineSplit[2]);
                context.write(new Text(""), new Text(Author + " " + Term + " " + TFval));

            }
            else{
                String dollar = lineSplit[0];
                Author = lineSplit[1];
                Term = lineSplit[2];
                TFval = Double.parseDouble(lineSplit[3]);

                context.write(new Text(dollar), new Text( Author + " " + Term + " " + TFval));
            }

        }
}

