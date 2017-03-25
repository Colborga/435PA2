/**
 * Created by colborga on 3/19/17.
 */
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OnlineCos2combiner extends Reducer<LongWritable, Text, Text, Text>{
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {



        Double StfidfX = new Double(0);
        Double Sauth2 = new Double(0);
        Double Sxyz2 = new Double(0);




        for(Text t : values){
            String[] split = t.toString().split("\\s+");

            Double tfidfX = new Double(split[0]);
            Double auth2 = new Double(split[1]);
            Double xyz2 = new Double(split[2]);

            StfidfX += tfidfX;
            Sauth2 += auth2;
            Sxyz2 += xyz2;

        }


        Text valuesPass = new Text(StfidfX + " " + Sauth2 + " " + Sxyz2);
        context.write(key , valuesPass);

    }

}

