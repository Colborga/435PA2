import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;


public class TFIDFreducer extends Reducer<Text,Text,Text,Text>{
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String[] hold = key.toString().split("\\s+");
        String Author = hold[0];
        String Term = hold[1];

        for(Text value : values) {
            String[] lineSplit = value.toString().split("\\s+");


            Double TF = new Double(lineSplit[0]);
            Double IDF = new Double(lineSplit[1]);
            Double TFIDF = TF * IDF;
            context.write(new Text(Author), new Text(Term + " " + TFIDF.toString()));
        }



    }
}
