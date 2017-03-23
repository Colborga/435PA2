import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class OnlineTFIDFreducer extends Reducer<Text,Text,Text,DoubleWritable>{
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Double one = new Double(values.iterator().next().toString());
        Double two = new Double(values.iterator().next().toString());
        Double TFIDF = new Double(one * two);


        context.write(new Text("OnlineAuthor" + " " + key), new DoubleWritable(TFIDF));
    }
}
