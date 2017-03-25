/**
 * Created by colborga on 3/19/17.
 */
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OnlineCosMapper extends Mapper<LongWritable, Text, Text, Text>{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split("\\s+");

        Text author = new Text();
        Text term = new Text();
        Text tfidf = new Text();
        Text pass = new Text();

        author.set(split[0]);
        term.set(split[1]);
        tfidf.set(split[2]);

        pass.set(author.toString() + " " + tfidf.toString());

        context.write(term , pass);


    }

}

