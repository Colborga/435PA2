/**
 * Created by colborga on 3/19/17.
 */

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class AuthorCountReducer extends Reducer<Text,Text,Text, IntWritable>{

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		ArrayList<Text> cache = new ArrayList<>();
		int AuthorCount = 0;
		for(Text t : values){
            String[] hold = t.toString().split("\\s+");
			if(hold[0].equals("$")){
				AuthorCount++;
                //context.write(new Text("Number of Authors: "), new IntWritable(AuthorCount));
			}
			else{
                cache.add(new Text(t));
            }
		}
            //context.write(new Text("Number of Authors: "), new IntWritable(AuthorCount));



            for(Text termVal : cache){
                String[] tfSplit = termVal.toString().split("\\s+");
                double TFval = 0.0;
                if(!tfSplit[2].isEmpty()){
                     TFval = Double.parseDouble(tfSplit[2]);
                }
                else{
                    TFval = 69;
                }

                context.write(new Text(tfSplit[0] + " " + tfSplit[1] + " " + TFval), new IntWritable(AuthorCount));
            }
        }
}
