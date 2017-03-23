/**
 * Created by colborga on 3/19/17.
 */

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class AuthorCountReducer extends Reducer<Text,Text,Text, Text>{

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        ArrayList<Text> cache = new ArrayList<>();
        String AuthorString = "";
        int AuthorCount = 0;
        Text textA = new Text();

        for(Text t : values){
            String[] hold = t.toString().split("\\s+");
            if(hold[0].equals("$")){
                AuthorCount++;
                if(values.iterator().hasNext()){
                    AuthorString += hold[1] + " ";
                }
                else{
                    AuthorString += hold[1];
                }

                //context.write(new Text("Number of Authors: "), new IntWritable(AuthorCount));
            }
            else{
                //textA.set(t);
                //cache.add(new Text(t));
            }
        }
        //context.write(new Text("Number of Authors: "), new IntWritable(AuthorCount));



        Text textB = new Text();
        textB.set(AuthorString);
        context.write(textB, new Text(""));

//        for(Text termVal : cache){
//            String[] tfSplit = termVal.toString().split("\\s+");
//            double TFval = 0.0;
//            if(!tfSplit[2].isEmpty()){
//                TFval = Double.parseDouble(tfSplit[2]);
//            }
//            else{
//                //TFval = 69;
//            }
//            textB.set(tfSplit[0] + " " + tfSplit[1] + " " + TFval + " " + AuthorCount + ":" + AuthorString);
//            context.write(textB, new Text(""));
//        }

        //context.write(new Text("&" + AuthorString), new Text(""));
    }

}