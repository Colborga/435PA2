/**
 * Created by colborga on 3/19/17.
 */

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class AuthorCountReducer extends Reducer<Text,Text,Text, Text>{

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		ArrayList<Text> cache = new ArrayList<>();String AuthorString = "";
		//int AuthorCount = 0;
        String author = "";
        String term = "";
            double TFval = 0.0;


		if(key.toString().equals("$")){
            for(Text t : values){
                String[] hold = t.toString().split("\\s+");
                author = hold[0];
                term = hold[1];
                    //AuthorCount++;
                    if(values.iterator().hasNext()){
                        AuthorString += hold[0] + " ";
                    }
                    else{
                        AuthorString += hold[0];
                    }


                if(!hold[2].isEmpty()){
                    TFval = Double.parseDouble(hold[2]);
                }

            }
            context.write(new Text(":" + AuthorString), new Text(""));
        }
        else{
//            for(Text t : values){
//                String[] hold = t.toString().split("\\s+");
//                if(key.toString().equals("$")){
//                    AuthorCount++;
//                    if(values.iterator().hasNext()){
//                        AuthorString += hold[1] + " ";
//                    }
//                    else{
//                        AuthorString += hold[1];
//                    }
//
//                    //context.write(new Text("Number of Authors: "), new IntWritable(AuthorCount));
//                }
//                else{
//                    cache.add(new Text(t));
//                }
//            }
//            //context.write(new Text("Number of Authors: "), new IntWritable(AuthorCount));

            for(Text termVal : values){
                String[] tfSplit = termVal.toString().split("\\s+");

                if(!tfSplit[2].isEmpty()){
                    TFval = Double.parseDouble(tfSplit[2]);
                }
                else{
                    //TFval = 69;
                }

                context.write(new Text(tfSplit[0] + " " + tfSplit[1] + " " + TFval ), new Text(""));
            }

            //context.write(new Text("&" + AuthorString), new Text(""));
        }
        }



}
