/**
 * Created by colborga on 3/19/17.
 */
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class IDFmapper extends Mapper<LongWritable, Text, Text, Text>{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

        String Term = "";
        /*ArrayList<String>*/ String Authors = "";//new ArrayList<>();
        ArrayList<String>  authorhold = new ArrayList<>();

        if(value.toString().startsWith(":")){
            String[] firstSplit = value.toString().split(":");

            //String[] lineSplit = firstSplit[0].split("\\s+");
//            String Author = lineSplit[0];
//            Term = lineSplit[1];
//            double TFval = Double.parseDouble(lineSplit[2]);
//            int AuthorCount = Integer.parseInt(lineSplit[3]);

            //context.write(new Text(Term), new Text(Author + " " + TFval + " " + AuthorCount));

//
//            String[] authorhold = firstSplit[1].split("\\s+");
//            for(String s : authorhold){
//                if(!Authors.contains(s)){
//                    Authors.add(s);
//                }
//            }

            Authors = value.toString();

            context.write(new Text(Term), new Text(Authors));
           //context.write(new Text(Term), new Text("&"+ Authors));

        }
        else{

            String[] lineSplit = value.toString().split("\\s+");
            String Author = lineSplit[0];
            Term = lineSplit[1];
            double TFval = Double.parseDouble(lineSplit[2]);
            //int AuthorCount = Integer.parseInt(lineSplit[3]);
            authorhold = new ArrayList<>(Arrays.asList(Authors.split("\\s+")));
            context.write(new Text(Term), new Text(Author + " " + TFval + " " + authorhold.size()));



        }

    }

}

