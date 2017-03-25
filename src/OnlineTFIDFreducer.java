import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class OnlineTFIDFreducer extends Reducer<Text,Text,Text,DoubleWritable>{
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

//        Double tf = new Double(values.iterator().next().toString()); //values.iterator().next().toString()
//        Double idf = new Double(values.iterator().next().toString()); //values.iterator().next().toString()

        Double tf = new Double(0); //values.iterator().next().toString()
        //if(values.iterator().hasNext()){
        Double idf = new Double(0); //values.iterator().next().toString()
//        while(tf == 0 && idf == 0 && values.iterator().hasNext()){
//            String holdValue = values.iterator().next().toString();
//
//            if(holdValue.contains("#")){
//                tf = new Double(holdValue.split("#")[1]);
//            }
//            else{
//                idf = new Double(holdValue);
//            }
//            if(tf != 0 && idf != 0){
//                context.write(new Text("OnlineAuthor" + " " + key + " " + tf ), new DoubleWritable(idf));
//                break;
//            }
//
//        }

        for(Text t : values){
            String holdValue = t.toString();

            if(holdValue.contains("#")){
                tf = new Double(holdValue.split("#")[1]);
            }
            else{
                idf = new Double(holdValue);
            }
            if(tf != 0 && idf != 0){
                context.write(new Text("OnlineAuthor" + " " + key), new DoubleWritable(idf * tf));
                break;
            }
            else if(tf == 0 && idf != 0 && (!values.iterator().hasNext())){
                context.write(new Text("OnlineAuthor" + " " + key), new DoubleWritable(idf * .5));
                break;
            }
        }

        //Double TFIDF = new Double(tf * idf);



    }
}
