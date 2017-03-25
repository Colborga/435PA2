import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;


public class OnlineCosReducer extends Reducer<Text,Text,Text,Text>{
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


        ArrayList<Text> cache = new ArrayList<>();


        Double DXYZtfidf = new Double(0);

        for(Text t : values){
            String[] split = t.toString().split("\\s+");

            if(split[0].equals("OnlineAuthor")){
                DXYZtfidf = new Double(split[1]);
            }
            else{
                cache.add(new Text(t));
                //context.write(new Text(t), new Text(DXYZtfidf.toString()));

            }
        }





        Text tfidf = new Text();
        Text k = new Text();
        Text v = new Text();
        for(Text c : cache){
            String[] cacheSplit = c.toString().split("\\s+");
            tfidf.set(cacheSplit[1]);

            Double Dtfidf = new Double(tfidf.toString());


            Double multaXYZ = new Double(Dtfidf*DXYZtfidf);
            Double tfidfS= new Double(Dtfidf*Dtfidf);
            Double XYZtfidfS= new Double(DXYZtfidf*DXYZtfidf);


            k.set(cacheSplit[0] + "," + "xyz" );
            v.set(":" + multaXYZ.toString() + " " + tfidfS.toString() + " " + XYZtfidfS.toString());

            context.write(k, v);
        }

    }
}
