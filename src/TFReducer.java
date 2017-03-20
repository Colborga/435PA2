import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.ArrayList;

public class TFReducer extends Reducer<Text,Text,Text,Text>{
	MultipleOutputs<Text, Text> mos;

	@Override
	public void setup(Context context) {
		 mos = new MultipleOutputs(context);
	}


	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		int count = 0;
		double maxFreq = 0;
		String mFTerm = "";
		int freq = 0;
 		ArrayList<Text> cache = new ArrayList<>();

		for(Text t : values){
			String[] tSplit = t.toString().split(",");
			freq = Integer.parseInt(tSplit[1]);
		         
			cache.add(new Text(t));
			if(freq > maxFreq){
				maxFreq = freq;
				mFTerm = tSplit[0];
			}
		}

		//this.mos.write("AuthorCount", NullWritable.get(), new Text("$ " + key + " " + mFTerm + " " + maxFreq));

		context.write(new Text("$ " + key + " " + mFTerm + " " + maxFreq), new Text(""));
		//4 terms "$" so that when you write this to a file you can count all of the lines that .split() = 4, thi		//this is that # of authors in the corpus

		double TFval = 0;
		//Calculate TF values for all of the terms for this author
		for(Text termVal : cache){
			String[] tfSplit = termVal.toString().split(",");
			freq = Integer.parseInt(tfSplit[1]);
			TFval = .5 +(.5*(freq/maxFreq));

			//this.mos.write("TFvalue", NullWritable.get(), new Text(key + " " + tfSplit[0] + " " + TFval));
			context.write(new Text(key), new Text(tfSplit[0] + " " + TFval));
		}

	        	
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}

}
