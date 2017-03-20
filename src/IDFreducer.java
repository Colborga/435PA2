import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IDFreducer extends Reducer<Text,Text,Text,DoubleWritable>{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		String hold = "";

		int count = 0;
		for(Text termVal : values){
			count++;
			hold += termVal.toString() + "<===>";
		}

		//where count is the number of authors that used this term

		//String[] hold = values.toString().split("\\s+");
		//String Author = hold[0];


		String[] AuthorsHold = hold.split("<===>");
		for(String s: AuthorsHold){
			String[] sHold = s.split("\\s+");
			String Author = sHold[0];
			double TFval = Double.parseDouble(sHold[1]);
			double AuthorCount = Double.parseDouble(sHold[2]);

			double idfVal = Math.log10(AuthorCount / count);

			context.write(new Text(Author + " " + key), new DoubleWritable(idfVal * TFval));
		}



		//double tfval = Double.parseDouble(hold[0]);


		//context.write(new Text(Author + " " + key + " " + AuthorCount), new IntWritable(count));

	}
}
