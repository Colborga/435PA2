import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;


public class
IDFreducer extends Reducer<Text,Text,Text,DoubleWritable>{

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		//Create a List of all Authors

		ArrayList<String> usedAuthors = new ArrayList<>();
		ArrayList<String> ListofAuthors = new ArrayList<>();
		ArrayList<Text> cache = new ArrayList<>();


		String mapperValue = "";

		int count = 0;
		for(Text termVal : values){
			String[] split = termVal.toString().split("&");
			String Author = split[0];
			//String Term = split[1];
			//String AuthorCount = split[2];
			usedAuthors.add(Author);

			count++;

//			else{
//				if(values.iterator().hasNext()) {
//					mapperValue += termVal.toString() + "<===>";
//					usedAuthors.add(termVal.toString().split("\\s+")[0]);
//					count++;
//
//				}else{
//					mapperValue += termVal.toString();
//					usedAuthors.add(termVal.toString().split("\\s+")[0]);
//					count++;
//				}
//			}
			cache.add(termVal);
		}
		double idfVal = 0;

		Text textA = new Text();
		DoubleWritable dwA = new DoubleWritable();

		for(Text plz : cache){
			String[] sHold = plz.toString().split("\\s+");
			String Author = sHold[0];
			String Term = sHold[1];
			double TFval = Double.parseDouble(sHold[1]);
			Double AuthorCount = Double.parseDouble(sHold[2]);
			//String AuthorCount = context.getConfiguration();

			idfVal = Math.log10(AuthorCount / count);

			textA.set(Author + " " + key + " " + TFval);
			dwA.set(idfVal);
			context.write(new Text(Author + " " + key + " " + TFval + " " + idfVal	), new DoubleWritable(AuthorCount));
			//ListofAuthors.remove(Author);
		}

//		for (String authorName : ListofAuthors) {
//			if (!usedAuthors.contains(authorName)) {
//				textA.set(authorName + " " + key + " " + .5);
//				dwA.set(idfVal);
//
//				context.write(textA, dwA);
//			}
//		}



		//double tfval = Double.parseDouble(mapperValue[0]);


		//context.write(new Text(Author + " " + key + " " + AuthorCount), new IntWritable(count));

	}
}