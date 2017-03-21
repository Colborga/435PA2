import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;


public class
IDFreducer extends Reducer<Text,Text,Text,DoubleWritable>{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		//Create a List of all Authors

		ArrayList<String> usedAuthors = new ArrayList<>();
		ArrayList<String> ListofAuthors = new ArrayList<>();


		String mapperValue = "";

		int count = 0;
		for(Text termVal : values){
			if(termVal.toString().startsWith("&")){
				String[] split = termVal.toString().split("&");
				ListofAuthors = new ArrayList<>(Arrays.asList(split[1].split(("\\s+"))));
			}
			else{
				if(values.iterator().hasNext()) {
					mapperValue += termVal.toString() + "<===>";
					usedAuthors.add(termVal.toString().split("\\s+")[0]);
					count++;

				}else{
					mapperValue += termVal.toString();
					usedAuthors.add(termVal.toString().split("\\s+")[0]);
					count++;
				}
			}


		}
		double idfVal = 0;
		
		for(String plz : mapperValue.split("<===>")) {
			String[] sHold = plz.split("\\s+");
			String Author = sHold[0];
			double TFval = Double.parseDouble(sHold[1]);
			double AuthorCount = Double.parseDouble(sHold[2]);

			idfVal = Math.log10(AuthorCount / count);
			context.write(new Text(Author + " " + key), new DoubleWritable(idfVal * TFval));
			//ListofAuthors.remove(Author);
		}

		for (String authorName : ListofAuthors) {
			if (!usedAuthors.contains(authorName)) {
				context.write(new Text(authorName + " " + key), new DoubleWritable(idfVal * .5));
			}
		}



		//double tfval = Double.parseDouble(mapperValue[0]);


		//context.write(new Text(Author + " " + key + " " + AuthorCount), new IntWritable(count));

	}
}
