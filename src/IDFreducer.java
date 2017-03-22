import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;


public class
IDFreducer extends Reducer<Text,Text,Text,DoubleWritable>{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		//Create a List of all Authors

		ArrayList<String> usedAuthors = new ArrayList<>();
		ArrayList<String> ListofAuthors = new ArrayList<>();

		ArrayList<String> cache = new ArrayList<>();
		ArrayList<Text> cache1 = new ArrayList<>();

		String mapperValue = "";

		int count = 0;
		double idfVal = 0;

		String Author = "";
		double TFval = 0.0;



//		if(values.iterator().toString().startsWith(":")){
//			String[] split = values.toString().split(":");
//			ListofAuthors = new ArrayList<>(Arrays.asList(split[1].split(("\\s+"))));
//			//context.write(new Text(ListofAuthors.toString()), new DoubleWritable(ListofAuthors.size()));
//			AuthorCount = new Double(ListofAuthors.size());
//		}else
//		{
//			AuthorCount = new Double(12);
//		}

		int AuthorCount = 69;

		for(Text termVal : values) {

			if (termVal.toString().startsWith(":")) {
				String[] split = termVal.toString().split(":");
				ListofAuthors = new ArrayList<>(Arrays.asList(split[1].split(("\\s+"))));
				context.write(new Text(ListofAuthors.toString()), new DoubleWritable(ListofAuthors.size()));

				AuthorCount = ListofAuthors.size();
			}
			else{
				cache1.add(termVal);
			}
		}


			for(Text termVal1 : cache1){

//				if(values.iterator().hasNext()) {
//					//mapperValue += termVal.toString() + "<===>";
//					usedAuthors.add(termVal.toString().split("\\s+")[0]);
//					count++;
//
//				}else{
//					//mapperValue += termVal.toString();
//					usedAuthors.add(termVal.toString().split("\\s+")[0]);
//					count++;
//				}

				//ListofAuthors.remove(Author);


				usedAuthors.add(termVal1.toString().split("\\s+")[0]);
				count++;
				cache.add(termVal1.toString() + " " + AuthorCount);

			}



		for(String cacheval : cache){
			String[] sHold = cacheval.toString().split("\\s+");
			Author = sHold[0];
			TFval = Double.parseDouble(sHold[1]);

			Double authorcount = new Double(sHold[2]);

			idfVal = Math.log10(authorcount / count);
			//context.write(new Text(Author + " " + key), new DoubleWritable(idfVal * TFval));
			context.write(new Text(Author + " " + key + " " + TFval), new DoubleWritable(authorcount));

		}


		for (String authorName : ListofAuthors) {
			if (!usedAuthors.contains(authorName)) {
				//context.write(new Text(authorName + " " + key), new DoubleWritable(idfVal * .5));
				context.write(new Text(authorName + " " + key + " " + "0.5"), new DoubleWritable(idfVal));
			}
		}




//		for(String plz : mapperValue.split("<===>")) {
//			String[] sHold = plz.split("\\s+");
//			String Author = sHold[0];
//			double TFval = Double.parseDouble(sHold[1]);
//			double AuthorCount = Double.parseDouble(sHold[2]);
//
//			idfVal = Math.log10(AuthorCount / count);
//			//context.write(new Text(Author + " " + key), new DoubleWritable(idfVal * TFval));
//			context.write(new Text(Author + " " + key + " " + TFval), new DoubleWritable(idfVal));
//			//ListofAuthors.remove(Author);
//		}




		//double tfval = Double.parseDouble(mapperValue[0]);


		//context.write(new Text(Author + " " + key + " " + AuthorCount), new IntWritable(count));

	}
}
