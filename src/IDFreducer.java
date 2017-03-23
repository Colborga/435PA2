import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;


public class IDFreducer extends Reducer<Text,Text,Text,DoubleWritable>{

	String content = "";
	FileInputStream fileStream;


	@Override
	protected void setup(Reducer.Context context) throws IOException, InterruptedException {
		try{
			Path[] stopWordsFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			//Path[] stopWordsFiles = context.getLocalCacheFiles();
			fileStream = new FileInputStream(stopWordsFiles[0].toString());
			content = new Scanner(fileStream).useDelimiter("\\Z").next();
//            if(stopWordsFiles != null && stopWordsFiles.length > 0) {
//                for(Path stopWordFile : stopWordsFiles) {
			//readFile(stopWordFile);
//                    //Path[] cacheFiles = context.getLocalCacheFiles();
//
//                }
//            }
		} catch(IOException ex) {

			System.err.println("Exception in mapper setup: " + ex.getMessage());
		}

	}


	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		//Create a List of all Authors

		ArrayList<String> usedAuthors = new ArrayList<>();
		ArrayList<String> ListofAuthors = new ArrayList<>(Arrays.asList((content).split("\\s+")));
		ArrayList<String> cache = new ArrayList<>();


		String mapperValue = "";

		int count = 0;


		for(Text termVal : values){
			//String[] split1 = termVal.toString().split(":");


			String[] split = termVal.toString().split("\\s+");
			String Authorname = split[0];

			usedAuthors.add(Authorname + " " + split[2]);

			count++;

			//cache.add(termVal.toString());
		}








//		for(String plz : cache){
//			String[] sHold = plz.toString().split("\\s+");
//			String Author = sHold[0];
//			String Term = sHold[1];
//			double TFval = Double.parseDouble(sHold[2]);
//			//Double AuthorCount = Double.parseDouble(sHold[2]);
//			//String AuthorCount = context.getConfiguration();
//
//
//
//			//textA.set(Author + " " + key + " " + TFval);
//
//			if (usedAuthors.contains(Author)) {
//				//context.write(new Text(Author + " " + key + " " + TFval), new DoubleWritable((idfVal)));
//			}
//
//			context.write(new Text(cache + " " + key + " " + TFval), new DoubleWritable((idfVal)));
//
//
//			//context.write(new Text(Author + " " + key + " " + TFval + " " + idfVal	), new DoubleWritable(ListofAuthors.size()));
//			//ListofAuthors.remove(Author);
//		}



		ArrayList<String> usedAuthorsNames = new ArrayList<>();

		for (String authorName : usedAuthors) {
			String TFval = authorName.split("\\s+")[1];
			String auth = authorName.split("\\s+")[0];
			double idfVal = Math.log10((double)ListofAuthors.size() / (double)count);
			context.write(new Text(auth + " " + key + " " + TFval), new DoubleWritable((idfVal)));
			//usedAuthors.remove(authorName);
			usedAuthorsNames.add(auth);
		}

		for (String authorName : ListofAuthors) {
			if (!usedAuthorsNames.contains(authorName)) {
				double idfVal = Math.log10((double)ListofAuthors.size() / (double)count);
				context.write(new Text(authorName + " " + key + " " + .5), new DoubleWritable((idfVal)));
			}
		}



		//double tfval = Double.parseDouble(mapperValue[0]);


		//context.write(new Text(Author + " " + key + " " + AuthorCount), new IntWritable(count));

	}
}