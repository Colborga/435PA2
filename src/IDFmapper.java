/**
 * Created by colborga on 3/19/17.
 */

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class IDFmapper extends Mapper<LongWritable, Text, Text, Text>{

//    private Set stopWords = new HashSet();
//    private Text word = new Text();
//    String content = "";
//    FileInputStream fileStream;
//
//    @Override
//    protected void setup(Context context) throws IOException, InterruptedException {
//        try{
//            Path[] stopWordsFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
//            //Path[] stopWordsFiles = context.getLocalCacheFiles();
//            fileStream = new FileInputStream(stopWordsFiles[0].toString());
//            content = new Scanner(fileStream).useDelimiter("\\Z").next();
////            if(stopWordsFiles != null && stopWordsFiles.length > 0) {
////                for(Path stopWordFile : stopWordsFiles) {
//                    //readFile(stopWordFile);
////                    //Path[] cacheFiles = context.getLocalCacheFiles();
////
////                }
////            }
//        } catch(IOException ex) {
//
//            System.err.println("Exception in mapper setup: " + ex.getMessage());
//        }
//
//    }
//    @Override
//    protected void setup(Context context) throws IOException, InterruptedException {
//        if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
//            URI[] mappingFileUri = context.getCacheFiles();
//
//            if (mappingFileUri != null) {
//                for (Path stopWordFile : mappingFileUri) {
//                    readFile(stopWordFile);
//
//                }
//            }
//        }
//    }


//    private void readFile(Path filePath) {
//        try{
//            BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));
//            String stopWord = null;
//            while((stopWord = bufferedReader.readLine()) != null) {
//                stopWords.add(stopWord.toLowerCase());
//            }
//        } catch(IOException ex) {
//            System.err.println("Exception while reading stop words file: " + ex.getMessage());
//        }
//    }




    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{


        //StringTokenizer st = new StringTokenizer(line," ");
        //ArrayList<String> Authors = new ArrayList<>(Arrays.asList(content.split("\\s+")));

        //String content = new Scanner(fileStream).useDelimiter("\\Z").next();

        String[] lineSplit = value.toString().split("\\s+");
        if(!lineSplit[0].equals("$")) {
            String Author = lineSplit[0];
            String Term = lineSplit[1];
            double TFval = Double.parseDouble(lineSplit[2]);
            //int AuthorCount = Integer.parseInt(lineSplit[3]);
            context.write(new Text(Term), new Text(Author + " " + Term + " " + TFval));
        }
        //Authors = firstSplit[1];
        //context.write(new Text(Term), new Text("&"+ Authors));


    }

}
