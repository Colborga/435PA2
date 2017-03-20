import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, Text>{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
      
        String[] ADString = value.toString().split("<===>");
        String[] AuthorList = ADString[0].split(" ");
        	String Author = AuthorList[AuthorList.length - 1].toString();
        
        	
        String Sentence = ADString[2].toString().replaceAll("[^a-zA-Z0-9 ]", "").toLowerCase();
        String[] FormatString = Sentence.split("\\s+");
        
        
        for(String word: FormatString) {       	
            if(word.trim().length() != 0){        
            	context.write(new Text(Author + " " + word), new Text("oneB"));
            }
        }
        
    }
   }

      
 

