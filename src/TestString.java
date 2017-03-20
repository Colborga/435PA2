import org.apache.hadoop.io.Text;

public class TestString {
   
    public static void main(String args[]){
    	 String FormatString = "I would have liked to visit the birthplace of  Gutenburg, but it could not be done, as no memorandum of the site of the  house has been kept.";
    		String[] arr = FormatString.split(" ");;
    		//For bigram
    	    final String startFlag ="_START_ ";
    	    final String endFlag =" _END_";
    	    String Bistring =  FormatString + endFlag;    
    	    
    	    String prev = startFlag;
    	    
    	    for(String word: Bistring.split(" ")) {       	   
    	    	
    	    	System.out.println(prev + " " + word);
    	    	prev = word;
    	    }
    	    	
    }
        
}
    
