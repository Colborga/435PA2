import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class OnlineMainClass {
//    private static final String UNIGRAM = "/PA2onlineUnigram";
//    private static final String TF = "/PA2onlineTF";
//    private static final String AUTHORCOUNT= "/PA2onlineAuthorCount";
//    private static final String OnlineIDF= "/PA2onlineIDF";
//    private static final String AAVOUT = "/PA2onlineAAV";
//    private static final String COS1out = "/PA2onlineCOS1";
//    private static final String COS2out = "/PA2onlineCOS2";
//
//
    private static final String TFIDFOUT = "/7PA2outputTFIDF";
    private static final String IDFOUT = "/7PA2outputIDF";



    private static final String UNIGRAM7= "/7PA2onlineUnigram";
    private static final String TF7 = "/7PA2onlineTF";
    private static final String AUTHORCOUNT7= "/7PA2onlineAuthorCount";
    private static final String OnlineIDF7 = "/7PA2onlineIDF";
    private static final String AAVOUT7 = "/7PA2onlineAAV";
    private static final String COS1out7 = "/7PA2onlineCOS1";
    private static final String COS2out7 = "/7PA2onlineCOS2";


    public static void main(String[] args) throws IOException, ClassNotFoundException,
            InterruptedException {
        if (args.length != 1) {
            System.out.printf("Usage: <jar file> <input dir>\n");
            System.exit(-1);
        }


        Configuration conf = new Configuration();

//        //////Unigram Job
//        Job OnlineUnigram = Job.getInstance(conf);
//        OnlineUnigram.setJarByClass(MainClass.class);
//        OnlineUnigram.setMapperClass(WordCountMapper.class);
//        OnlineUnigram.setReducerClass(WordCountReducer.class);
//        OnlineUnigram.setOutputKeyClass(Text.class);
//        OnlineUnigram.setOutputValueClass(Text.class);
//        OnlineUnigram.setInputFormatClass(TextInputFormat.class);
//        OnlineUnigram.setOutputFormatClass(TextOutputFormat.class);
//        FileInputFormat.setInputPaths(OnlineUnigram, new Path(args[0]));
//        FileOutputFormat.setOutputPath(OnlineUnigram, new Path(UNIGRAM7));
////
//        OnlineUnigram.waitForCompletion(true);
//
//        /////TF job
//        Job OnlineTF = Job.getInstance(conf);
//        OnlineTF.setJarByClass(MainClass.class);
//        OnlineTF.setMapperClass(TFMapper.class);
//        OnlineTF.setReducerClass(TFReducer.class);
//        OnlineTF.setOutputKeyClass(Text.class);
//        OnlineTF.setOutputValueClass(Text.class);
//        OnlineTF.setInputFormatClass(TextInputFormat.class);
//        OnlineTF.setOutputFormatClass(TextOutputFormat.class);
//        FileInputFormat.setInputPaths(OnlineTF, new Path(UNIGRAM7));
//        FileOutputFormat.setOutputPath(OnlineTF, new Path(TF7));
//
//        OnlineTF.waitForCompletion(true);
//
//        /////Calculated TF, Now smash this into the IDF that is stored in HDFS
//
//        //IDF job
//        Job getIDF = Job.getInstance(conf);
//        getIDF.setJarByClass(MainClass.class);
//        getIDF.setMapperClass(CleanIDFmapper.class);
//        getIDF.setReducerClass(CleanIDFreducer.class);
//        getIDF.setOutputKeyClass(Text.class);
//        getIDF.setOutputValueClass(Text.class);
//        getIDF.setInputFormatClass(TextInputFormat.class);
//        getIDF.setOutputFormatClass(TextOutputFormat.class);
//        FileInputFormat.setInputPaths(getIDF, new Path(IDFOUT));
//        FileOutputFormat.setOutputPath(getIDF, new Path(OnlineIDF7));
//
//        getIDF.waitForCompletion(true);
//
//        //IDF job
//        Job TFIDF = Job.getInstance(conf);
//        TFIDF.setNumReduceTasks(20);
//        TFIDF.setJarByClass(MainClass.class);
//        TFIDF.setMapperClass(OnlineTFIDFmapper.class);
//        TFIDF.setReducerClass(OnlineTFIDFreducer.class);
//        TFIDF.setOutputKeyClass(Text.class);
//        TFIDF.setOutputValueClass(Text.class);
//        TFIDF.setInputFormatClass(TextInputFormat.class);
//        TFIDF.setOutputFormatClass(TextOutputFormat.class);
//        MultipleInputs.addInputPath(TFIDF, new Path(IDFOUT), TextInputFormat.class);
//        MultipleInputs.addInputPath(TFIDF, new Path(TF7) ,TextInputFormat.class);
//        FileOutputFormat.setOutputPath(TFIDF, new Path(AAVOUT7));
//
//        TFIDF.waitForCompletion(true);

        //COS1 job
//        Job COS1 = Job.getInstance(conf);
//       // COS1.setNumReduceTasks(20);
//        COS1.setJarByClass(MainClass.class);
//        COS1.setMapperClass(OnlineCosMapper.class);
//        COS1.setReducerClass(OnlineCosReducer.class);
//        COS1.setOutputKeyClass(Text.class);
//        COS1.setOutputValueClass(Text.class);
//        COS1.setInputFormatClass(TextInputFormat.class);
//        COS1.setOutputFormatClass(TextOutputFormat.class);
//        MultipleInputs.addInputPath(COS1, new Path(AAVOUT7), TextInputFormat.class);
//        MultipleInputs.addInputPath(COS1, new Path(TFIDFOUT) ,TextInputFormat.class);
//        FileOutputFormat.setOutputPath(COS1, new Path(COS1out7));
//
//        COS1.waitForCompletion(true);
//

        //COS1 job
        Job COS2 = Job.getInstance(conf);
        COS2.setJarByClass(MainClass.class);
        COS2.setMapperClass(OnlineCos2mapper.class);
        COS2.setCombinerClass(OnlineCos2combiner.class);
        COS2.setReducerClass(OnlineCos2reducer.class);
        COS2.setOutputKeyClass(Text.class);
        COS2.setOutputValueClass(Text.class);
        COS2.setInputFormatClass(TextInputFormat.class);
        COS2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(COS2, new Path(COS1out7));
        FileOutputFormat.setOutputPath(COS2, new Path(COS2out7));

        COS2.waitForCompletion(true);













    }

}
