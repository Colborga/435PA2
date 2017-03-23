import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class OnlineMainClass {
    private static final String UNIGRAM = "/PA2online";
    private static final String TF = "/PA2onlineTF";
    private static final String AUTHORCOUNT= "/PA2onlineAuthorCount";
    private static final String OnlineIDF= "/PA2onlineIDF";
    private static final String IDFOUT = "/PA2outputIDF";
    private static final String AAVOUT = "/PA2onlineAAV";

    public static void main(String[] args) throws IOException, ClassNotFoundException,
            InterruptedException {
        if (args.length != 1) {
            System.out.printf("Usage: <jar file> <input dir>\n");
            System.exit(-1);
        }


        Configuration conf = new Configuration();

        //////Unigram Job
        Job OnlineUnigram = Job.getInstance(conf);
        OnlineUnigram.setJarByClass(MainClass.class);
        OnlineUnigram.setMapperClass(WordCountMapper.class);
        OnlineUnigram.setReducerClass(WordCountReducer.class);
        OnlineUnigram.setOutputKeyClass(Text.class);
        OnlineUnigram.setOutputValueClass(Text.class);
        OnlineUnigram.setInputFormatClass(TextInputFormat.class);
        OnlineUnigram.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(OnlineUnigram, new Path(args[0]));
        FileOutputFormat.setOutputPath(OnlineUnigram, new Path(UNIGRAM));

        OnlineUnigram.waitForCompletion(true);

        /////TF job
        Job OnlineTF = Job.getInstance(conf);
        OnlineTF.setJarByClass(MainClass.class);
        OnlineTF.setMapperClass(TFMapper.class);
        OnlineTF.setReducerClass(TFReducer.class);
        OnlineTF.setOutputKeyClass(Text.class);
        OnlineTF.setOutputValueClass(Text.class);
        OnlineTF.setInputFormatClass(TextInputFormat.class);
        OnlineTF.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(OnlineTF, new Path(UNIGRAM));
        FileOutputFormat.setOutputPath(OnlineTF, new Path(TF));

        OnlineTF.waitForCompletion(true);

        //Calculated TF, Now smash this into the IDF that is stored in HDFS

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
//        FileOutputFormat.setOutputPath(getIDF, new Path(OnlineIDF));
//
//        getIDF.waitForCompletion(true);

        //IDF job
        Job TFIDF = Job.getInstance(conf);
        TFIDF.setJarByClass(MainClass.class);
        TFIDF.setMapperClass(OnlineTFIDFmapper.class);
        TFIDF.setReducerClass(OnlineTFIDFreducer.class);
        TFIDF.setOutputKeyClass(Text.class);
        TFIDF.setOutputValueClass(Text.class);
        TFIDF.setInputFormatClass(TextInputFormat.class);
        TFIDF.setOutputFormatClass(TextOutputFormat.class);
        MultipleInputs.addInputPath(TFIDF, new Path(IDFOUT), TextInputFormat.class);
        MultipleInputs.addInputPath(TFIDF, new Path(TF) ,TextInputFormat.class);
        FileOutputFormat.setOutputPath(TFIDF, new Path(AAVOUT));

        TFIDF.waitForCompletion(true);









    }

}
