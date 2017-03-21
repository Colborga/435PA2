import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class MainClass {
    private static final String UNIGRAM = "/PA2output";
    private static final String TF = "/PA2output2";
    private static final String AUTHORCOUNT= "/PA2outputAuthorCount";
    private static final String IDF= "/PA2outputIDF";
    private static final String AAVOUT = "/PA2outputAAV";

    public static void main(String[] args) throws IOException, ClassNotFoundException,
            InterruptedException {
        if (args.length != 1) {
            System.out.printf("Usage: <jar file> <input dir>\n");
            System.exit(-1);
        }







        Configuration conf = new Configuration();
        Job job=Job.getInstance(conf);
        job.setJarByClass(MainClass.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(UNIGRAM));

        job.waitForCompletion(true);

        //TF job
        Job job1 = Job.getInstance(conf);
        job1.setJarByClass(MainClass.class);
        job1.setMapperClass(TFMapper.class);
        job1.setReducerClass(TFReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job1, new Path(UNIGRAM));
        FileOutputFormat.setOutputPath(job1, new Path(TF));
        //MultipleOutputs.addNamedOutput(job1, "AuthorCount", TextOutputFormat.class, job.getOutputKeyClass(), job.getOutputValueClass());
        //MultipleOutputs.addNamedOutput(job1, "TFvalue" , TextOutputFormat.class, job.getOutputKeyClass(), job.getOutputValueClass());

        job1.waitForCompletion(true);

        //IDF job
        Job AuthorCount = Job.getInstance(conf);
        AuthorCount.setJarByClass(MainClass.class);
        AuthorCount.setMapperClass(AuthorCountMapper.class);
        AuthorCount.setReducerClass(AuthorCountReducer.class);
        AuthorCount.setOutputKeyClass(Text.class);
        AuthorCount.setOutputValueClass(Text.class);
        AuthorCount.setInputFormatClass(TextInputFormat.class);
        AuthorCount.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(AuthorCount, new Path(TF));
        FileOutputFormat.setOutputPath(AuthorCount, new Path(AUTHORCOUNT));

        AuthorCount.waitForCompletion(true);



        //IDF job
        Job TFIDF = Job.getInstance(conf);
        TFIDF.setJarByClass(MainClass.class);
        TFIDF.setMapperClass(IDFmapper.class);
        TFIDF.setReducerClass(IDFreducer.class);
        TFIDF.setOutputKeyClass(Text.class);
        TFIDF.setOutputValueClass(Text.class);
        TFIDF.setInputFormatClass(TextInputFormat.class);
        TFIDF.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(TFIDF, new Path(AUTHORCOUNT));
        FileOutputFormat.setOutputPath(TFIDF, new Path(IDF));

        TFIDF.waitForCompletion(true);

        //IDF job
        Job AAV = Job.getInstance(conf);
        AAV.setJarByClass(MainClass.class);
        AAV.setMapperClass(AAVmapper.class);
        AAV.setReducerClass(AAVreducer.class);
        AAV.setOutputKeyClass(Text.class);
        AAV.setOutputValueClass(Text.class);
        AAV.setInputFormatClass(TextInputFormat.class);
        AAV.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(AAV, new Path(IDF));
        FileOutputFormat.setOutputPath(AAV, new Path(AAVOUT));

        AAV.waitForCompletion(true);



    }

}
