import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class MainClass {
    private static final String UNIGRAM = "/PA2output";
    private static final String TF = "/PA2output2";
    private static final String AUTHORCOUNT= "/PA2outputAuthorCount";
    private static final String IDFOUT= "/PA2outputIDF";
    private static final String AAVOUT = "/PA2outputAAV";
    private static final String TFIDFOUT = "/PA2outputTFIDF";

    private static final String mb7UNIGRAM = "/7PA2output";
    private static final String mb7TF = "/7PA2output2";
    private static final String mb7AUTHORCOUNT= "/7PA2outputAuthorCount";
    private static final String mb7IDFOUT= "/7PA2outputIDF";
    private static final String mbAAVOUT = "/7PA2outputAAV";
    private static final String mb7TFIDFOUT = "/7PA2outputTFIDF";

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
        FileOutputFormat.setOutputPath(job, new Path(mb7UNIGRAM));

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
        FileInputFormat.setInputPaths(job1, new Path(mb7UNIGRAM));
        FileOutputFormat.setOutputPath(job1, new Path(mb7TF));
        //MultipleOutputs.addNamedOutput(job1, "AuthorCount", TextOutputFormat.class, job.getOutputKeyClass(), job.getOutputValueClass());
        //MultipleOutputs.addNamedOutput(job1, "TFvalue" , TextOutputFormat.class, job.getOutputKeyClass(), job.getOutputValueClass());

        job1.waitForCompletion(true);


        Job AuthorCount = Job.getInstance(conf);
        AuthorCount.setJarByClass(MainClass.class);
        AuthorCount.setMapperClass(AuthorCountMapper.class);
        AuthorCount.setReducerClass(AuthorCountReducer.class);
        AuthorCount.setOutputKeyClass(Text.class);
        AuthorCount.setOutputValueClass(Text.class);
        AuthorCount.setInputFormatClass(TextInputFormat.class);
        AuthorCount.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(AuthorCount, new Path(mb7TF));
        FileOutputFormat.setOutputPath(AuthorCount, new Path(mb7AUTHORCOUNT));

        AuthorCount.waitForCompletion(true);



        //IDF job
        DistributedCache.addCacheFile(new Path("/7PA2outputAuthorCount/part-r-00000").toUri(), conf);
        Job IDF = Job.getInstance(conf);
        IDF.setJarByClass(MainClass.class);
        IDF.setMapperClass(IDFmapper.class);
        IDF.setReducerClass(IDFreducer.class);
        IDF.addCacheArchive(new Path("/7PA2outputAuthorCount/part-r-00000").toUri());
        IDF.setOutputKeyClass(Text.class);
        IDF.setOutputValueClass(Text.class);
        IDF.setInputFormatClass(TextInputFormat.class);
        IDF.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(IDF, new Path(mb7TF));
        FileOutputFormat.setOutputPath(IDF, new Path(mb7IDFOUT));

        IDF.waitForCompletion(true);

        //IDF job
        Job AAV = Job.getInstance(conf);
        //AAV.setNumReduceTasks(20);
        AAV.setJarByClass(MainClass.class);
        AAV.setMapperClass(TFIDFmapper.class);
        AAV.setReducerClass(TFIDFreducer.class);
        AAV.setOutputKeyClass(Text.class);
        AAV.setOutputValueClass(Text.class);
        AAV.setInputFormatClass(TextInputFormat.class);
        AAV.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(AAV, new Path(mb7IDFOUT));
        FileOutputFormat.setOutputPath(AAV, new Path(mb7TFIDFOUT));


        AAV.waitForCompletion(true);



    }

}
