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
    public static void main(String[] args) throws IOException, ClassNotFoundException,
            InterruptedException {
        if (args.length != 5) {
            System.out.printf("Usage: <jar file> <input dir> <1st output dir> <2nd output directory> <3rd output> <4th>\n");
            System.exit(-1);
        }
        Configuration conf =new Configuration();
        Job job=Job.getInstance(conf);
        job.setJarByClass(MainClass.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        //TF job
        Job job1 = Job.getInstance(conf);
        job1.setJarByClass(MainClass.class);
        job1.setMapperClass(AuthorWordCountMapper.class);
        job1.setReducerClass(AuthorWordCountReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job1, new Path(args[1]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        //MultipleOutputs.addNamedOutput(job1, "AuthorCount", TextOutputFormat.class, job.getOutputKeyClass(), job.getOutputValueClass());
        //MultipleOutputs.addNamedOutput(job1, "TFvalue" , TextOutputFormat.class, job.getOutputKeyClass(), job.getOutputValueClass());

        job1.waitForCompletion(true);

        //IDF job
        Job job3 = Job.getInstance(conf);
        job3.setJarByClass(MainClass.class);
        job3.setMapperClass(AuthorCountMapper.class);
        job3.setReducerClass(AuthorCountReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job3, new Path(args[2]));
        FileOutputFormat.setOutputPath(job3, new Path(args[3]));

        job3.waitForCompletion(true);



        //IDF job
        Job job2 = Job.getInstance(conf);
        job2.setJarByClass(MainClass.class);
        job2.setMapperClass(IDFmapper.class);
        job2.setReducerClass(IDFreducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2, new Path(args[3]));
        FileOutputFormat.setOutputPath(job2, new Path(args[4]));

        job2.waitForCompletion(true);



    }

}
