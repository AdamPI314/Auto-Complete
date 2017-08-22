import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Driver {
    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
        String inputDir = args[0];
        String nGramOutputDir = args[1];
        String numGram = args[2];
        String frequencyThreshold = args[3];
        String numOfFollowingWords = args[4];

        // job1, parse NGram and save to HDFS
        Configuration configuration1 = new Configuration();
        // Read sentence by sentence
        configuration1.set("textinputformat.record.delimiter", ".");
        configuration1.set("numGram", numGram);

        Job job1 = Job.getInstance(configuration1);
        job1.setJobName("NGram");
        job1.setJarByClass(Driver.class);

        job1.setMapperClass(NGram.NGramMapper.class);
        job1.setReducerClass(NGram.NGramReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(job1, new Path(inputDir));
        TextOutputFormat.setOutputPath(job1, new Path(nGramOutputDir));

        job1.waitForCompletion(true);


        // connect two jobs, previous job's output is current job's input
        Configuration configuration2 = new Configuration();
        configuration2.set("threshold", frequencyThreshold);
        configuration2.set("n", numOfFollowingWords);

        // Use dbConfiguration to configure all the jdbcDriver, db user, db password, database
        DBConfiguration.configureDB(configuration2,
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://128.138.143.161:3306/test",
                "root",
                "bigdata");

        Job job2 = Job.getInstance(configuration2);
        job2.setJobName("WordFollowingWordCount");
        job2.setJarByClass(Driver.class);

        //How to add external dependency to current project?
        /*
          1. upload dependency to hdfs
          2. use this "addArchiveToClassPath" method to define the dependency path on hdfs
         */
        job2.addArchiveToClassPath(new Path("/mysql/mysql-connector-java-5.1.39-bin.jar"));
        
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);        
        job2.setOutputKeyClass(DBOutputWritable.class);
        job2.setOutputValueClass(NullWritable.class);
        
        job2.setMapperClass(autoCompleteModel.AcmMapper.class);
        job2.setReducerClass(autoCompleteModel.AcmReducer.class);
        
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(DBOutputFormat.class);

        TextInputFormat.setInputPaths(job2, nGramOutputDir);
        DBOutputFormat.setOutput(job2,
                "output",
                new String[]{"phrase_1", "phrase_2", "count"});

        job2.waitForCompletion(true);

    }
}
