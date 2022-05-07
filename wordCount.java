package Assignment03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import java.util.*;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
//mapper and reducer are class
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class wordCount extends Configured implements Tool {

    /**
     * defines the Map job. Maps input key-value pairs to a set of intermediate key-value pairs
    * Mapper -map each word of file to 1
     *
     * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
     **/

    /**
     * frameworks convert input into key,value pair for mapper
     * key,value (first word character position,whole line)
     * 0,arya sansa john
     * 16,arya sansa john
     * 33,arya sansa john
     * 47,arya sansa john
     *
     *
     */
    public static class Map extends Mapper<LongWritable, Text , Text , IntWritable > {


        final static IntWritable one = new IntWritable(1);

        //The Text class defines a node that displays a text. Paragraphs are separated
        // by '\n' and the text is wrapped on paragraph boundaries.
        Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //convert doc into bunch of string
            //there was no mention of changing string to lowercase
            //i saw some example on internet where they did

            //value contains line which is in text format
            //key,value->(Long,String)
            //one mapper doesn't talk to another .its independent
            //128 block size
            String l = value.toString()
                    .replace('\"', ' ')
                    .replace('!', ' ')
                    .replace('(', ' ')
                    .replace('-', ' ')
                    .replace('.', ' ')
                    .replace('[',' ')
                    .replace(']',' ')
                    .replace('\'', ' ')
                    .replace('?', ' ')
                    .replace('=', ' ')
                    .replace(')', ' ')
                    .replace(';', ' ')
                    .replace(',',' ');

            Set<String> tokensSet=new HashSet<String>();

            //divide each line string into word
            StringTokenizer tokenizer = new StringTokenizer(l);

            while (tokenizer.hasMoreTokens()) {
                //System.out.println("word" +word+one);
                //write into set
                tokensSet.add(tokenizer.nextToken());

            }
            for (String token: tokensSet)
            {
                word.set(token);
                // give pair of words ,1
                // context is used to supply information to newly created object, common resources or member functions.
                //context is output collector in mapreduce
                //similar to system.out
                context.write(word, one);
            }
        }
    }

    /**
     * It reduces a set of intermediate values that share a key to a smaller set of values
     * Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
     */

    public static class Reduce extends Reducer<Text ,IntWritable , Text , IntWritable >{
        @Override
        public void reduce( Text key , Iterable <IntWritable> values, Context context) throws IOException ,
                InterruptedException{
            int sum = 0;

            for(IntWritable val : values)
            {

                //sum for each word
                sum +=val.get();
            }
            context.write(key , new IntWritable(sum));
        }
    }



    @Override
    public int run(String[] args) throws Exception
    {

        //will read hadoop configuration
        //
        Configuration conf = getConf();

        //configure job
        //this job responsilbe for running program
        Job job = Job.getInstance(conf, "word count");
        //main class
        job.setJarByClass(wordCount.class);
        job.setMapperClass(wordCount.Map.class);
        //Text.class-writer class
        //driver needs to know
        //by default it could be 3 reducer
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(wordCount.Reduce.class);

        //below 2 line could be used by mapper too if the data type for key and value
        //is same for mapper too
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path output=new Path(args[1]);
        Path input=new Path(args[0]);

        //delete output file before starting job to write into file
        output.getFileSystem(conf).delete(output,true);


        //set path for input and output file
        //FileInputFormat-file format
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception
    {
        //run in hadoop configuration
        ToolRunner.run(new Configuration(),new wordCount(), args);
    }

}
