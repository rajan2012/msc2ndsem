package Assignment03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;


public class GrepCount extends Configured implements Tool {

    /**
     * defines the Map job. Maps input key-value pairs to a set of intermediate key-value pairs
     * Mapper -map each word of file to 1
     *
     * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
     **/

    public static class Map extends Mapper<LongWritable, Text , Text , NullWritable > {


        String toFInd ="MapReduce";
        final static IntWritable one = new IntWritable(1);
        Text docx = new Text();



        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

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




            int index_pos=l.indexOf(':');
            //when there is empty line or no : in sentence
            if ((index_pos==-1)||(index_pos==l.length())) return;
            String docxid=l.substring(0, index_pos).trim();
            String content=l.substring(index_pos+1);





            if(content.contains(toFInd)){
                //docxid where mapreduce is there
                //docx.set(docxid.concat(content));
                docx.set(docxid);
                // context is used to supply information to newly created object, common resources or member functions.
                context.write(docx, NullWritable.get());
            }
        }
    }




    @Override
    public int run(String[] args) throws Exception
    {
        Configuration conf = getConf();

        //configure job
        Job job = Job.getInstance(conf, "grep");
        job.setJarByClass(GrepCount.class);
        job.setMapperClass(GrepCount.Map.class);
        job.setNumReduceTasks(0);
        //job.setReducerClass(wordCount.Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);


        Path output=new Path(args[1]);
        Path input=new Path(args[0]);

        //delete output file before starting job to write into file
        output.getFileSystem(conf).delete(output,true);


        //set path for input and output file
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        //when all job finish then it is submitted to reducer
        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception
    {
        ToolRunner.run(new Configuration(),new GrepCount(), args);
    }

}
