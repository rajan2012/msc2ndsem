package Assignment03;

import java.util.ArrayList;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.LongWritable;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


/**
 * first mapper process all the line and then send it to reducer
 */


/**
 *
 * this is nested class
 * Terminology: Nested classes are divided into two categories: non-static and static. Non-static nested
 * classes are called inner classes. Nested classes that are declared static are called static nested classes.
 * class OuterClass {
 *     ...
 *     class InnerClass {
 *         ...
 *     }
 *     static class StaticNestedClass {
 *         ...
 *     }
 * }
 *
 * A nested class is a member of its enclosing class. Non-static nested classes (inner classes) have access
 * to other members of the enclosing class, even if they are declared private. Static nested classes do not have
 * access to other members of the enclosing class. As a member of the OuterClass, a nested class can be declared
 * private, public, protected, or package private. (Recall that outer classes can only be declared public or package private.)
 *
 *
 * To instantiate an inner class, you must first instantiate the outer class. Then, create the inner
 * object within the outer object with this syntax:
 *
 * OuterClass outerObject = new OuterClass();
 * OuterClass.InnerClass innerObject = outerObject.new InnerClass();
 *
 * As with class methods and variables, a static nested class is associated with its outer class. And like static
 * class methods, a static nested class cannot refer directly to instance variables or methods defined in its
 * enclosing class: it can use them only through an object reference
 *
 * WHEN SHADOWTEST IS TOP CLASS AND FIRSTLEVEL IS STATIC CLASS INSIDE IT
 *  public static void main(String... args) {
 *         ShadowTest st = new ShadowTest();
 *         ShadowTest.FirstLevel fl = st.new FirstLevel();
 *         fl.methodInFirstLevel(23);
 *     }
 *
 *
 * https://docs.oracle.com/javase/tutorial/java/javaOO/nested.html
 */


/**
 *
 * An inner class can also be static, which means that you can access it without creating an object of the outer class:
 *
 * Example
 * class OuterClass {
 *   int x = 10;
 *
 *   static class InnerClass {
 *     int y = 5;
 *   }
 * }
 *
 * public class Main {
 *   public static void main(String[] args) {
 *     OuterClass.InnerClass myInner = new OuterClass.InnerClass();
 *     System.out.println(myInner.y);
 *   }
 * }
 *
 *
 *  just like static attributes and methods, a static inner class does not have access to members of the outer class.
 *
 *
 * One advantage of inner classes IF NOT STATIC , is that they can access attributes and methods of the outer class:
 *
 * Example
 * class OuterClass {
 *   int x = 10;
 *
 *   class InnerClass {
 *     public int myInnerMethod() {
 *       return x;
 *     }
 *   }
 * }
 *
 * public class Main {
 *   public static void main(String[] args) {
 *     OuterClass myOuter = new OuterClass();
 *     OuterClass.InnerClass myInner = myOuter.new InnerClass();
 *     System.out.println(myInner.myInnerMethod());
 *   }
 * }
 *
 * meaning of static -This means we'll create only one instance of that static member that is shared across all instances of the class
 * , static variables are stored in the heap memory
 * Nested classes that we declare static are called static nested classes.
 * Nested classes that are non-static are called inner classes.
 *
 */




public class InvertexIndex extends Configured implements Tool {

    /**
     * defines the Map job. Maps input key-value pairs to a set of intermediate key-value pairs
     * Mapper -map each word of file to 1
     *
     * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
     *      input for mapper
     *      *    key: (0)  --key is 0 for first line then for 2nd line ,length of first line and likewise it gets added
     *      *    value: (1: "Hey, vitrivr!" - A Multimodal UI for Video Retrieval)
     *       1 in val is line number
     *
     * inpute key: 0
     * value:  1: "Hey, vitrivr!" - A Multimodal UI for Video Retrieval
     *  length of value:  56
     * inpute key: 58
     * value:  2: Inferring User Interests for Passive Users on Twitter by Leveraging Followee Biographies.
     *  length of value :  92
     * inpute key: 152
     * value:  3: Effective DBMS space management on native Flash
     *
     *
     * -----------------------------------------------------------------------------------
     *     each line of file is processed by mapper till all lines are done
     *     like here 1st line Hey  vitrivr     A Multimodal UI for Video Retrieval
     *     calc doc and word and all the process for 1st line
     *     Hey 1
     *     vitrivr 1
     *     A 1
     *     Multimodal 1
     *     UI 1
     *     for 1
     *     Video 1
     *     Retrieval 1
     *
     * then another line input
     *  inpute key 58
     *  value  2: Inferring User Interests for Passive Users on Twitter by Leveraging Followee Biographies.
     *     ##then 2nd line
     *      Inferring User Interests for Passive Users on Twitter by Leveraging Followee Biographies
     *
     *t's always good practice to refer to a generic type by specifying his specific type, by using Class<?>
     *     you're respecting this practice (you're aware of Class to be parameterizable) but you're not
     *     restricting your parameter to have a specific type.
     *
     *
     **/




    //IntWritable
    public static class Map extends Mapper<LongWritable, Text, Text , IntWritable> {



        final static IntWritable docid = new IntWritable(1);
        Text word = new Text();



        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

//           System.out.println("lines map inpute key: "+ key);
//           System.out.println("lines map value:  "+ value);
//           System.out.println("length  of value:  "+ value.toString().length());
//            System.out.println("lines context   "+ context);
            //convert doc into bunch of string
            //and replace special character
            String l=value.toString()
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


            //System.out.println("lines are"+ l);
            //System.out.println("lines after l");

            int index_pos=l.indexOf(':');
            //when there is empty line or no : in sentence
            if ((index_pos==-1)||(index_pos==l.length())) return;
            String docxid=l.substring(0, index_pos);
            String content=l.substring(index_pos+1);

            //trier is city

            //Returns an Integer instance holding the value of the specified parameter int i.
            docid.set(Integer.valueOf(docxid));

            //divide each line string into word
            StringTokenizer tokenizer = new StringTokenizer(content);

            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                //System.out.println("words and docid  are "+ word+ " " +docid);
                // give pair of words ,1
                // context is used to supply information to newly created object, common resources or member functions.
                //System.out.println("output for mapper "+ word+ " "+docid);
                context.write(word, docid);
            }
        }


    }

    /**
     * It reduces a set of docid that share a key to a smaller set of values when its size>3
     * Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
     *
     *     input for reducer
     * it takes  "A"  word as key and display all the value for a key
     *
     * 49
     *
     * 1
     *
     * 46
     *
     * 41
     *
     * 5
     *
     * 48
     *
     * 6
     *
     * 8
     *
     * 11
     *
     * 20
     *
     * ## iterates through all the value of key "A" , and do some operation like sum or list of all the elements
     * ##then 2nd key comes and like this till all the keys are over
     * process for one word ,write in file and then move to next word
     */

    public static class Reduce extends Reducer<Text ,IntWritable , Text , Text > {



        ArrayList<Integer> docText = new ArrayList<Integer>();

        Text result = new Text();




        @Override
        public void reduce( Text key , Iterable <IntWritable > values, Context context) throws IOException ,
                InterruptedException{


            //System.out.println("lines key "+ key);
            //System.out.println("lines after l "+ values);
            //System.out.println("after line");

            //Set<Integer> abc =new HashSet<Integer>();

            //System.out.println("inside reduce lines");
            //System.out.println("lines after l");


            for(IntWritable val : values)
            {

                //only add value in arraylist if its not available
                if(!docText.contains(val.get())) {
                    //System.out.println("val "+ val.get());
                    //System.out.println("after line");
                    docText.add(val.get());


                    //sort value in arraylist
                    Collections.sort(docText);
               }


            }
            if (docText.size()>3){
                //[1,2,3]

                //remove special character and give space after ','
                String docText_final=docText.toString().replace("[","").replace("]","").replace(",",", ");
                result.set(docText_final);
                //System.out.println("output for reducer "+ key+ " "+result);
                context.write(key , result);

            }
            //System.out.println("after line");
            //empty arraylist after each key
            docText.removeAll(docText);
        }
    }



    @Override
    public int run(String[] args) throws Exception
    {
        Configuration conf = getConf();

        //configure job
        Job job = Job.getInstance(conf, "Inverted Index");
        job.setJarByClass(InvertexIndex.class);
        job.setMapperClass(InvertexIndex.Map.class);
        job.setReducerClass(InvertexIndex.Reduce.class);
        //for mapper key and value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //for reducer key and value
        job.setOutputKeyClass(Text.class);
        //IntWritable is the Wrapper Class/Box Class in Hadoop similar
        // to Integer Class in Java. IntWritable is the Hadoop flavour of Integer,
        // which is optimized to provide serialization in Hadoop
        job.setOutputValueClass(IntWritable.class);
        //job.setOutputValueClass(NullWritable.class);

        Path output=new Path(args[1]);
        Path input=new Path(args[0]);

        //delete output file before starting job to write into file
        output.getFileSystem(conf).delete(output,true);


        //set path for input and output file
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception
    {
        ToolRunner.run(new Configuration(),new InvertexIndex(), args);
    }

}



