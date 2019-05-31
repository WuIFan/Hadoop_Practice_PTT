import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.UnsupportedEncodingException;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.util.ArrayList;
import java.util.List;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;

public class ptt_sort {
    public static class SortMapper
            extends Mapper<Object, Text, IntWritable, Text>{

        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            
            String[] toSplit = value.toString().split("\t");


            // String data = toSplit[0].split(" ")[4];
            // word.set(data);
            word.set(toSplit[0]);
            IntWritable num = new IntWritable(Integer.parseInt(toSplit[1]));
            context.write(num,word);
        }
    }

    public static class SortReducer
            extends Reducer<IntWritable,Text,Text,IntWritable> {

            private IntWritable result = new IntWritable();

        //@Override
        public void reduce(Iterable<IntWritable> key,Text  values,
                           Context context
        ) throws IOException, InterruptedException {
            int reduceSum = 0;
            for (IntWritable k : key) {
                reduceSum += k.get();
            }
            result.set(reduceSum);
            context.write(values,result);
            // for(Text value : values){
                // context.write(values, key);
            // }
        }
    }

    // public class hot implements WritableComparable<hot> {

    //     private String date;
    //     private String title;
    //     private int num;

    //     public hot(String date,String title,Int num) {
    //         this.date = date;
    //         this.title = title;
    //         this.num = num;
    //     }

    //     public void set(String date,String title,Int num) {
    //         this.date = date;
    //         this.title = title;
    //         this.num = num;
    //     }
    // }


    // public class IntWritableDecreasingComparator extends Comparator {
    //     @SuppressWarnings("rawtypes")
    //     public int compare( WritableComparable a,WritableComparable b){
    //         return -super.compare(a, b);
    //     }
    //     public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    //         return -super.compare(b1, s1, l1, b2, s2, l2);
    //     }
    // }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        // Job job = Job.getInstance(config, "ptt word count");
        // job.setJarByClass(ptt.class);
        // job.setReducerClass(WordCountReducer.class);
        // job.setMapperClass(WordCountMapper.class);

        // // job.setCombinerClass(WordCountReducer.class);
        // // job.setSortComparatorClass(DescComparator.class);

        // job.setOutputKeyClass(Text.class);
        // job.setOutputValueClass(IntWritable.class);

        // FileInputFormat.addInputPath(job, new Path(args[0]));
        // FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //job 2
        Job job2 = Job.getInstance(config, "ptt sort");
        job2.setJarByClass(ptt_sort.class);
        job2.setReducerClass(SortReducer.class);
        job2.setMapperClass(SortMapper.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        //sort
        // job2.setSortComparatorClass(IntWritableDecreasingComparator.class);
        //job2.setSortComparatorClass(DescComparator.class);

        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);

        // if (job.waitForCompletion(true)){
        //     System.exit(job2.waitForCompletion(true) ? 0 : 1);
        // }  
    }
}