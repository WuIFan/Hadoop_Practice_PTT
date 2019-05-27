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

import org.apache.hadoop.io.WritableComparator;

public class ptt {
    public static class WordCountMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable plugOne  = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            // StringTokenizer st = new StringTokenizer(value.toString());
            // while (st.hasMoreTokens()) {
            //     word.set(st.nextToken());
            //     context.write(word, plugOne);
            // }
            String[] toSplit = value.toString().split(",");
            String toSeg = toSplit[1];
            String[] date = toSplit[0].split(" ");
            String dateFormat = date[0] + " " + date[1] + " " + date[2] + " " + date[4] + " " ;
            // String toSeg = value.toString().split(",")[1];
            List<String> list = segment(toSeg);
            for (String l : list) {
                word.set(dateFormat + l);
                context.write(word,plugOne);
            }
        }
    }

    public static List<String> segment(String str) throws IOException{
        //byte[] byt = str.getBytes();
        //InputStream is = new ByteArrayInputStream(byt);
        StringReader reader = new StringReader(str);
        IKSegmenter iks = new IKSegmenter(reader,true);
        //iks.reset(reader);
        Lexeme lexeme;
        List<String> list = new ArrayList<String>();
        while((lexeme = iks.next()) != null){
            String text = lexeme.getLexemeText();
            list.add(text);
        }
        return list;
    }

    public static class WordCountReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {

        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int reduceSum = 0;
            for (IntWritable val : values) {
                reduceSum += val.get();
            }
            if (reduceSum > 2) {
                result.set(reduceSum);
                System.out.println(key);
                context.write(key, result);
            } else {

            }
            
        }
    }

    public static class SortMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable plugOne  = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            
            String[] toSplit = value.toString().split("\t");
            word.set(toSplit[0] + "2");
            int num = Integer.valueOf(toSplit[1]);
            context.write(word,new IntWritable(num));
        }
    }

    public static class SortReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {

        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
                context.write(key, result);
        }
    }

    public static class DescComparator extends WritableComparator{
 
		protected DescComparator() {
			super(IntWritable.class,true);
		}
 
		@Override
		public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3,
				int arg4, int arg5) {
			return -super.compare(arg0, arg1, arg2, arg3, arg4, arg5);
		}
		@Override
		public int compare(Object a,Object b){
			return -super.compare(a, b);
		}
	}

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        Job job = Job.getInstance(config, "ptt word count");
        job.setJarByClass(ptt.class);
        job.setReducerClass(WordCountReducer.class);
        job.setMapperClass(WordCountMapper.class);

        // job.setCombinerClass(WordCountReducer.class);
        // job.setSortComparatorClass(DescComparator.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //job 2
        Job job2 = Job.getInstance(config, "ptt sort");
        job2.setJarByClass(ptt.class);
        job2.setReducerClass(SortReducer.class);
        job2.setMapperClass(SortMapper.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        if (job.waitForCompletion(true)){
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }  
    }
}