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

public class ptt {
    public static class WordCountMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        //計數使用，設定為1。每當找到相同的字就會+1。
        private final static IntWritable plugOne  = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            //使用StringTokenizer效能會比使用split好。預設使用空白、tab或是換行當作分隔符號。
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


    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        Job job = Job.getInstance(config, "ptt word count");
        job.setJarByClass(ptt.class);
        job.setReducerClass(WordCountReducer.class);
        job.setMapperClass(WordCountMapper.class);
        //設定setCombinerClass後，每個mapper會在sorting後，對結果先做一次reduce
        job.setCombinerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //執行程式時，第一個參數（agrs[0]）為欲計算檔案路徑
        FileInputFormat.addInputPath(job, new Path(args[0]));
        //第二個參數（agrs[1]）為計算結果存放路徑
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}