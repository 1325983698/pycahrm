package com.qst.mapreduce.wordcount.wordcount;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {

        // 1 累加求和
        int sum=0;
        for (IntWritable count : values) {
            sum += count.get();
        }

        // 2 输出
        IntWritable v = new IntWritable();
        v.set(sum);
        context.write(key,v);
    }
}