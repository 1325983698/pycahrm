package com.qst.mapreduce.wordcount.bike;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BikeReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
//        每个最高温度出现的天数
        int count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }


        Text outKey = key;

        IntWritable outValue = new IntWritable(count);
        context.write(outKey,outValue);
    }
}
