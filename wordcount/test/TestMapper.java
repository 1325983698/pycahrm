package com.qst.mapreduce.wordcount.test;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TestMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行数据
        String Line = value.toString();
        //拆分成单词数组
        String[] wordList = Line.split(" ");
        for (String word : wordList) {
            Text outKey = new Text(word);
            IntWritable outValue = new IntWritable(1);
            context.write(outKey,outValue);
        }
    }

}
