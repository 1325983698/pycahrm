package com.qst.mapreduce.wordcount.app;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AppMapper extends Mapper<LongWritable, Text,Text, App> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行数据
        String line = value.toString();
        //拆分成单词数组
        String[] wordList = line.split("\t");
        Text outKey = new Text(wordList[2]);
        App app = new App();
        app.setCount(Integer.parseInt(wordList[3]));
        app.setTime(Integer.parseInt(wordList[4]));
        app.setConsume(Integer.parseInt(wordList[5]));
        context.write(outKey,app);
    }
}
