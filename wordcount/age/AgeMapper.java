package com.qst.mapreduce.wordcount.age;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AgeMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行数据
        String line = value.toString();
        //拆分成单词数组
        String[] wordList = line.split(",");
        int age = Integer.parseInt(wordList[2]);
        //计算全体学生的平均年龄
//        Text outKey = new Text("age");
        //分别计算男生、女生的平均年龄
        Text outKey = new Text(wordList[3]);
        IntWritable outValue = new IntWritable(age);
        context.write(outKey,outValue);
    }
}
