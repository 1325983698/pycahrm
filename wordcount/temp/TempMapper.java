package com.qst.mapreduce.wordcount.temp;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TempMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行数据
        String line = value.toString();
        //拆分成单词数组
        String[] wordList = line.split(",");
        int temp = Integer.parseInt(wordList[2]);
        if(temp>30){
//            Text outKey = new Text(wordList[2]);
            Text outKey = new Text("total");  //把所有气温聚合起来，输出一共有多少天
            IntWritable outValue = new IntWritable(1);
            context.write(outKey,outValue);
        }
    }

}
