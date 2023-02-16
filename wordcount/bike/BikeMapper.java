package com.qst.mapreduce.wordcount.bike;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BikeMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行数据
        String line = value.toString();
        //拆分成单词数组
        String[] wordList = line.split(",");
        String[] timeList = wordList[4].split(" ");
        String[] dayTimeList = timeList[0].split("-");

        Text outKey = new Text(timeList[0]);
        IntWritable outValue = new IntWritable(1);
        context.write(outKey,outValue);

    }

}
