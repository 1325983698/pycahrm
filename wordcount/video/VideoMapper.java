package com.qst.mapreduce.wordcount.video;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.naming.Context;
import java.io.IOException;

// 定义一个Map类继承Mapper父类
public class VideoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    // 构造Map输出的Text类型key、IntWritable类型value=1对象
    private Text category = new Text();
    private final static IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // 将每行数据转为字符串后，以“\ t”作为分隔符来分割每行字符串
        // 然后将分割后的各元素存储在字符串数组中
        String[] line = value.toString().split("\t");
        // 只记录有意义的数据
        if (line.length > 7) {  // 滤除数据集中不符合要求的log信息
            category.set(line[3]);  // 指定视频类型(category)为Map输出的key
        }
        // 将Map输出<key, value>=<category，one>写入context (即Reduce)
        context.write(category, one);
    }
}
