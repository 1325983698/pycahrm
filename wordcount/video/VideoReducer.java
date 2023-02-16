package com.qst.mapreduce.wordcount.video;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

// 定义一个Reduce类继承Reducer父类
public class VideoReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    // Reduce接收Map传入的 (被写入context的) <key,value>对
    // 输入Reduce前，Shuffle阶段已经根据key的字母进行排序了
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int counter = 0;  // 初始化counter
        // 遍历key(category)相同的values
        for (IntWritable val : values) {
            counter += val.get();  // 累计同一category的数量
        }
        // 将统计结果写入context (即HDFS)
        context.write(key, new IntWritable(counter));
    }
}
