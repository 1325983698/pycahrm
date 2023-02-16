package com.qst.mapreduce.wordcount.student;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class StudentMapper extends Mapper<LongWritable, Text,Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行数据
        String line = value.toString();
        //拆分成单词数组
        String[] wordList = line.split(",");
        String name = wordList[1];

//        where语言就是在Mapper里添加if判断
        if(name.contains("a")){  //姓名中是否包含"a"
            IntWritable outValue = new IntWritable(1);
            context.write(value,new Text(""));
        }
    }
}
