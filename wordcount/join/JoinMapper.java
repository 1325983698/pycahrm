package com.qst.mapreduce.wordcount.join;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class JoinMapper extends Mapper<LongWritable, Text,Text, JoinApp> {
    HashMap<String,String> nameMap=new HashMap();

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, JoinApp>.Context context) throws IOException, InterruptedException {
        //通过缓存文件得到姓名文件地址
        URI[] cacheFiles = context.getCacheFiles();
        Path path = new Path(cacheFiles[0]);
        //获取文件系统对象,并开流
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fs.open(path);
        //通过包装流转换为reader,方便按行读取
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
        //逐行读取，按行处理
        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            String[] split = line.split("\t");
            nameMap.put(split[0], split[1]);
        }
        //关闭流
        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行数据
        String line = value.toString();
        //拆分成单词数组
        String[] wordList = line.split("\t");
        String id = wordList[1];
        String name = nameMap.get(id);
        Text outKey = new Text(wordList[2]);
        JoinApp app = new JoinApp();
        app.setCount(Integer.parseInt(wordList[3]));
        app.setTime(Integer.parseInt(wordList[4]));
        app.setConsume(Integer.parseInt(wordList[5]));
        app.setName(name);
        context.write(outKey,app);
    }
}

