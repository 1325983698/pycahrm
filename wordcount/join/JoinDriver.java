package com.qst.mapreduce.wordcount.join;

import com.qst.mapreduce.wordcount.app.AppReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class JoinDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        // 1 获取配置信息以及获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 关联本Driver程序的jar
        job.setJarByClass(JoinDriver.class);

        // 3 关联Mapper和Reducer的jar
        job.setMapperClass(JoinMapper.class);
//        job.setReducerClass(AppReducer.class);

        // 4 设置Mapper输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(JoinApp.class);

        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 6 设置输入和输出路径
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileInputFormat.setInputPaths(job, new Path("C:\\Users\\hp\\MapReduceDemo\\input\\app.txt"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\hp\\MapReduceDemo\\output"));

        //设置缓存文件路径
        job.addCacheFile(new URI("file:///C:/Users/hp/MapReduceDemo/input/name.txt"));

        // 7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}

