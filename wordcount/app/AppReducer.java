package com.qst.mapreduce.wordcount.app;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AppReducer extends Reducer<Text, App,Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<App> values, Context context) throws IOException, InterruptedException {
        int num = 0;
        int countSum = 0 ;
        int TimeSum = 0 ;
        for (App value : values) {
            num++;
            countSum+=value.getCount();
            TimeSum+=value.getTime();
        }
        Text outKey=key;
        Text outValue =
                new Text(countSum/num+"\t"+TimeSum/num);
        context.write(outKey,outValue);
    }
}

