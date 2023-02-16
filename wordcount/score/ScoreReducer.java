package com.qst.mapreduce.wordcount.score;

import com.qst.mapreduce.wordcount.app.App;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ScoreReducer extends Reducer<Text, Text,Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(new Text(key), new Text(value));
        }
    }
}

