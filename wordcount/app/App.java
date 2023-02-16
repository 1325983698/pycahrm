package com.qst.mapreduce.wordcount.app;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class App implements Writable {
    //属性
    private int count;
    private int time;
    private int consume;

    //get()和set()方法
    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int getTime() {
        return time;
    }

    public void setTime(int time) {
        this.time = time;
    }

    public int getConsume() {
        return consume;
    }

    public void setConsume(int consume) {
        this.consume = consume;
    }

    //重写WriteInt方法
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(count);
        dataOutput.writeInt(time);
        dataOutput.writeInt(consume);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        count = dataInput.readInt();
        time = dataInput.readInt();
        consume = dataInput.readInt();
    }

    //重写toString()方法
    @Override
    public String toString() {
        return count + "\t" + time + "\t" + consume;
    }
}