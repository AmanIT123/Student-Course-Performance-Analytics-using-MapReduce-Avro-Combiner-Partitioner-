package com.mr;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class StudentMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] fields = value.toString().split(",");

        if (fields.length == 3) {
            context.write(new Text(fields[1]), new Text(fields[2]));
        }
    }
}