package com.mr;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class StudentReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        int count = 0;
        int max = Integer.MIN_VALUE;

        for (Text val : values) {
            int marks = Integer.parseInt(val.toString());
            sum += marks;
            count++;
            max = Math.max(max, marks);
        }

        double avg = (double) sum / count;

        context.write(key, new Text("Avg=" + avg + ", Max=" + max));
    }
}