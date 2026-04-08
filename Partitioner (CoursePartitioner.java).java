package com.mr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CoursePartitioner extends Partitioner<Text, Text> {

    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {

        String course = key.toString();

        if (course.equalsIgnoreCase("Math"))
            return 0 % numReduceTasks;
        else if (course.equalsIgnoreCase("Science"))
            return 1 % numReduceTasks;
        else
            return 2 % numReduceTasks;
    }
}