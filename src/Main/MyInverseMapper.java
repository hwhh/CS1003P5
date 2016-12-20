package Main;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;



class MyInverseMapper extends Mapper<Text, Text, LongWritable, Text> {

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        //Maps the words to the counts
        context.write(new LongWritable(Long.parseLong(value.toString())),key);
    }


}
