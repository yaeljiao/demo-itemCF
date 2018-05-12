package step3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Mapper3 extends Mapper<LongWritable, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] rowAndLine = value.toString().split("\t");

        String rowNum = rowAndLine[0];
        System.out.println(rowNum);
        String[] lines = rowAndLine[1].split(",");
        System.out.println(lines);

        for (int i = 0; i < lines.length; i++) {
            String column = lines[i].split("_")[0];
            String valueStr = lines[i].split("_")[1];
            // 进行转置，将列作为key
            outKey.set(column);
            outValue.set(rowNum + "_" + valueStr);
            context.write(outKey, outValue);
        }
    }
}
