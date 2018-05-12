package step4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

public class Mapper4 extends Mapper<LongWritable, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();
    private List<String> cacheList = new ArrayList<String>();

    private DecimalFormat decimalFormat = new DecimalFormat("0.00");

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        FileReader fr = new FileReader("itemUserScore2");
        BufferedReader br = new BufferedReader(fr);

        String line = null;
        while ((line = br.readLine()) != null) {
            cacheList.add(line);
        }

        fr.close();
        br.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String rowOfMatrix1 = value.toString().split("\t")[0];
        String[] columnValueArrayOfMatrix1 = value.toString().split("\t")[1].split(",");

        for (String line : cacheList) {
            String rowOfMatrix2 = line.split("\t")[0];
            String[] columnValueArrayOfMatrix2 = line.split("\t")[1].split(",");

            double result = 0;

            for (String columnValueOfMatrix1: columnValueArrayOfMatrix1) {
                String columnMatrix1 = columnValueOfMatrix1.split("_")[0];
                String valueMatrix1 = columnValueOfMatrix1.split("_")[1];

                //
                for (String columnValueOfMatrix2: columnValueArrayOfMatrix2) {
                    if (columnValueOfMatrix2.startsWith(columnMatrix1 + "_")) {
                        String valueMatrix2 = columnValueOfMatrix2.split("_")[1];
                        //
                        result += Double.valueOf(valueMatrix1) * Double.valueOf(valueMatrix2);
                    }

                }
            }

            // 如果结果是0，则跳过循环
            if (result == 0) {
                continue;
            }

            outKey.set(rowOfMatrix1);
            outValue.set(rowOfMatrix2 + "_" + decimalFormat.format(result));
            context.write(outKey, outValue);
        }

    }
}

