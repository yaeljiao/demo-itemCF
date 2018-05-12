package step2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

public class Mapper2 extends Mapper<LongWritable, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();

    private List<String> cacheList = new ArrayList<String>();
    private DecimalFormat decimalFormat = new DecimalFormat("0.00");

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        FileReader fileReader = new FileReader("itemUserScore1");
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        String line = null;

        while ((line = bufferedReader.readLine()) != null) {
            cacheList.add(line);
        }

        bufferedReader.close();
        fileReader.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 左侧矩阵的行
        String rowOfMatrix1 = value.toString().split("\t")[0];
        // 左侧矩阵的 列_值
        String[] columnValueArrayOfMatrix1 = value.toString().split("\t")[1].split(",");

        // 计算左侧矩阵的空间距离
        double denominator1 = 0;
        for (String columnValue : columnValueArrayOfMatrix1) {
            String score = columnValue.split("_")[1];
            denominator1 += Double.valueOf(score) * Double.valueOf(score);
        }
        denominator1 = Math.sqrt(denominator1);

        for (String line : cacheList) {
            String rowOfMatrix2 = line.split("\t")[0];
            String[] columnValueArrayOfMatrix2 = line.split("\t")[1].split(",");

            // 计算右侧矩阵的空间距离
            double denominator2 = 0;
            for (String columnValue : columnValueArrayOfMatrix2) {
                String score = columnValue.split("_")[1];
                denominator2 += Double.valueOf(score) * Double.valueOf(score);
            }
            denominator2 = Math.sqrt(denominator2);


            // 计算分子
            int numerator = 0;

            for (String columnValueOfMatrix1 : columnValueArrayOfMatrix1) {
                String columnMatrix1 = columnValueOfMatrix1.split("_")[0];
                String valueMatrix1 = columnValueOfMatrix1.split("_")[1];

                for (String columnValueOfMatrix2 : columnValueArrayOfMatrix2) {
                    if (columnValueOfMatrix2.startsWith(columnMatrix1 + "_")) {
                        String valueMatrix2 = columnValueOfMatrix2.split("_")[1];

                        numerator += Integer.valueOf(valueMatrix1) * Integer.valueOf(valueMatrix2);
                    }

                }
            }

            // 计算余弦相似度
            double cos = numerator / (denominator1 * denominator2);
            if (cos == 0) {
                // 如果等于0,就跳出循环，不进行输出
                continue;
            }

            // result是结果矩阵中的某个元素，其行是rowOfMatrix1，列是rowOfMatrix2
            outKey.set(rowOfMatrix1);
            // 最终的结果保留两位小数
            outValue.set(rowOfMatrix2 + "_" + decimalFormat.format(cos));
            context.write(outKey, outValue);
        }
    }
}
