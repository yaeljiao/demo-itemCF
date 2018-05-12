package step5;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 步骤五的输入是步骤四的输出，缓存是步骤一的输出
 */
public class Mapper5 extends Mapper<LongWritable, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();
    private List<String> cacheList = new ArrayList<String>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        FileReader fr = new FileReader("itemUserScore3");
        BufferedReader br = new BufferedReader(fr);

        // 每一行的格式
        String line = null;
        while ((line = br.readLine()) != null) {
            cacheList.add(line);
        }

        fr.close();
        br.close();
    }

    /**
     1	A_9.87,B_2.38,C_23.90
     2	A_16.59,B_8.27,C_4.25
     3	C_23.95,A_4.44
     4	B_3.27,C_22.85,A_11.68
     5	A_6.45,B_7.42
     6	C_3.10,A_15.40,B_9.77
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String  itemMatrix1= value.toString().split("\t")[0];
        String[] userScoreArrayMatrix1 = value.toString().split("\t")[1].split(",");

        /**
         * cacheList为
         1  A_2,C_5
         2  A_10,B_3
         3  C_15
         4  A_3,C_5
         5  B_3
         6  A_5,B_5
         */
        for (String line : cacheList) {
            String itemMatrix2 = line.split("\t")[0];
            String[] userScoreArrayMatrix2 = line.split("\t")[1].split(",");

            if (itemMatrix1.equals(itemMatrix2)) {

                for (String userScoreMatrix1: userScoreArrayMatrix1) {
                    boolean flag = false;
                    String userMatrix1 = userScoreMatrix1.split("_")[0];
                    String scoreMatrix1 = userScoreMatrix1.split("_")[1];

                    for (String userScoreMatrix2: userScoreArrayMatrix2) {
                        String userMatrix2 = userScoreMatrix2.split("_")[0];
                        if (userMatrix1.equals(userMatrix2)) {
                            flag = true;
                        }
                    }

                    if (flag == false) {
                        outKey.set(userMatrix1);
                        outValue.set(itemMatrix1 + "_" + scoreMatrix1);
                        context.write(outKey, outValue);
                    }
                }
            }
        }

    }
}

