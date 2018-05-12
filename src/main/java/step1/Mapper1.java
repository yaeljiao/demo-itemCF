package step1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();

    /**
     * A,1,点击
     * C,3,收藏
     * B,2,搜索
     * B,5,搜索
     * B,6,收藏
     * A,2,付款
     * C,3,付款
     * C,4,收藏
     * C,1,收藏
     * A,1,点击
     * A,6,收藏
     * A,4,搜索
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(",");
        String userID = values[0];
        String itemID = values[1];
        String score = values[2];

        /**
         * key: itemID
         * value: userID_score
         */
        outKey.set(itemID);
        outValue.set(userID + "_" + score);
        context.write(outKey, outValue);
    }
}
