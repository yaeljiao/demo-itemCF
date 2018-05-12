package step1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Reducer1 extends Reducer<Text, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();

    /**
     * @param key     ex. key = 1
     * @param values  ex. values = [A_点击, B_付款, ...]
     * @param context
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String itemID = key.toString();

        Map<String, Integer> map = new HashMap<String, Integer>();

        for (Text text : values) {
            String userID = text.toString().split("_")[0];
            String score = text.toString().split("_")[1];

            if (map.get(userID) == null) {
                map.put(userID, Integer.valueOf(score));
            } else {
                Integer preScore = map.get(userID);
                map.put(userID, preScore + Integer.valueOf(score));
            }

        }

        StringBuilder sb = new StringBuilder();

        for (Map.Entry<String, Integer> entry: map.entrySet()) {
            String userID = entry.getKey();
            String score = String.valueOf(entry.getValue());
            sb.append(userID + "_" + score + ",");
        }

        String line = null;
        if (sb.toString().endsWith(",")) {
            line = sb.toString().substring(0, sb.toString().length() - 1);
        }

        outKey.set(itemID);
        outValue.set(line);
        context.write(outKey, outValue);
    }
}
