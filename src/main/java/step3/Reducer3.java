package step3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reducer3 extends Reducer<Text, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();

    /**
     * key 输入给reducer的行号（真实矩阵的列）
     * values [行号_值，行号_值，行号_值，行号_值...]
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder stringBuilder = new StringBuilder();
        for (Text text: values) {
            stringBuilder.append(text + ",");
        }

        String line = null;
        if (stringBuilder.toString().endsWith(",")) {
            line = stringBuilder.substring(0, stringBuilder.toString().length()-1);
        }

        outKey.set(key);
        outValue.set(line);
        context.write(outKey, outValue);
    }
}
