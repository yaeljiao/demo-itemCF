package step2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MapReduce2 {
    /**
     * 输入是第一步的输出
     1	A_2,C_5
     2	A_10,B_3
     3	C_15
     4	A_3,C_5
     5	B_3
     6	A_5,B_5
     */
    private static final String IN_PATH = "/ItemCF/step1_output/";
    private static final String OUT_PATH = "/ItemCF/step2_output";
    private static final String CACHE_PATH = "/ItemCF/step1_output/part-r-00000";
    private static final String HDFS_ADDR = "hdfs://localhost:9000";

    private int run() {
        try {
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS", HDFS_ADDR);

            // 创建一个job实例
            Job job = Job.getInstance(configuration, "step2");

            // 添加分布式缓存
            job.addCacheFile(new URI(CACHE_PATH + "#itemUserScore1"));

            // 设置Job的主类
            job.setJarByClass(MapReduce2.class);

            // 设置Job的Mapper类和Reducer类
            job.setMapperClass(Mapper2.class);
            job.setReducerClass(Reducer2.class);

            // 设置Mapper的输出类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            // 设置Reducer的输出类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileSystem fs = FileSystem.get(configuration);

            Path inputPath = new Path(IN_PATH);
            if (fs.exists(inputPath)) {
                FileInputFormat.addInputPath(job, inputPath);
            }

            Path outputPath = new Path(OUT_PATH);
            fs.delete(outputPath, true);

            // 设置输出路径
            FileOutputFormat.setOutputPath(job, outputPath);

            return job.waitForCompletion(true) ? 1 : -1;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        return -1;
    }

    public static void main(String[] args) {
        int result = -1;
        result = new MapReduce2().run();

        if (result == 1) {
            /**
             1	1_1.00,2_0.36,3_0.93,4_0.99,6_0.26
             2	1_0.36,2_1.00,4_0.49,5_0.29,6_0.88
             3	4_0.86,3_1.00,1_0.93
             4	1_0.99,4_1.00,6_0.36,3_0.86,2_0.49
             5	2_0.29,5_1.00,6_0.71
             6	1_0.26,5_0.71,6_1.00,2_0.88,4_0.36
             */
            System.out.println("step2 runs successfully");
        } else if (result == -1) {
            System.out.println("step2 runs failed");
        }
    }
}
