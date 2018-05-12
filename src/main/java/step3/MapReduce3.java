package step3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 将评分矩阵进行转置
 */
public class MapReduce3 {
    private static final String IN_PATH = "/ItemCF/step1_output";
    private static final String OUT_PATH = "/ItemCF/step3_output";
    private static final String HDFS_ADDR = "hdfs://localhost:9000";

    public int run() {
        try {
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS", HDFS_ADDR);

            // 创建一个job实例
            Job job = Job.getInstance(configuration, "step3");

            // 设置Job的主类
            job.setJarByClass(MapReduce3.class);

            // 设置Job的Mapper类和Reducer类
            job.setMapperClass(Mapper3.class);
            job.setReducerClass(Reducer3.class);

            // 设置Mapper的输出类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            // 设置Reducer的输出类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            // 设置输入路径
            FileSystem fs = FileSystem.get(configuration);
            Path inputPath = new Path(IN_PATH);
            if (fs.exists(inputPath)) {
                FileInputFormat.addInputPath(job, inputPath);
            }

            // 设置输出路径
            Path outputPath = new Path(OUT_PATH);
            fs.delete(outputPath, true);
            FileOutputFormat.setOutputPath(job, outputPath);

            return job.waitForCompletion(true) ? 1 : -1;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        return -1;
    }

    /**
     1	A_2,C_5
     2	A_10,B_3
     3	C_15
     4	A_3,C_5
     5	B_3
     6	A_5,B_5
     */
    public static void main(String[] args) {
        int result = -1;
        result = new MapReduce3().run();

        if (result == 1) {
            /**
             A	6_5,4_3,2_10,1_2
             B	6_5,5_3,2_3
             C	4_5,3_15,1_5
             */
            System.out.println("step3 runs successfully");
        } else if (result == -1) {
            System.out.println("step3 runs failed");
        }
    }
}
