package step4;

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

public class MapReduce4 {
    private static final String IN_PATH = "/ItemCF/step2_output";
    private static final String OUT_PATH = "/ItemCF/step4_output";
    private static final String CACHE = "/ItemCF/step3_output/part-r-00000";
    private static final String HDFS_ADDR = "hdfs://localhost:9000";

    public int run() {
        try {
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS", HDFS_ADDR);

            // 创建一个job实例
            Job job = Job.getInstance(configuration, "step4");

            // 添加分布式缓存
            job.addCacheArchive(new URI(CACHE + "#itemUserScore2"));

            // 设置Job的主类
            job.setJarByClass(MapReduce4.class);

            // 设置Job的Mapper类和Reducer类
            job.setMapperClass(Mapper4.class);
            job.setReducerClass(Reducer4.class);

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
            // 如果文件路径存在，则删除
            fs.delete(outputPath, true);
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
        result = new MapReduce4().run();

        if (result == 1) {
            /**
             1	A_9.87,B_2.38,C_23.90
             2	A_16.59,B_8.27,C_4.25
             3	C_23.95,A_4.44
             4	B_3.27,C_22.85,A_11.68
             5	A_6.45,B_7.42
             6	C_3.10,A_15.40,B_9.77
             */
            System.out.println("step4 runs successfully");
        } else if (result == -1) {
            System.out.println("step4 runs failed");
        }
    }
}
