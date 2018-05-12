package step1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MapReduce1 {
    private static final String IN_PATH = "/ItemCF/step1_input/ActionList";
    private static final String OUT_PATH = "/ItemCF/step1_output";
    private static final String HDFS_ADDR = "hdfs://localhost:9000";

    public int run() {
        try {
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS", HDFS_ADDR);

            // create a job instance
            Job job = Job.getInstance(configuration, "step1");

            // 设置Job的主类
            job.setJarByClass(MapReduce1.class);

            // 设置Job的Mapper类和Reducer类
            job.setMapperClass(Mapper1.class);
            job.setReducerClass(Reducer1.class);

            // 设置Mapper的输出类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            // 设置Reducer的输出类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileSystem fs = FileSystem.get(configuration);
            Path inputPath = new Path(IN_PATH);
            // 设置输入路径
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
        }

        return -1;
    }

    public static void main(String[] args) {
        int result = new MapReduce1().run();

        if (result == 1) {
            /**
             1	A_2,C_5
             2	A_10,B_3
             3	C_15
             4	A_3,C_5
             5	B_3
             6	A_5,B_5
             */
            System.out.println("step1 runs successfully");
        } else if (result == -1) {
            System.out.println("step1 runs failed");
        }
    }
}
