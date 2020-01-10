package main.java; /**
 * @author 19028
 * @date 2020/1/10 11:03
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

public class MR1 {

    public static class MR1Mapper extends Mapper<Object, Text, Text, IntWritable> {

        /**
         * 1. 执行map任务
         * 2. 将读入的数据按\t分割，过滤掉脏数据
         * 3. 取出第四列, 输出
         */
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] lines = value.toString().split("\t");
            if (lines.length == 4) {
                String[] words = lines[3].split(" ");
                for (String word : words) {
                    context.write(new Text(word), new IntWritable(1));

                }
            }
        }
    }

    public static class MR1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        //创建map，用于存放结果并排序， 可以用queue代替
        Map<String, Integer> map = new HashMap<>();

        /**
         * 1. 接收map任务处理完的数据
         * 2. 对同一个key的value进行累加， 得到wordcount
         * 3. 输出最终结果
         */
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) {
            int sum = 0;
            for (IntWritable ignored : values) {
                sum++;
            }
            map.put(key.toString(), sum);
        }

        /**
         * 排序并输出
         * 获取map中的EntrySet，然后用Collections进行排序， 并只输出前100
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            List<Entry<String, Integer>> list = new ArrayList<>(map.entrySet());
            Collections.sort(list, (o1, o2) -> o2.getValue() - o1.getValue());
            for (Entry<String, Integer> stringIntegerEntry : list.subList(0, 100)) {
                context.write(new Text(stringIntegerEntry.getKey()),
                        new IntWritable(stringIntegerEntry.getValue()));
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        long start = System.currentTimeMillis();
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("fs.default.name", "hdfs://localhost:9000");
        conf.set("mapreduce.app-submission.cross-platform", "true");//跨平台提交

        Job job = Job.getInstance(conf);

        job.setJarByClass(MR1.class);

        job.setMapperClass(MR1Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job, new Path("hdfs://localhost:9000/usr/local/hadoop/input/"));

        job.setReducerClass(MR1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/usr/local/hadoop/output"));

        job.waitForCompletion(true);
        System.out.println((System.currentTimeMillis() - start));

    }
}

