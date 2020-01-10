package main.java;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * @author 19028
 * @date 2020/1/10 17:19
 */

public class MR2 {

    public static class MR2Mapper extends Mapper<Object, Text, Text, IntWritable> {

        /**
         * 对比a， 多一个保存stopWord的set
         */
        Set<String> stopWords = new HashSet<>();

        Scanner scanner = new Scanner(System.in);
        private String filePath = scanner.nextLine();

        /**
         * 通过setup方法， 在map任务启动前， 将停词加载到set中
         */
        @Override
        protected void setup(Context context) throws IOException {
            BufferedReader br = new BufferedReader(
                    new FileReader(filePath));
            String stopWord;
            while ((stopWord = br.readLine()) != null) {
                stopWords.add(stopWord);
            }
        }

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] lines = value.toString().split("\t");
            if (lines.length == 4) {
                String[] words = lines[3].split(" ");
                for (String word : words) {
                    // 如果停词set中不存在， 则输出到下游
                    if (!stopWords.contains(word)) {
                        context.write(new Text(word), new IntWritable(1));
                    }
                }
            }
        }
    }

    public static class MR2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        //Use queue
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

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            List<Map.Entry<String, Integer>> list = new ArrayList<>(map.entrySet());
            Collections.sort(list, (o1, o2) -> o2.getValue() - o1.getValue());
            for (Map.Entry<String, Integer> stringIntegerEntry : list.subList(0, 100)) {
                context.write(new Text(stringIntegerEntry.getKey()),
                        new IntWritable(stringIntegerEntry.getValue()));
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(MR2.class);

        job.setMapperClass(MR2Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        job.setReducerClass(MR2Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
        System.out.println((System.currentTimeMillis() - start));
    }

}
