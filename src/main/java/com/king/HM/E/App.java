package com.king.HM.E;

import com.king.HM.B.JobMain;
import com.king.HM.D.NewKey;
import com.king.HM.D.WcMapper;
import com.king.HM.D.WcReducer;
import com.king.util.ReadOutput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

/**
 * @program: hdfs
 * @description:
 * @author: King
 * @create: 2021-06-24 19:54
 */
public class App extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure(); //自动快速地使用缺省Log4j环境
        int i = ToolRunner.run(new App(), args);
        ReadOutput.read("D:\\wordcount\\output\\E");
        System.exit(i);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Path in1 = new Path("D:\\wordcount\\input\\E\\allCity.txt");
        Path in2 = new Path("D:\\wordcount\\input\\E\\someCity.txt");
        Path outPath = new Path("D:\\wordcount\\output\\E");
        if (strings.length >= 1) {
            in1 = new Path(strings[0]);
            in2 = new Path(strings[1]);
            outPath = new Path(strings[2]);
        }
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Job job = Job.getInstance(super.getConf(), "二次排序");
        job.setJarByClass(App.class);
        job.setInputFormatClass(TextInputFormat.class);

        MultipleInputs.addInputPath(job, in1, TextInputFormat.class, AllCityMapper.class);
        MultipleInputs.addInputPath(job, in2, TextInputFormat.class, SomeCityMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(CityReduce.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        TextOutputFormat.setOutputPath(job, outPath);

        boolean b = job.waitForCompletion(true);

        return b ? 0 : 1;
    }
}
