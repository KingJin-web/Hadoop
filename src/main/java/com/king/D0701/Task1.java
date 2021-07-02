package com.king.D0701;

import com.king.util.ReadOutput;
import com.king.weblog.Pv_mr;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * @program: hdfs
 * @description: 先计算出不同用户在不同时段在不同基站停留的时间.
 * @author: King
 * @create: 2021-07-01 19:53
 */
public class Task1 extends Configured implements Tool {
    enum Counter {
        TIMESKIP, //时间格式有误
        OUTOFTIMESKIP, // 时间不在指定时间段类
        LINESKIP, //源文件有误
        USERSKIP //

    }

    public static class TaskMapper extends Mapper<LongWritable, Text, Text, Text> {
        //        private Text outputKey = new Text();
//        private static final IntWritable outputValue = new IntWritable(1);
        private TableLine tl = new TableLine();
        private String date;
        private String[] timepoint;
        private boolean dataSource;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.date = context.getConfiguration().get("date");
            this.timepoint = context.getConfiguration().get("timepoint").split("-");
            FileSplit fs = (FileSplit) context.getInputSplit();
            String fileName = fs.getPath().getName();
            if (fileName.startsWith("POS")) {
                dataSource = true;
            } else if (fileName.startsWith("NET")) {
                dataSource = false;
            } else {
                throw new IOException("File name should start with POS or NET");
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String s = value.toString();
            try {
                tl.set(s, this.dataSource, this.date, this.timepoint);
            } catch (LineException e) {
                if (e.getFlag() == -1) {
                    context.getCounter(Counter.OUTOFTIMESKIP).increment(1);
                } else {
                    context.getCounter(Counter.TIMESKIP).increment(1);
                }
                return;
            } catch (Exception e) {
                context.getCounter(Counter.LINESKIP).increment(1);
                return;
            }

            context.write(tl.OutKey(), tl.OutValue());

        }
    }

    public static class TaskReduce extends Reducer<Text, Text, NullWritable, Text> {
        private Text outputValue = new Text();
        // 用户IMSI|基站|时间段|时长
        private String imsi;
        private String position;
        private String timeFlag;
        private String time;
        private String date;

        private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.date = context.getConfiguration().get("date");
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] s1 = key.toString().split("\\|");
            imsi = s1[0];
            timeFlag = s1[1];

            TreeMap<Long, String> uploads = new TreeMap<>();
            String value;
            String s2[];
            for (Text v : values) {

                value = v.toString();
                s2 = value.split("\\|");

                try {
                    uploads.put(Long.valueOf(s2[1]), s2[0]);
                } catch (NumberFormatException e) {
                    context.getCounter(Counter.TIMESKIP).increment(1);
                }

            }
            try {
                Date tmp = this.formatter.parse(this.date + " " + timeFlag.split("-")[1] + ":00:00");
                uploads.put(tmp.getTime() / 1000L, "OFF");
                HashMap<String, Float> locs = getStayTime(uploads);
                System.out.println(locs);
                for (Entry<String, Float> entry : locs.entrySet()) {
                    StringBuilder builder = new StringBuilder();
                    builder.append(imsi).append("|");
                    builder.append(entry.getKey()).append("|");
                    builder.append(timeFlag).append("|");
                    builder.append(entry.getValue());
                    outputValue.set(builder.toString());
                    context.write(NullWritable.get(), outputValue);
                }


            } catch (ParseException e) {
                e.printStackTrace();
            }


            super.reduce(key, values, context);
        }

        private HashMap<String, Float> getStayTime(TreeMap<Long, String> uploads) {
            Entry<Long, String> upload, nextUpload;
            HashMap<String, Float> locs = new HashMap<>();
            Iterator<Entry<Long, String>> it = uploads.entrySet().iterator();
            upload = it.next();
            while (it.hasNext()) {
                nextUpload = it.next();
                float diff = (float) ((nextUpload.getKey() - upload.getKey()) / 60.0);
                if (diff <= 60.0) {
                    if (locs.containsKey(upload.getValue())) {
                        locs.put(upload.getValue(), locs.get(upload.getValue()) + diff);
                    } else {
                        locs.put(upload.getValue(), diff);
                    }
                }
                upload = nextUpload;
            }
            return locs;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        conf.set("date", strings[2]);
        conf.set("timepoint", strings[3]);
        FileSystem fs = FileSystem.get(conf);
        Path inPath = new Path(strings[0]);
        Path outPath = new Path(strings[1]);
        Job job = Job.getInstance(super.getConf(), "基站");
        job.setJarByClass(Task1.class);

        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        job.setMapperClass(TaskMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(TaskReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        boolean b = job.waitForCompletion(true);

        ReadOutput.readAll(outPath);
        return b ? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure(); //自动快速地使用缺省Log4j环境
        if (args == null || args.length < 1) {
            args = new String[4];
            args[0] = "D:\\wordcount\\input\\D0701";
            args[1] = "D:\\wordcount\\output\\D0701";
            args[2] = "2021-07-01";
            args[3] = "07-09-17-24";
        }
        if (args.length != 4) {
            System.err.println("");
            System.err.println("Usage: BaseStationDataPreprocess < input path > < output path > < date > < timepoint >");
            System.err.println("Example: BaseStationDataPreprocess /user/james/Base /user/james/Output 2012-09-12 07-09-17-24");
            System.err.println("Warning: Timepoints should be begined with a 0+ two digit number and the last timepoint should be 24");
            System.err.println("Counter:");
            System.err.println("\t" + "TIMESKIP" + "\t" + "Lines which contain wrong date format");
            System.err.println("\t" + "OUTOFTIMESKIP" + "\t" + "Lines which contain times that out of range");
            System.err.println("\t" + "LINESKIP" + "\t" + "Lines which are invalid");
            System.err.println("\t" + "USERSKIP" + "\t" + "Users in some time are invalid");
            System.exit(-1);
        }

        int i = ToolRunner.run(new Task1(), args);

        System.exit(i);
    }
}
