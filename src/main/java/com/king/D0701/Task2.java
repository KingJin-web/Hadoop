package com.king.D0701;

import com.king.util.ReadOutput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author Exception
 * @create 2021-07-02-19:31
 * @content
 */
public class Task2 extends Configured implements Tool {

    /**
     * 计数器
     * 用于计数各种异常数据
     */
    enum Counter {
        TIMESKIP,   //时间格式有误
        OUTOFTIMESKIP,  //时间不在参数指定的时间段内
        LINESKIP,   //源文件有误
        USERSKIP    //某个用户某个时间段被整个放弃
    }

    /**
     * 读取一行数据
     * 以"IMSI+时间段"作为KEY发射出去
     */
    public static class myMapper1 extends Mapper<LongWritable, Text, Text, Text> {
        String date;
        String[] timepoint;
        boolean dataSource;

        /**
         * 初始化:setUp()执行一次,读取配置.
         */
        @Override
        protected void setup(Context context) throws IOException {
            this.date = context.getConfiguration().get("date"); //读取日期
            this.timepoint = context.getConfiguration().get("timepoint").split("-");    // 读取时间分割点
            //提取输入的文件名,判断是POS.txt,还是NET.txt
            FileSplit fs = (FileSplit) context.getInputSplit();
            String fileName = fs.getPath().getName();
            if (fileName.startsWith("POS")) {
                dataSource = true;
            } else if (fileName.startsWith("NET")) {
                dataSource = false;
            } else {
                throw new IOException("File Name should starts with POS or NET");
            }
        }

        /**
         * Map任务
         * 读取基站数据
         * 找出数据所对应时间段
         * 以IMSI和时间段作为KEY
         * CGI和时间作为VALUE
         *
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            TableLine tableLine = new TableLine();

            //读取行
            try {
                tableLine.set(line, this.dataSource, this.date, this.timepoint);
            } catch (LineException e) {
                if (e.getFlag() == -1) {
                    //mapreduce中的一个Map<xxx,数量>
                    context.getCounter(Task1.Counter.OUTOFTIMESKIP).increment(1);
                } else {
                    context.getCounter(Task1.Counter.TIMESKIP).increment(1);
                }
                return;
            } catch (Exception e) {
                context.getCounter(Task1.Counter.LINESKIP).increment(1);
                return;
            }
            context.write(tableLine.OutKey(), tableLine.OutValue());
        }
    }

    //因为NewObject要做值输出,所以要实现Writable,另外,要执行浅拷贝,所以要实现Cloneable

    public static class NewObject implements Writable, Cloneable {
        private String position;    //基站
        private float time; //时间

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(position);
            dataOutput.writeFloat(time);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.position = dataInput.readUTF();
            this.time = dataInput.readFloat();
        }

        public NewObject() {

        }

        public NewObject(String position, float time) {
            this.position = position;
            this.time = time;
        }

        public String getPosition() {
            return position;
        }

        public void setPosition(String position) {
            this.position = position;
        }

        public float getTime() {
            return time;
        }

        public void setTime(float time) {
            this.time = time;
        }

        //实现浅克隆
        @Override
        public NewObject clone() {
            NewObject newObject = null;
            try {
                newObject = (NewObject) super.clone();
            } catch (CloneNotSupportedException e) {
                e.printStackTrace();
            }
            return newObject;
        }
    }

    public static class myMapper2 extends Mapper<LongWritable, Text, Text, NewObject> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] str = value.toString().trim().split("\\|");
            if (str != null && str.length > 1) {
                String imsi = str[0];
                String position = str[1];
                String timestep = str[2];
                float time = Float.parseFloat(str[3]);
                NewObject no = new NewObject(position, time);
                context.write(new Text(imsi + "|" + timestep), no);
            }
        }
    }

    public static class myReduce1 extends Reducer<Text, Text, NullWritable, Text> {
        private String date;
        private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        /**
         * 初始化
         *
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) {
            this.date = context.getConfiguration().get("date"); //读取日期
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //手机号
            String imsi = key.toString().split("\\|")[0];
            //时间段
            String timeFlag = key.toString().split("\\|")[1];

            //用一个TreeMap(排好序)记录时间,它可以按键排序,这样,只要把时间戳放在键上就可以排序了
            //      时间戳   基站
            TreeMap<Long, String> uploads = new TreeMap<>();
            String valueString;

            for (Text value : values) {
                valueString = value.toString();
                try {
                    //          时间戳                                             基站名
                    uploads.put(Long.valueOf(valueString.split("\\|")[1]), valueString.split("\\|")[0]);
                } catch (NumberFormatException e) {
                    //时间戳格式错误,无法进行,Lon.valueOf()运算
                    context.getCounter(Task1.Counter.TIMESKIP).increment(1);
                    continue;
                }
            }

            try {
                //在最后添加"OFF"位置,即本时间段中,最后一个时间点
                //tmp的值: 2021-07-01 07:00:00
                Date tmp = this.formatter.parse(this.date + " " + timeFlag.split("-")[1] + ":00:00");
                uploads.put((tmp.getTime() / 1000L), "OFF");

                //汇总数据:<基站,停留时长>
                HashMap<String, Float> locs = getStayTime(uploads);

                //输出
                for (Map.Entry<String, Float> entry : locs.entrySet()) {
                    StringBuilder builder = new StringBuilder();
                    builder.append(imsi).append("|");
                    builder.append(entry.getKey()).append("|");
                    builder.append(timeFlag).append("|");
                    builder.append(entry.getValue());

                    context.write(NullWritable.get(), new Text(builder.toString()));
                }
            } catch (Exception e) {
                context.getCounter(Task1.Counter.USERSKIP).increment(1);
                return;
            }
        }

        /**
         * 获取位置停留信息
         */
        private HashMap<String, Float> getStayTime(TreeMap<Long, String> uploads) {
            Map.Entry<Long, String> upload, nextUpload;
            HashMap<String, Float> locs = new HashMap<>();

            //初始化
            Iterator<Map.Entry<Long, String>> it = uploads.entrySet().iterator();
            upload = it.next();
            //计算
            while (it.hasNext()) {
                nextUpload = it.next();
                //用后一条的时间戳-前一条数据的时间戳/60得到分钟
                float diff = (float) (nextUpload.getKey() - upload.getKey()) / 60.0f;
                if (diff <= 60.0) { //时间间隔过大则代表关机
                    if (locs.containsKey(upload.getValue())) {
                        locs.put(upload.getValue(), locs.get(upload.getValue()) + diff);
                    } else {
                        locs.put(upload.getValue(), diff);
                    }
                }
                upload = nextUpload;    //将当前这条数据,当前前一条数据,再循环
            }
            return locs;
        }
    }

    public static class myReduce2 extends Reducer<Text, NewObject, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<NewObject> values, Context context) throws IOException, InterruptedException {
            List<NewObject> list = new ArrayList<>();
            for (NewObject no : values) {
                list.add(no.clone());   //特别要注意,在reduce中,key和values都只有一个对象生成,如果不用clone()的话,则都是最后一个对象的值
            }
            //排序
            Collections.sort(list, new Comparator<NewObject>() {
                @Override
                public int compare(NewObject o1, NewObject o2) {
                    //因为要降序,所以取反
                    return -(int) (o1.time - o2.time);
                }
            });
            int i = 0;
            for (NewObject nn : list) {
                if (i > 2) {
                    break;
                }
                i++;
                context.write(NullWritable.get(), new Text(key.toString() + "|" + nn.getPosition() + "|" + nn.getTime()));
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        conf.set("date", args[3]);
        conf.set("timepoint", args[4]);

        Job job = new Job(conf, "基础分析");
        job.setJarByClass(Task2.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));   //输入路径
        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputPath);    //输出路径
        //创建  输出 文件 ，并判断 这个输出 文件是否存在
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        job.setMapperClass(myMapper1.class);    //调用上面的Map类作为Map任务代码
        job.setReducerClass(myReduce1.class);   //调用上面的Reduce类作为Reduce任务代码
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "每个用户在不同时段停留最长的三个位置");
        job2.setJarByClass(Task2.class);

        Path inputPath2 = new Path(args[1]);    //将上一输出文件当成job2的输入文件
        Path outputPath2 = new Path(args[2]);
        //创建  输出 文件 ，并判断 这个输出 文件是否存在
        if (fs.exists(outputPath2)) {
            fs.delete(outputPath2, true);
        }

        FileInputFormat.addInputPath(job2, inputPath2);
        FileOutputFormat.setOutputPath(job2, outputPath2);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(NewObject.class);

        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(Text.class);

        job2.setMapperClass(myMapper2.class);
        job2.setReducerClass(myReduce2.class);

        job2.waitForCompletion(true);

        return job2.isSuccessful() ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure(); //自动快速地使用缺省Log4j环境
        if (args == null || args.length < 1) {
            args = new String[5];
            args[0] = "D:\\wordcount\\input\\D0701";
            args[1] = "D:\\wordcount\\output\\D0701";
            args[2] = "D:\\wordcount\\output\\D0701\\data";
            args[3] = "2021-07-01";
            args[4] = "07-09-17-24";
        }
        //参数格式要求
        if (args.length != 5) {
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

        //运行任务
        int res = ToolRunner.run(new Configuration(), new Task2(), args);

        ReadOutput.readAll(args[1],true);
        System.exit(res);
    }
}

