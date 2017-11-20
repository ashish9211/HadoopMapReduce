
import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IndMedal {
static class MyMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
public void map(LongWritable key,Text value,Context context ) throws IOException, InterruptedException
{
	Text year=new Text();
String str[]=value.toString().split("\t");
year.set(str[3]);
if(str[2].equalsIgnoreCase("India"))
{


context.write(year, new IntWritable(Integer.parseInt(str[9])));
}
}
}
static class MyReducer extends Reducer<Text, IntWritable,Text,IntWritable>
{
public void reduce(Text key,Iterable<IntWritable> value,Context context ) throws IOException, InterruptedException
{
	int sum=0;
	for(IntWritable val : value)
	{
		
		sum+=val.get();
	}
	context.write(key, new IntWritable(sum));
	
}
}
public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	Configuration conf=new Configuration();
	Job job=Job.getInstance(conf,"IndMedal");
	job.setJarByClass(IndMedal.class);
	job.setMapperClass(MyMapper.class);
	job.setReducerClass(MyReducer.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(IntWritable.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job,new Path(args[1]));
	System.exit(job.waitForCompletion(true)? 0:1);
}
}

