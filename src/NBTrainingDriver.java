package nbc;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.PropertyConfigurator;


public class NBTrainingDriver {
	private static String strfs = "hdfs://localhost:9000";
	public static void main(String[] args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", strfs);
		Job job = Job.getInstance(conf, "NBTraining");
		job.setJarByClass(NBTrainingDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(TrainingMapper.class);
		// TODO: specify a reducer
		job.setReducerClass(TrainingReducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path("/out_2015081018"));
		
		if (!job.waitForCompletion(true)) {
			System.out.println("Training failed");
			return;
		}
		System.out.println("Training complete");
		FileSystem fs = FileSystem.get(conf);
		fs.copyToLocalFile(false, new Path("/out_2015081018/part-r-00000")
			, new Path(args[1]), true);
	}

}
