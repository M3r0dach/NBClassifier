package nbc;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TrainingReducer extends
		Reducer<Text, LongWritable, Text, LongWritable> {

	public void reduce(Text _key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		// process values
		long sum = 0;
		LongWritable rst = new LongWritable();
		for (LongWritable val : values) {
			sum += val.get();
		}
		rst.set(sum);
		context.write(_key, rst);
	}

}
