package nbc;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TrainingMapper extends
		Mapper<LongWritable, Text, Text, LongWritable> {
	private Text word = new Text();
	private LongWritable one = new LongWritable(1);
	
	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		String line = ivalue.toString();
		String label = line.substring(0, line.indexOf("\t"));
		String text = line.substring(line.indexOf("\t")+1);
		StringTokenizer tokenizer = new StringTokenizer(text);
		
		word.set(label);
		context.write(word, one);
		while(tokenizer.hasMoreTokens()) {
			String val = tokenizer.nextToken();
			if(val.matches("[\u4e00-\u9fa5]+")) {
				word.set(label+"-"+val);
				context.write(word, one);
			}
		}
	}

}
