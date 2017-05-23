package ch.fhnw.ddm.u1;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// values as inputLine splitted by tab.
		String[] inputLine = value.toString().split("\t+");
		// Writes extracted category & amount to the context.
		context.write(new Text(inputLine[3]), new DoubleWritable(Double.parseDouble(inputLine[4])));
	}
}