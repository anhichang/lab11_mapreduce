package ch.fhnw.ddm.u1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException,
			InterruptedException {
		
		double sum = 0;

		// Go through all values to sum up amount per cat
		for (DoubleWritable value : values) {
			sum += value.get();
		}

		//write reduced data back to context
		context.write(key, new DoubleWritable(sum));
	}
}
