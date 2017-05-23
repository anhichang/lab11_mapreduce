package ch.fhnw.ddm.u2;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		StringBuilder value = new StringBuilder();
		//concatenate values with delimiter
		for (Text v : values) value.append("|" + (v.toString().replace(",", "|")));
		//write reduced data back to context
		context.write(key, new Text(""+value));
	}
}
