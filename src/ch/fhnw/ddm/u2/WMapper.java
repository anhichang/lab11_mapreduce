package ch.fhnw.ddm.u2;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WMapper extends Mapper<Text, Text, Text, Text> {
	@Override
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		// ignores lines starting with a # (comments)
		if (key.charAt(0) == '#') return;

		// Writes extracted key [en] & value [de,fr,it]-pair to the context.
		context.write(key, new Text(value));
	}
}