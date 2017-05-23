package ch.fhnw.ddm.u1;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		//validate arguments (input/output)
		String input, output;
		if (args.length == 2) {
			input = args[0];
			output = args[1];
		} else {
			System.err.println("Incorrect number of arguments.  Expected: input output");
			return -1;
		}
		
		//load job config
		Job job = new Job(getConf());
		job.setJarByClass(PDriver.class);
		job.setJobName(this.getClass().getName());

		//config input & output file/folder
		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		//setup our mapper
		job.setMapperClass(PMapper.class);
		//setup our reducer
		job.setReducerClass(PReducer.class);

		//config output types (category[Text], amount[Double])
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		//wait until job completed
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		//Instantiate driver & start ToolRunner
		//args from run config (input/output)
		PDriver driver = new PDriver();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}
}