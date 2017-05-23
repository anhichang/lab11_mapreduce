package ch.fhnw.ddm.u2;
import java.util.Arrays;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		//validate arguments (at least 2)
		String output;
		String[] inputs;
		if (args.length >= 2) {
			output = args[args.length-1];
			inputs = Arrays.copyOfRange(args, 0, args.length-2) ;
		} else {
			System.err.println("Incorrect number of arguments. Expected: input output");
			return -1;
		}
		  
			
		//load job config
		Job job = new Job(getConf());
		job.setJarByClass(WDriver.class);
		job.setJobName(this.getClass().getName());

		//config input files and register mapper & in;utformatclass
		for(String input : inputs){
			MultipleInputs.addInputPath(job, new Path(input),KeyValueTextInputFormat.class, WMapper.class);
		}
		//config output 
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		//setup our reducer
		job.setMapperClass(WMapper.class);
		job.setReducerClass(WReducer.class);

		//config output type
		job.setMapOutputKeyClass(Text.class);
		job.setOutputKeyClass(Text.class);

		//wait until job completed
		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		//Instantiate driver & start ToolRunner
		//args with multiple dictionaries [de,en,it,etc.]
		WDriver driver = new WDriver();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}
	
	
}