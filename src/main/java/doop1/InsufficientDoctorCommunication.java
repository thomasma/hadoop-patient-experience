package doop1;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InsufficientDoctorCommunication {
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		if (args.length != 2) {
			System.err.println("Usage: hadoop "
					+ InsufficientDoctorCommunication.class.getName()
					+ " (input_path) (output_path)");
			System.exit(-1);
		}

		Job job = new Job();
		job.setJarByClass(InsufficientDoctorCommunication.class);
		job.setJobName("InsufficientDoctorCommunication");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(InsufficientDoctorCommunicationMapper.class);
		job.setCombinerClass(InsufficientDoctorCommunicationReducer.class);
		job.setReducerClass(InsufficientDoctorCommunicationReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}