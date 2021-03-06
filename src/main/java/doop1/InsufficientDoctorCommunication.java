package doop1;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class InsufficientDoctorCommunication {
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		if (args.length != 2) {
			System.err.println("Usage: hadoop "
					+ InsufficientDoctorCommunication.class.getName()
					+ " (input_path) (output_path)");
			System.exit(-1);
		}

		JobConf conf = new JobConf(DoctorCommunication.class);
		conf.setJobName("doctorcommunication");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);

		conf.setMapperClass(DoctorCommunication.InsufficientDoctorCommunicationMapper.class);
		conf.setCombinerClass(DoctorCommunication.InsufficientDoctorCommunicationReducer.class);
		conf.setReducerClass(DoctorCommunication.InsufficientDoctorCommunicationReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}