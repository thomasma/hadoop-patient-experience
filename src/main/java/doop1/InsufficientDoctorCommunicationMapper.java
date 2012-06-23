package doop1;
import java.io.IOException;

import org.apache.commons.lang.text.StrTokenizer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InsufficientDoctorCommunicationMapper extends
		Mapper<LongWritable, Text, Text, DoubleWritable> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		StrTokenizer line = StrTokenizer.getCSVInstance(value.toString());
		String[] tokens = line.getTokenArray();
		String state = tokens[6];
		String docFb = tokens[13];
		double docFbPercent = 0d;
		try {
			docFbPercent = Double.parseDouble(docFb.substring(0,
					docFb.lastIndexOf('%')));
		} catch (Exception ex) {
			docFbPercent = 0d;
		}
		context.write(new Text(state), new DoubleWritable(docFbPercent));
	}
}