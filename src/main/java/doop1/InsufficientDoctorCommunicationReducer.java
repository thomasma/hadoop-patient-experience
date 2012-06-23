package doop1;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InsufficientDoctorCommunicationReducer extends
		Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	public void reduce(Text key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {
		double maxValue = Double.MIN_VALUE;
		for (DoubleWritable value : values) {
			maxValue = Math.max(maxValue, value.get());
		}
		context.write(key, new DoubleWritable(maxValue));
	}
}