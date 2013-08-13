package doop1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.lang.text.StrTokenizer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class DoctorCommunication {
    public static class InsufficientDoctorCommunicationMapper extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, DoubleWritable> {

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output,
                Reporter reporter) throws IOException {
            StrTokenizer line = StrTokenizer.getCSVInstance(value.toString());
            String[] tokens = line.getTokenArray();
            String state = tokens[6];
            String docFb = tokens[13];
            double docFbPercent = 0d;
            try {
                docFbPercent = Double.parseDouble(docFb.substring(0, docFb.lastIndexOf('%')));
            } catch (Exception ex) {
                docFbPercent = 0d;
            }
            output.collect(new Text(state), new DoubleWritable(docFbPercent));
        }
    }

    public static class InsufficientDoctorCommunicationReducer extends MapReduceBase implements
            Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterator<DoubleWritable> values,
                OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
            double maxValue = Double.MIN_VALUE;
            while (values.hasNext()) {
                maxValue = Math.max(maxValue, values.next().get());
            }
            output.collect(key, new DoubleWritable(maxValue));
        }
    }
}
