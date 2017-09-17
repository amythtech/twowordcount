package cloud.assignment.twoword;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TwoWordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String inputStr = value.toString();
		List<String> doubleWrds = new ArrayList<>();
		String[] lines = inputStr.split("\\r?\\n");

		for (int i = 0; i < lines.length; i++) {
			// divide input string by line
			String filterLine = new String(lines[i].trim());

			// Get word from each line and combine two consequetive words
			String[] eachLineWrd = filterLine.split(" ");
			int len = eachLineWrd.length;
			int k = 0;
			while (k < len - 1) {
				doubleWrds.add(eachLineWrd[k] + " " + eachLineWrd[k + 1]);
				k++;
			}
		}

		List<String> twoWord = new ArrayList<String>();

		for (int j = 0; j < doubleWrds.size(); j++)
			twoWord.add(doubleWrds.get(j));

		for (String eachWord : twoWord) {
			Text outputKey = new Text(eachWord.trim());
			IntWritable outputValue = new IntWritable(1);
			context.write(outputKey, outputValue);
		}
	}

}