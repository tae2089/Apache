package airline.reduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AirLineReducer extends Reducer<Text,IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable(); //이러면 값 이 0

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException{
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        result.set(sum);
        context.write(key, result);
    }

}
