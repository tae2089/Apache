package airline.map;

import airline.domain.AirLineInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirLiineMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    //1값 주기
    private final static IntWritable one = new IntWritable(1);

    //key 값 만들기
    private Text outputkey = new Text();

    public void map(LongWritable key, Text value , Context context) throws IOException,InterruptedException {

        AirLineInfo data = new AirLineInfo(value);
        outputkey.set(data.getYear() + "," + data.getMonth());
        if (data.getDepartureDelayTime() > 0) {
            context.write(outputkey, one);
        }
    }

}
