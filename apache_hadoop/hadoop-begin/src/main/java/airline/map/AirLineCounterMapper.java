package airline.map;

import airline.domain.AirLineInfo;
import airline.domain.DelayCounters;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirLineCounterMapper extends Mapper<LongWritable,Text, Text, IntWritable> {
    private String workType;
    //1값 주기
    private final static IntWritable one = new IntWritable(1);

    //key 값 만들기
    private Text outputkey = new Text();

    public void map(LongWritable key, Text value , Context context) throws IOException,InterruptedException {

        AirLineInfo data = new AirLineInfo(value);

        if (workType.equals("departure")) {
            if (data.isDepartureDelayAvailable()) {
                if (data.getDepartureDelayTime() > 0) {
                    outputkey.set(data.getYear() + "," + data.getMonth());
                    context.write(outputkey, one);
                } else if (data.getDepartureDelayTime() == 0) {
                    context.getCounter(DelayCounters.scheduled_departure).increment(1);
                } else if (data.getDepartureDelayTime() < 0) {
                    context.getCounter(DelayCounters.early_departure).increment(1);
                }
            }else{
                context.getCounter(DelayCounters.not_available_departure).increment(1);
            }

        } else if (workType.equals("arrival")) {
            if (data.isArriveDelayAvailable()) {
                if (data.getArriverDelayTime() > 0) {
                    outputkey.set(data.getYear() + "," + data.getMonth());
                    context.write(outputkey, one);
                }else if(data.getArriverDelayTime() == 0){
                    context.getCounter(DelayCounters.scheduled_arrival).increment(1);
                } else if (data.getArriverDelayTime() < 0) {
                    context.getCounter(DelayCounters.early_arrival).increment(1);
                }
            }else{
                context.getCounter(DelayCounters.not_available_arrival).increment(1);
            }
        }else{
            context.getCounter(DelayCounters.not_available_arrival).increment(1);
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        workType = context.getConfiguration().get("workType");
    }
}
