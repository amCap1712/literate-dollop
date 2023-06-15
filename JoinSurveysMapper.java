import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class JoinSurveysMapper
    extends Mapper<LongWritable, Text, TextPair, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        // skip header line
        if (key.get() == 0L) {
            return;
        }

        String line = value.toString();
        // line is of the format record_id,month,day,year,plot_id,species_id,sex,hindfoot_length,weight
        String[] tokens = line.split(",", -1);

        // skip entries without a species_id
        if (tokens[5].isEmpty()) {
            return;
        }

        String date = tokens[2] + "/" + tokens[1] + "/" + tokens[3];

        TextPair pair = new TextPair(tokens[5], "1");
        Text mappedValue = new Text(tokens[0] + "," + date);
        context.write(pair, mappedValue);
    }

}