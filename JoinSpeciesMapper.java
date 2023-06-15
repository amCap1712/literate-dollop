import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class JoinSpeciesMapper
    extends Mapper<LongWritable, Text, TextPair, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        // skip header line
        if (key.get() == 0L) {
            return;
        }

        String line = value.toString();
        // line is of the format species_id,genus,species,taxa
        String[] tokens = line.split(",", -1);

        TextPair pair = new TextPair(tokens[0], "0");
        context.write(pair, new Text(tokens[2] + "," + tokens[3]));
    }

}