import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class JoinReducer extends Reducer<TextPair, Text, Text, Text> {

  @Override
  protected void reduce(TextPair key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    Iterator<Text> iter = values.iterator();
    String species = iter.next().toString();

    while (iter.hasNext()) {
      String survey = iter.next().toString();
      Text output = new Text(survey + "," + species);
      context.write(null, output);
    }
  }
}