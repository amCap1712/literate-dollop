import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Join {
    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException {
    	Configuration conf = new Configuration();
        conf.set("m", "2");
        conf.set("n", "3");
        conf.set("p", "2");

        Job job = Job.getInstance(conf, "SpeciesSurvey");
        job.setJarByClass(Join.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, JoinSpeciesMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, JoinSurveysMapper.class);
        job.setMapOutputKeyClass(TextPair.class);

        job.setReducerClass(JoinReducer.class);
        job.setPartitionerClass(KeyPartitioner.class);
        job.setGroupingComparatorClass(TextPair.FirstComparator.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }
}