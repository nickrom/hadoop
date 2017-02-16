package sortflights;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SortDelayedFlights {
    public static void main(String[] args) throws IOException,InterruptedException,ClassNotFoundException {
        if (args.length != 2) {
            System.err.println("Usage: SortDelayedFlights <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(SortDelayedFlights.class);
        job.setJobName("Partial Sort");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(DelayedFlightsMapper.class);
        job.setPartitionerClass(DelayedFlightsPartitioner.class);
        job.setGroupingComparatorClass(DelayedFlightsComparator.class);
        job.setReducerClass(DelayedFlightsReducer.class);

        job.setOutputKeyClass(DataFlightsInput.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(2);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
