package joinflights;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class JoinFlights {
    public static void main(String[] args) throws IOException,InterruptedException,ClassNotFoundException {
        if (args.length != 3) {
            System.err.println("Usage: JoinFlights <input path flights> <input path airports> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(JoinFlights.class);
        job.setJobName("Join flights and airports");
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, DelayedFlightsMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AirportNameMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setPartitionerClass(JoinFlightsPartitioner.class);
        job.setGroupingComparatorClass(JoinFlightsComparator.class);
        job.setReducerClass(JoinFlightsReducer.class);
        job.setMapOutputKeyClass(FlightsAirportJoinerKey.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(2);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
