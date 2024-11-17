import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IsolationForestDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: IsolationForestDriver <input path> <output path>");
            System.exit(-1);
        }

        // Configuration and Job setup
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Isolation Forest Anomaly Detection");

        job.setJarByClass(IsolationForestDriver.class);
        job.setMapperClass(IsolationForestMapper.class);
        //job.setCombinerClass(IsolationForestCombiner.class);
        job.setReducerClass(IsolationForestReducer.class);

        // Explicitly set the Mapper output key and value classes
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Set the final output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Input and Output paths
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        // Check if output path exists and delete if it does
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // Run the job and exit based on completion status
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
