import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IsolationForestCombiner extends Reducer<Text, Text, Text, Text> {
    private static final double ANOMALY_THRESHOLD = 0.7;  // Threshold for anomaly

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            // Split the value to extract the data point and the anomaly score
            String[] parts = value.toString().split("_");
            if (parts.length == 2) {
                try {
                    String dataPoint = parts[0];
                    double anomalyScore = Double.parseDouble(parts[1]);

                    // Only write the data point if the anomaly score exceeds the threshold
                    if (anomalyScore >= ANOMALY_THRESHOLD) {
                        context.write(key, new Text(dataPoint));
                    }
                } catch (NumberFormatException e) {
                                        e.printStackTrace();
                }
            }
        }
    }
}
