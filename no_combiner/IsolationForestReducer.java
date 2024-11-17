import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class IsolationForestReducer extends Reducer<Text, Text, Text, Text> {
    private Set<String> seenPointIndices = new HashSet<>(); // To track unique point indices
    private Set<String> repeatedPointIndices = new HashSet<>(); // To track point indices appearing more than once
    private Map<String, String> outputMap; // To store final outputs
        private static final double ANOMALY_THRESHOLD = 0.7;  // Threshold for anomaly

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        outputMap = new HashMap<>(); // Initialize the output map
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String pointIndex = key.toString();

                // Prepare data for output
                for (Text value : values) {
                        // Split the value to extract the data point and the anomaly score
            String[] valueParts = value.toString().split("_");
            if (valueParts.length == 2) {
                try {
                    String dataPoint = valueParts[0];
                    double anomalyScore = Double.parseDouble(valueParts[1]);

                    // Only write the data point if the anomaly score exceeds the threshold
                    if (anomalyScore >= ANOMALY_THRESHOLD) {
                                                if (seenPointIndices.contains(pointIndex)) {
                                                        // If it has been seen before, add it to the repeated set
                                                        repeatedPointIndices.add(pointIndex);
                                                        String valueToStore = dataPoint;
                                                        String valueStored = "";
                                                        if (outputMap.containsKey(pointIndex)) {
                                                                valueStored = outputMap.get(pointIndex) + ",";
                                                        }
                                                        outputMap.put(pointIndex, valueStored + valueToStore); // Store output for later
                                                } else {
                                                        // Otherwise, add it to the seen set
                                                        seenPointIndices.add(pointIndex);
                                                }
                    }
                } catch (NumberFormatException e) {
                                        e.printStackTrace();
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Write all accumulated results after processing all keys
        for (Map.Entry<String, String> entry : outputMap.entrySet()) {
            context.write(new Text(entry.getKey()), new Text(entry.getValue()));
        }
    }
}
