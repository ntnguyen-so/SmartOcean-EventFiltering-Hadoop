import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;  // Import Log4j Logger

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class IsolationForestMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final Logger LOG = Logger.getLogger(IsolationForestMapper.class); // Initialize Logger
    private Text outputKey = new Text();  // Output key (data point index)
    private Text outputValue = new Text();  // Output value (data point value_anomaly score)
    private IsolationForest isolationForest;  // IsolationForest instance
    private List<Double> dataIndices = new ArrayList<>();  // List to store data indices
    private List<Double> dataPoints = new ArrayList<>();  // List to store data points

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        isolationForest = new IsolationForest(100, 256);  // Initialize Isolation Forest with parameters
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();

        if (line.isEmpty()) {
            return;
        }

        String[] parts = line.split(",");
        if (parts.length < 2) {
            return;
        }

        try {
            double dataIndex = Double.parseDouble(parts[0]);
            double dataPoint = Double.parseDouble(parts[1]);

            dataIndices.add(dataIndex); // Add the data index to the list for storing keys
            dataPoints.add(dataPoint);  // Add the data point to the list for training
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        LOG.info("Starting cleanup phase. Training Isolation Forest with " + dataPoints.size() + " data points.");
        List<List<Double>> trainingData = new ArrayList<>();
        for (Double point : dataPoints) {
            List<Double> singlePointList = new ArrayList<>();
            singlePointList.add(point);
            trainingData.add(singlePointList);
        }

        // Train the Isolation Forest
        isolationForest.fit(trainingData);

        // Now score each data point using the trained model
        for (int i = 0; i < dataPoints.size(); i++) {
            List<Double> singleDataPoint = new ArrayList<>();
            Double dataPoint = dataPoints.get(i);
            singleDataPoint.add(dataPoint);

            double anomalyScore = isolationForest.getAnomalyScore(trainingData, singleDataPoint);

            // Output value will be the combination of data point and anomaly score
            outputValue.set(dataPoint + "_" + anomalyScore);
            Double pointIndex = dataIndices.get(i); // Get the index of the data point which will be stored as key
            outputKey.set(String.valueOf(pointIndex));
            context.write(outputKey, outputValue);
        }
    }
}
