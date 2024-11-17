import random
import sys

def generate_data(num_points, output_file, anomaly_ratio=0.05):
    with open(output_file, 'w') as f:
        for i in range(num_points):
            if random.random() < anomaly_ratio:  # Introduce anomalies based on the specified ratio
                # Generate an anomaly (e.g., a value outside the range [0, 1])
                anomaly_value = round(random.uniform(1.5, 2.0), 1)  # Example anomaly
                f.write(f"{i},{anomaly_value}\n")
            else:
                # Generate a normal data point
                data_point = round(random.uniform(0, 1), 1)
                f.write(f"{i},{data_point}\n")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python3 generate_data.py <number_of_points> <output_file> <anomaly_ratio>")
        sys.exit(1)

    try:
        num_points = int(sys.argv[1])
        output_file = sys.argv[2]
        anomaly_ratio = float(sys.argv[3])  # Ratio of anomalies to introduce (between 0 and 1)

        if not (0 <= anomaly_ratio <= 1):
            raise ValueError("Anomaly ratio must be between 0 and 1.")

        generate_data(num_points, output_file, anomaly_ratio)
        print(f"Generated {num_points} data points with an anomaly ratio of {anomaly_ratio} in {output_file}")
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)
