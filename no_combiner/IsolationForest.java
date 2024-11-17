import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class IsolationForest {
    private int numTrees;
    private int subsampleSize;
    private int maxDepth = 10;  // Maximum depth of each tree
    private List<IsolationTree> trees = new ArrayList<>();
    private Random random = new Random();

    public IsolationForest(int numTrees, int subsampleSize) {
        this.numTrees = numTrees;
        this.subsampleSize = subsampleSize;
    }

    // Train the isolation forest by building multiple trees
    public void fit(List<List<Double>> data) {
        trees.clear(); // Clear any existing trees to retrain
        int dataSize = data.size();
        for (int i = 0; i < numTrees; i++) {
            // Create a random subsample
            List<List<Double>> subsample = new ArrayList<>();
            for (int j = 0; j < subsampleSize; j++) {
                subsample.add(data.get(random.nextInt(dataSize)));
            }
            // Build and add an isolation tree to the forest
            IsolationTree tree = buildTree(subsample, 0);
            trees.add(tree);
        }
    }

    // Compute anomaly score for a given data point
    public double getAnomalyScore(List<List<Double>> data, List<Double> dataPoint) {
        fit(data); // Train the model on the dataset before scoring

        int isolatedCount = 0;
        for (IsolationTree tree : trees) {
            if (isIsolated(tree, dataPoint)) {
                isolatedCount++;
            }
        }
        // Anomaly score is the fraction of trees in which the point is isolated
        return 1 - ((double) isolatedCount / numTrees);
    }

    // Check if a data point is isolated within a specific tree
    private boolean isIsolated(IsolationTree tree, List<Double> dataPoint) {
        IsolationTree currentNode = tree;
        int depth = 0;

        while (!currentNode.isLeaf() && depth < maxDepth) {
            // Decide which branch to follow
            if (dataPoint.get(currentNode.splitIndex) < currentNode.splitValue) {
                currentNode = currentNode.leftChild;
            } else {
                currentNode = currentNode.rightChild;
            }
            depth++;
        }

        // Isolation is determined by reaching a leaf quickly
        return depth < maxDepth;
    }

    // Build an isolation tree from a subsample (recursive function)
    private IsolationTree buildTree(List<List<Double>> subsample, int depth) {
        // Base case: stop if max depth is reached or subsample size is 1
        if (depth >= maxDepth || subsample.size() <= 1) {
            return new IsolationTree();  // Leaf node
        }

        // Randomly select an attribute and split value for branching
        int splitIndex = random.nextInt(subsample.get(0).size());  // Random feature index
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
        for (List<Double> point : subsample) {
            double value = point.get(splitIndex);
            if (value < min) min = value;
            if (value > max) max = value;
        }

        if (min == max) { // All points are identical on this feature
            return new IsolationTree();
        }

        double splitValue = min + (max - min) * random.nextDouble();

        // Partition the data based on the split
        List<List<Double>> leftSubsample = new ArrayList<>();
        List<List<Double>> rightSubsample = new ArrayList<>();

        for (List<Double> point : subsample) {
            if (point.get(splitIndex) < splitValue) {
                leftSubsample.add(point);
            } else {
                rightSubsample.add(point);
            }
        }

        // Recursively build left and right subtrees
        IsolationTree leftChild = buildTree(leftSubsample, depth + 1);
        IsolationTree rightChild = buildTree(rightSubsample, depth + 1);

        return new IsolationTree(splitIndex, splitValue, leftChild, rightChild);
    }

    // Inner class for isolation tree structure
    private class IsolationTree {
        int splitIndex;       // Feature index for splitting
        double splitValue;     // Value for splitting
        IsolationTree leftChild;  // Left subtree
        IsolationTree rightChild; // Right subtree

        // Constructor for leaf nodes
        IsolationTree() {
            this.splitIndex = -1;
            this.splitValue = Double.NaN;
            this.leftChild = null;
            this.rightChild = null;
        }

        // Constructor for internal nodes
        IsolationTree(int splitIndex, double splitValue, IsolationTree leftChild, IsolationTree rightChild) {
            this.splitIndex = splitIndex;
            this.splitValue = splitValue;
            this.leftChild = leftChild;
            this.rightChild = rightChild;
        }

        // Check if this node is a leaf
        boolean isLeaf() {
            return leftChild == null && rightChild == null;
        }
    }
}
