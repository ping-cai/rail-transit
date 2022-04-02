package domain;
/**
 * The Node class implements A node in A directed graph keyed on A label of type String, with adjacency lists for
 * representing edges.
 * <p>
 * Created by brandonsmock on 5/31/15.
 */

import java.io.Serializable;
import java.util.*;

public class Node implements Serializable {
    protected String label;
    protected Map<String, Double> neighbors;
    // adjacency list, with HashMap for each edge weight

    public Node() {
        neighbors = new HashMap<>();
    }

    public Node(String label) {
        this.label = label;
        neighbors = new HashMap<>();
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public Map<String, Double> getNeighbors() {
        return neighbors;
    }

    public void setNeighbors(Map<String, Double> neighbors) {
        this.neighbors = neighbors;
    }

    public void addEdge(String toNodeLabel, Double weight) {
        neighbors.put(toNodeLabel, weight);
    }

    public double removeEdge(String toNodeLabel) {
        if (neighbors.containsKey(toNodeLabel)) {
            double weight = neighbors.get(toNodeLabel);
            neighbors.remove(toNodeLabel);
            return weight;
        }

        return Double.MAX_VALUE;
    }

    public Set<String> getAdjacencyList() {
        return neighbors.keySet();
    }

    public LinkedList<Edge> getEdges() {
        LinkedList<Edge> edges = new LinkedList<Edge>();
        for (String toNodeLabel : neighbors.keySet()) {
            edges.add(new Edge(label, toNodeLabel, neighbors.get(toNodeLabel)));
        }

        return edges;
    }

    public String toString() {
        StringBuilder nodeStringB = new StringBuilder();
        nodeStringB.append(label);
        nodeStringB.append(": {");
        Set<String> adjacencyList = this.getAdjacencyList();
        Iterator<String> alIt = adjacencyList.iterator();
        Map<String, Double> neighbors = this.getNeighbors();
        while (alIt.hasNext()) {
            String neighborLabel = alIt.next();
            nodeStringB.append(neighborLabel);
            nodeStringB.append(": ");
            nodeStringB.append(neighbors.get(neighborLabel));
            if (alIt.hasNext())
                nodeStringB.append(", ");
        }
        nodeStringB.append("}");
        nodeStringB.append("\n");

        return nodeStringB.toString();
    }
}
