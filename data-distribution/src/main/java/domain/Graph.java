package domain;


/**
 * The Graph class implements A weighted, directed graph using an adjacency list representation.
 * <p>
 * Created by brandonsmock on 6/1/15.
 */

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Graph implements Serializable {
    private Map<String, Node> nodes;

    public Graph(Map<String, Node> nodes) {
        this.nodes = nodes;
    }

    public Graph(List<Edge> edges) {
        this.nodes = new HashMap<>();
        for (Edge edge : edges) {
            addEdge(edge);
        }
    }

    public Graph clone() {
        HashMap cloneMap = (HashMap) this.nodes;
        return new Graph((HashMap) cloneMap.clone());
    }


    public static Graph createGraph(List<Edge> edgeList) {
        return new Graph(edgeList);
    }

    public void addNode(String label) {
        if (!nodes.containsKey(label))
            nodes.put(label, new Node(label));
    }

    public void addEdge(String label1, String label2, Double weight) {
        if (!nodes.containsKey(label1))
            addNode(label1);
        if (!nodes.containsKey(label2))
            addNode(label2);
        nodes.get(label1).addEdge(label2, weight);
    }

    public void addEdge(Edge edge) {
        addEdge(edge.getFromNode(), edge.getToNode(), edge.getWeight());
    }

    public void addEdges(List<Edge> edges) {
        for (Edge edge : edges) {
            addEdge(edge);
        }
    }

    public Edge removeEdge(String label1, String label2) {
        if (nodes.containsKey(label1)) {
            double weight = nodes.get(label1).removeEdge(label2);
            if (weight != Double.MAX_VALUE) {
                return new Edge(label1, label2, weight);
            }
        }
        return null;
    }

    public Map<String, Node> getNodes() {
        return nodes;
    }

    public List<Edge> getEdgeList() {
        List<Edge> edgeList = new LinkedList<Edge>();

        for (Node node : nodes.values()) {
            edgeList.addAll(node.getEdges());
        }

        return edgeList;
    }

    public List<Edge> removeNode(String label) {
        LinkedList<Edge> edges = new LinkedList<Edge>();
        if (nodes.containsKey(label)) {
            Node node = nodes.remove(label);
            edges.addAll(node.getEdges());
            edges.addAll(removeEdgesToNode(label));
        }

        return edges;
    }

    public List<Edge> removeEdgesToNode(String label) {
        List<Edge> edges = new LinkedList<Edge>();
        for (Node node : nodes.values()) {
            if (node.getAdjacencyList().contains(label)) {
                double weight = node.removeEdge(label);
                edges.add(new Edge(node.getLabel(), label, weight));
            }
        }
        return edges;
    }
}