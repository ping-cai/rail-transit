package domain;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

public class DirectedPath implements Serializable {
    private List<DirectedEdge> edges;
    private double totalCost;

    public DirectedPath() {
        edges = new LinkedList<DirectedEdge>();
        totalCost = 0;
    }

    public DirectedPath(List<DirectedEdge> edges) {
        this.edges = edges;
        for (int i = 0; i < edges.size(); i++) {
            this.totalCost += edges.get(i).getEdge().getWeight();
        }
    }

    public DirectedPath(LinkedList<DirectedEdge> edges, double totalCost) {
        this.edges = edges;
        this.totalCost = totalCost;
    }

    public List<DirectedEdge> getEdges() {
        return edges;
    }

    public void setEdges(LinkedList<DirectedEdge> edges) {
        this.edges = edges;
    }

    public double getTotalCost() {
        return totalCost;
    }

    public void setTotalCost(double totalCost) {
        this.totalCost = totalCost;
    }

    @Override
    public String toString() {
        return "{" +
                "\"path\":" + edges +
                ", \"totalCost\":" + totalCost +
                '}';
    }
}
