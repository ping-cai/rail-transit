package domain;

import java.io.Serializable;
import java.util.Objects;

/**
 * The Edge class implements standard properties and methods for A weighted edge in A directed graph.
 * <p>
 * Created by Brandon Smock on 6/19/15.
 */
public class Edge implements Cloneable, Serializable {
    private String fromNode;
    private String toNode;
    private double weight;

    public Edge() {
    }

    public Edge(String fromNode, String toNode) {
        this.fromNode = fromNode;
        this.toNode = toNode;
    }

    public Edge(String fromNode, String toNode, double weight) {
        this.fromNode = fromNode;
        this.toNode = toNode;
        this.weight = weight;
    }

    public void setFromNode(String fromNode) {
        this.fromNode = fromNode;
    }

    public String getFromNode() {
        return fromNode;
    }

    public String getToNode() {
        return toNode;
    }

    public void setToNode(String toNode) {
        this.toNode = toNode;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public double getWeight() {
        return weight;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Edge edge = (Edge) o;
        return Objects.equals(fromNode, edge.fromNode) &&
                Objects.equals(toNode, edge.toNode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fromNode, toNode);
    }

    public Edge clone() throws CloneNotSupportedException {
        return (Edge) super.clone();
    }
}