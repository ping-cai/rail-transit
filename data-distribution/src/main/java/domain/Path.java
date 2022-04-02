package domain;

import java.io.Serializable;
import java.util.LinkedList;

/**
 * The Path class implements A path in A weighted, directed graph as A sequence of Edges.
 * <p>
 * Created by Brandon Smock on 6/18/15.
 */
public class Path implements Cloneable, Comparable<Path>, Serializable {
    //    双向链表来表示边集
    private LinkedList<Edge> edges;
    //    整个双向链表表示叠加出来的物理路径费用
    private double totalCost;

    public void setEdges(LinkedList<Edge> edges) {
        this.edges = edges;
    }

    public void setTotalCost(double totalCost) {
        this.totalCost = totalCost;
    }

    public Path() {
        edges = new LinkedList<>();
        totalCost = 0;
    }

    public Path(LinkedList<Edge> edges) {
        this.edges = edges;
        totalCost = 0;
        for (Edge edge : edges) {
            totalCost += edge.getWeight();
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        if (edges.size() == 0) {
            return "pathList:[]";
        }
        edges.forEach(edge -> {
            String fromNode = String.format("%s->", edge.getFromNode());
            builder.append(fromNode);
        });
        Edge lastEdge = edges.getLast();
        String toNode = lastEdge.getToNode();
        builder.append(toNode);
        return String.format("pathList:[%s],cost:[%s unit length]", builder.toString(), totalCost);
    }

    public LinkedList<Edge> getEdges() {
        return edges;
    }

    public double getTotalCost() {
        return totalCost;
    }

    public void addFirst(Edge edge) {
        edges.addFirst(edge);
        totalCost += edge.getWeight();
    }

    public boolean equalsEdges(Path path2) {
        if (path2 == null)
            return false;

        LinkedList<Edge> edges2 = path2.getEdges();

        int numEdges1 = edges.size();
        int numEdges2 = edges2.size();

        if (numEdges1 != numEdges2) {
            return false;
        }

        for (int i = 0; i < numEdges1; i++) {
            Edge edge1 = edges.get(i);
            Edge edge2 = edges2.get(i);
            if (!edge1.getFromNode().equals(edge2.getFromNode()))
                return false;
            if (!edge1.getToNode().equals(edge2.getToNode()))
                return false;
        }

        return true;
    }

    public int compareTo(Path path2) {
        double path2Cost = path2.getTotalCost();
        if (totalCost == path2Cost)
            return 0;
        if (totalCost > path2Cost)
            return 1;
        return -1;
    }

    public Path clone() throws CloneNotSupportedException {
        return (Path) super.clone();
    }

    public Path cloneTo(int i) throws CloneNotSupportedException {
        LinkedList<Edge> edges = new LinkedList<>();
        int l = this.edges.size();
        if (i > l)
            i = l;

        //for (Edge edge : this.edges.subList(0,i)) {
        for (int j = 0; j < i; j++) {
            edges.add(this.edges.get(j).clone());
        }

        return new Path(edges);
    }

    public void addPath(Path p2) {
        // ADD CHECK TO SEE THAT PATH P2'S FIRST NODE IS SAME AS THIS PATH'S LAST NODE

        this.edges.addAll(p2.getEdges());
        this.totalCost += p2.getTotalCost();
    }

}
