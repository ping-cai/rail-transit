package domain;

import java.io.Serializable;

public class DirectedEdge implements Serializable {
    private Edge edge;
    private String direction;

    public DirectedEdge() {
    }

    public DirectedEdge(Edge edge, String direction) {
        this.edge = edge;
        this.direction = direction;
    }

    public Edge getEdge() {
        return edge;
    }

    public void setEdge(Edge edge) {
        this.edge = edge;
    }

    public String getDirection() {
        return direction;
    }

    public void setDirection(String direction) {
        this.direction = direction;
    }

    @Override
    public String toString() {
        return "{" +
                "\"edge\":" + "{" + "\"fromNode\":\"" + edge.getFromNode() + "\",\"toNode\":\"" + edge.getToNode() + "\", \"weight\":" + edge.getWeight() + "}" +
                ", \"direction\":'" + direction + '\'' +
                '}';
    }
}
