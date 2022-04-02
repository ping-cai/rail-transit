package algorithm;

import domain.*;

import java.util.Map;
import java.util.PriorityQueue;

/**
 * @apiNote
 */
public final class Dijkstra {

    private Dijkstra() {
    }

    public static Path shortestPath(Graph graph, String sourceLabel, String targetLabel) {
        Map<String, Node> nodes = graph.getNodes();
        ShortestPathTree predecessorTree = new ShortestPathTree(sourceLabel);
        PriorityQueue<DijkstraNode> pq = new PriorityQueue<>();
        for (String nodeLabel : nodes.keySet()) {
            DijkstraNode newNode = new DijkstraNode(nodeLabel);
            newNode.setDist(Double.MAX_VALUE);
            newNode.setDepth(Integer.MAX_VALUE);
            predecessorTree.add(newNode);
        }

        DijkstraNode sourceNode = predecessorTree.getNodes().get(predecessorTree.getRoot());
        if (sourceNode == null) {
            return null;
        }
        sourceNode.setDist(0);
        sourceNode.setDepth(0);
        pq.add(sourceNode);

        while (!pq.isEmpty()) {
            DijkstraNode current = pq.poll();
            String currLabel = current.getLabel();
            if (currLabel.equals(targetLabel)) {
                Path shortestPath = new Path();
                String currentN = targetLabel;
                String parentN = predecessorTree.getParentOf(currentN);
                while (parentN != null) {
                    shortestPath.addFirst(new Edge(parentN, currentN, nodes.get(parentN).getNeighbors().get(currentN)));
                    currentN = parentN;
                    parentN = predecessorTree.getParentOf(currentN);
                }
                return shortestPath;
            }
            Map<String, Double> neighbors = nodes.get(currLabel).getNeighbors();
            for (String currNeighborLabel : neighbors.keySet()) {
                DijkstraNode neighborNode = predecessorTree.getNodes().get(currNeighborLabel);
                Double currDistance = neighborNode.getDist();
                Double newDistance = current.getDist() + nodes.get(currLabel).getNeighbors().get(currNeighborLabel);
                if (newDistance < currDistance) {
                    DijkstraNode neighbor = predecessorTree.getNodes().get(currNeighborLabel);
                    pq.remove(neighbor);
                    neighbor.setDist(newDistance);
                    neighbor.setDepth(current.getDepth() + 1);
                    neighbor.setParent(currLabel);
                    pq.add(neighbor);
                }
            }
        }
        return null;
    }
}
