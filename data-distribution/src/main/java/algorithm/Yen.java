package algorithm;

import domain.Edge;
import domain.Graph;
import domain.Path;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Yen's algorithm for computing the K shortest loopless paths between two nodes in A graph.
 * <p>
 * Copyright (C) 2015  Brandon Smock (dr.brandon.smock@gmail.com, GitHub: bsmock)
 * <p>
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received A copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * <p>
 * Created by Brandon Smock on September 23, 2015.
 * Last updated by Brandon Smock on December 24, 2015.
 */
public final class Yen {
    /**
     * Computes the K shortest paths in A graph from node s to node t using Yen's algorithm
     *
     * @param graph       the graph on which to compute the K shortest paths from s to t
     * @param sourceLabel the starting node for all of the paths
     * @param targetLabel the ending node for all of the paths
     * @param K           the number of shortest paths to compute
     * @return A list of the K shortest paths from s to t, ordered from shortest to longest
     */
    public static List<Path> ksp(Graph graph, String sourceLabel, String targetLabel, int K) throws CloneNotSupportedException {
        // 先计算最短路径
        Path kthPath = Dijkstra.shortestPath(graph, sourceLabel, targetLabel);
        // 没有直接退出
        if (kthPath == null) {
            return null;
        }
        // 初始化k短路径的存储列表
        ArrayList<Path> ksp = new ArrayList<>();
        ksp.add(kthPath);
        PriorityQueue<Path> candidates = new PriorityQueue<Path>();
        // 迭代计算另外的k-1条路径
        for (int k = 1; k < K; k++) {
            Path previousPath = ksp.get(k - 1);
            if (previousPath == null) {
                return ksp;
            }
            /* 迭代（k-1）条最短路径中除目标节点外的所有节点；
            对于每个节点，通过临时修改图，然后运行Dijkstra算法，
            在修改后的图中找到节点和目标之间的最短路径，最多生成一条新的候选路径*/
            for (int i = 0; i < previousPath.getEdges().size(); i++) {
                // 初始化容器以存储此节点/迭代的修改（删除）边
                LinkedList<Edge> removedEdges = new LinkedList<>();

                // SpurNode=第（k-1）条最短路径中当前访问的节点
                String spurNode = previousPath.getEdges().get(i).getFromNode();

                // 根路径=到分支节点的（k-1）st路径的前缀部分
                Path rootPath = previousPath.cloneTo(i);

                /* 迭代所有（k-1）条最短路径 */
                for (Path p : ksp) {
                    Path stub = p.cloneTo(i);
                    // 检查此路径是否与（k-1）条最短路径具有相同的前缀/根
                    if (rootPath.equalsEdges(stub)) {
                        /* 如果是这样，请从图形中删除路径中的下一条边（稍后，这将强制SpurNode将根路径与未找到的后缀路径连接起来 */
                        Edge re = p.getEdges().get(i);
                        graph.removeEdge(re.getFromNode(), re.getToNode());
                        removedEdges.add(re);
                    }
                }

                /* 从图形中临时删除根路径中的所有节点，除了“spur”节点 */
                for (Edge rootPathEdge : rootPath.getEdges()) {
                    String rn = rootPathEdge.getFromNode();
                    if (!rn.equals(spurNode)) {
                        removedEdges.addAll(graph.removeNode(rn));
                    }
                }

                // Spur path=简化图中从Spur节点到目标节点的最短路径
                Path spurPath = Dijkstra.shortestPath(graph, spurNode, targetLabel);

                // 如果确定了一条新的岔路。。。
                if (spurPath != null) {
                    // 连接根路径和支路以形成新的候选路径
                    Path totalPath = rootPath.clone();
                    totalPath.addPath(spurPath);

                    // 如果之前未生成候选路径，添加它
                    if (!candidates.contains(totalPath))
                        candidates.add(totalPath);
                }

                // 恢复在此迭代过程中删除的所有边
                graph.addEdges(removedEdges);
            }

            /* 确定成本最低的候选路径 */
            boolean isNewPath;
            do {
                kthPath = candidates.poll();
                isNewPath = true;
                if (kthPath != null) {
                    for (Path p : ksp) {
                        // 检查此候选路径是否与以前找到的路径重复
                        if (p.equalsEdges(kthPath)) {
                            isNewPath = false;
                            break;
                        }
                    }
                }
            } while (!isNewPath);

            // 如果没有更多的候选路径，就停下来
            if (kthPath == null)
                break;
            // 添加被标识为k最短路径的最佳、不重复的候选路径
            ksp.add(kthPath);
        }
        return ksp;
    }

}
