
// In order to start with this question, a basic understanding of graph algorithm is required
// we already know that each node is a location in this case it is a park, where vertex is considered the weight
// distance between one node to the other. However, we cannot traverse from that point if a node is not Connected
// essentially a node has to be connected with another node, and the total distance is necessary for calculation from that one node
// to the other, we have to use point again with point distance.

// we also need to perform a check to see that the longitude and latitude is not null, because of incomplete records.
// we don't need to be extensive about everything not being null because in this case it is just park to park
// then we use totaDist which is the point.distance calculation from current park to nearby_park
// put that in :CONNECTED precisely because we want to calculate the total distance or the vertexes between each node
// from start to end and we will use that later on to calculate the total distance

// we can do a match statement to gain a visualisation of all the possibilities that you can go starting from PEFO to GUMO
// this is before we implement shortestpath dijistra's algorithm
// how dijkstra's algorithm work, it is a greedy algorithm that will always go for the minimum weight to achieve going from one point
// to the other, which makes it very efficient, the complexity of dijkstra's is O(V + ElogV) where V = Vertex and E = Edges
// so it uses a form of priority queue to prioritise min using a heap data structure, we don't need to implement that because our library
// already takes care of it.
// our source node and target node is just start point and end point where totalCost is considered the weight, in this case the Distance
// that it takes to transverse from one point to the other.
// when dijkstra's algorithm is successfully executed, the path is only one because there is only one shortest path, so the path node
// is that one path with minimum distance that it has transversed.
// it also has to be bidirectional so you can come back from endpark to startpark as well


LOAD CSV WITH HEADERS FROM "file:///park_connections_a2.csv" AS park_row
MATCH (p:Park {park_code: park_row.`Park Code`}), (nearby_park:Park {park_code: park_row.`Connected Park Code`})
WHERE p.latitude IS NOT NULL AND p.longitude IS NOT NULL AND nearby_park.latitude IS NOT NULL AND nearby_park.longitude IS NOT NULL
WITH p, nearby_park,
    point.distance(
        point({latitude: toFloat(p.latitude), longitude: toFloat(p.longitude)}),
        point({latitude: toFloat(nearby_park.latitude), longitude: toFloat(nearby_park.longitude)})
    ) AS totalDist
MERGE (p)-[:CONNECTED {totalDist: totalDist}]->(nearby_park)
MERGE (nearby_park)-[:CONNECTED {totalDist: totalDist}]->(p);


CALL gds.graph.project(
    'myGraph',
    'Park',
    'CONNECTED',
    {
        relationshipProperties: 'totalDist'
    }
);

//This is the path for dijkstra's
MATCH path = (source:Park {park_code: 'PEFO'})-[:CONNECTED*]-(target:Park {park_code: 'GUMO'})
RETURN path LIMIT 1;


MATCH (source:Park {park_code: 'PEFO'}), (target:Park {park_code: 'GUMO'})
CALL gds.shortestPath.dijkstra.stream('myGraph', {
  sourceNode: source,
  targetNode: target,
  relationshipWeightProperty: 'totalDist'
})
YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
UNWIND nodeIds AS nodeId
MATCH (parkNode) WHERE id(parkNode) = nodeId
RETURN
  gds.util.asNode(sourceNode).park_name AS `Start Park`,
  gds.util.asNode(targetNode).park_name AS `End Park`,
  totalCost AS `Total Travel Distance`,
  collect(parkNode.park_name) AS `Other Parks`;


