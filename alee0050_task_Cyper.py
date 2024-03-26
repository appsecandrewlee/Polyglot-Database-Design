from neo4j import GraphDatabase

#this is the connection port of which the server access is obtained through :SERVER STATUS
URI = "bolt://localhost:7689"
#credentials, as we are using the default user neo4j <username> and Pta59ypt123 <password> 
AUTH = ("neo4j", "Pta59ypt123")
driver = GraphDatabase.driver(URI, auth=AUTH)
#this is initializing the driver object using neo4j import 


#simple print statement to indicate which question it is 
print("B1\n")

#using the to be fair we only need records, but this is saved for future implementation where a developer would want to measure
#the efficiency through summary and the keys is just used to return default object
records, summary, keys = driver.execute_query("""
// MATCH all the species with the relationship known_as
MATCH (s:Species)-[r:KNOWN_AS]->(c:CommonName)
// match species found_in park relationship
MATCH (s)-[f:FOUND_IN]->(p:Park)
// common name should contain Coyote
WHERE c.common_name CONTAINS 'Coyote'

WITH p, c.common_name AS comName, SUM(f.population) AS tot
// Filter out single names by checking for the existence of a comma
WHERE comName CONTAINS ','
RETURN p.park_name AS `Park Name`, collect(comName) AS `Coyote Species`, tot AS `Coyote Population`
ORDER BY tot DESC;
    """
)
#this is just used to print all the records
for record in records:
    print(record)

print("\n")
print("B2\n")

#this is repetitive, see explanation above
records, summary, keys = driver.execute_query("""MATCH (s:Species)-[:HAS]->(c:conservationStatus{cs:"Endangered"})
MATCH (s)-[f:FOUND_IN]->(p:Park)

WITH COUNT(s) AS maxSpeciesCount,p
ORDER BY maxSpeciesCount DESC, p.area_in_acres DESC LIMIT 1

MATCH (nearby_park: Park) 
WHERE p <> nearby_park
                   
WITH 
    maxSpeciesCount, 
    p, 
    nearby_park,
    point({latitude: p.latitude, longitude: p.longitude}) AS p1,  
    point({latitude: nearby_park.latitude,longitude: nearby_park.longitude}) AS p2

WITH p, nearby_park, maxSpeciesCount, point.distance(p1, p2) AS dist
ORDER BY dist ASC
return p.park_name AS `Top parks with endangered species`, nearby_park.park_name AS `Other Parks`, round(dist/1000,2) AS `Distance(KM)`
LIMIT 3;
""")

for record in records:
    print(record)

print("\n")
print("B3\n")

#see explanation above.
records, summary, keys = driver.execute_query("""
MATCH (p:Park)-[r:LOCATED_IN]->(s:State)
WITH s.state_code AS `State Code`, 
     SUM(p.area_in_acres) AS `Total Area Covered (acres)`, 
     collect(p.park_code) AS `Park Codes`
ORDER BY `Total Area Covered (acres)` DESC
RETURN `State Code`, `Total Area Covered (acres)`, `Park Codes`
LIMIT 3;
""")

for record in records:
    print(record)