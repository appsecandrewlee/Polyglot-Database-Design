//LOADING CSV from species_a2
LOAD CSV WITH HEADERS FROM "file:///species_a2.csv" AS row 
//WITH row is to reference the value, the WITH statement is essentially calling a variable that exists and that has the latest 
//value from previous operations, as cypher queries cannot reference a variable like a normal programming language, WITH will take the previous 
//LOAD csv statement AS row, and it reads CSV row by row when it is iterating through the csv file. 
WITH row 

WHERE row.`Species ID` IS NOT NULL AND TRIM(row.`Species ID`) <> ""
MERGE (species:Species {species_id: row.`Species ID`})


//this ensures that our species ID is not a incomplete record and we trim any trailing white spaces 
//we have different edge cases for an example a species ID can be : "    " which is just white space, but it counts as a character still
//we don't want that, and we use the inequality comparison operator for that, if there is a white space then we trim it
//merging is just creating something that does not exist, if it does that this does nothing. it is a form of update as update queries 
//can make new if something does not exist, or it simply leaves it alone if it does exist with the same record


WITH row //current row
UNWIND split(COALESCE(row.`Common Names`,"N/A"),',') AS new 
MERGE (c:CommonName {common_name: trim(new)})

//in this case we are using UNWIND to iterate through the Common names in our CSV file, if there is an empty or null value
//in our common names, we want to split that and call the empty/null val next, because we don't want to use that as our value
//we will then call split to split that into individual common names, trim just removes anything that is next
//for an example our input can be "Eastern Coyote, Coyote SCAT", we want to seperate those groupping Eastern Coyote 
//and Coyote SCAT individually from the our rows

WITH row
WHERE row.`Scientific Name` IS NOT NULL
AND row.Occurrence IS NOT NULL
AND row.Abundance IS NOT NULL
AND row.Seasonality IS NOT NULL 
AND row.`Conservation Status` IS NOT NULL 
AND row.Nativeness IS NOT NULL
AND row.`Record Status` IS NOT NULL
AND row.Family IS NOT NULL
AND row.Order IS NOT NULL
AND row.Category IS NOT NULL

//we need to do this because we are not sure if any of the fields can be missing, if you manually check there are actually missing fields 
//that is obviously an incomplete record, it is something that we don't want when we are doing data cleaning 

MERGE(Scientific: ScientificName {scientific: row.`Scientific Name`})
MERGE(occurence: Occurrence{o:row.Occurrence})
MERGE(abundance: Abundance{a:row.Abundance})
MERGE(seasonality: Seasonality{s:row.Seasonality})
MERGE(conservationStatus: conservationStatus{cs:row.`Conservation Status`})
MERGE(native: Native{n:row.Nativeness})
MERGE(recordStatus:RecordStatus {rs: row.`Record Status`})
MERGE(family:Family{f:row.Family})
MERGE(order:Order{ord:row.Order})
MERGE(category:Category{cat:row.Category});

//MERGE statements is just creating reference nodes taken from species_a2, we are creating a node for every single column
//if we do a MATCH statement using n, n is a simply an abstract representation of all the nodes, and we return it we will be able to see
//every single node that has been created using the CSV file, we can also call apoc.meta.graph() to display a high



// Load parks CSV
LOAD CSV WITH HEADERS FROM "file:///parks_a2.csv" AS park_row
WITH park_row
WHERE park_row.`Park Name` IS NOT NULL
AND park_row.`Park Code` IS NOT NULL 
AND park_row.`State Code` IS NOT NULL
AND park_row.`Area in Acres` IS NOT NULL
AND park_row.Latitude IS NOT NULL 
AND park_row.Longitude IS NOT NULL 
AND park_row.`State Name` IS NOT NULL
AND park_row.`Est Day` IS NOT NULL
AND park_row.`Est Month` IS NOT NULL
AND park_row.`Est Year` IS NOT NULL

MERGE (state:State {state_code: park_row.`State Code`, state_name: park_row.`State Name`})
MERGE (park:Park {
  park_code: park_row.`Park Code`,
  park_name: park_row.`Park Name`,
  area_in_acres: toInteger(park_row.`Area in Acres`),
  longitude: toFloat(park_row.Longitude),
  latitude: toFloat(park_row.Latitude),
  est_date: date({
    year: toInteger(park_row.`Est Year`),
    month: toInteger(park_row.`Est Month`),
    day: toInteger(park_row.`Est Day`)
  })
});



//relationships 


LOAD CSV WITH HEADERS FROM "file:///parks_a2.csv" AS row
WITH row
WHERE row.`Park Code` IS NOT NULL
AND row.`State Code` IS NOT NULL
AND row.`Area in Acres` IS NOT NULL



MATCH (park: Park {park_code: row.`Park Code`}),
(state: State {state_code: row.`State Code`})
MERGE (park)-[:LOCATED_IN {area_covered: toInteger(row.`Area in Acres`)}]->(state);


//B3 
MERGE (parkDEVA:Park {park_code: "DEVA"})-[:LOCATED_IN {area_covered: 790152}]->(stateCA:State {state_code: "CA"})
MERGE (parkDEVA)-[:LOCATED_IN {area_covered: 3950760}]->(stateNV:State {state_code: "NV"})

MERGE (parkYELL:Park {park_code: "YELL"})-[:LOCATED_IN {area_covered: 521490}]->(stateWY:State {state_code: "WY"})
MERGE (parkYELL)-[:LOCATED_IN {area_covered: 521490}]->(stateMT:State {state_code: "MT"})
MERGE (parkYELL)-[:LOCATED_IN {area_covered: 521490}]->(stateID:State {state_code: "ID"})

MERGE (parkGRSM:Park {park_code: "GRSM"})-[:LOCATED_IN {area_covered: 510390}]->(stateTN:State {state_code: "TN"})
MERGE (parkGRSM)-[:LOCATED_IN {area_covered: 11100}]->(stateNC:State {state_code: "NC"});



LOAD CSV WITH HEADERS FROM "file:///species_a2.csv" AS row
WITH row

WITH row
MATCH (species:Species {species_id: row.`Species ID`})
MATCH (Scientific:ScientificName {scientific: row.`Scientific Name`})
MERGE (species)-[:BELONGS_TO]->(Scientific)
WITH row
MATCH (species:Species {species_id: row.`Species ID`})
MATCH (occurence:Occurrence {o:row.Occurrence})
MERGE (species)-[:HAS]->(occurence)
WITH row
MATCH (species:Species {species_id: row.`Species ID`})
MATCH (recordStatus:RecordStatus {rs: row.`Record Status`})
MERGE (species)-[:HAS]->(recordStatus)
WITH row
MATCH (species:Species {species_id: row.`Species ID`})
MATCH (conservationStatus:conservationStatus {cs: row.`Conservation Status`})
MERGE (species)-[:HAS]->(conservationStatus)
WITH row
MATCH (species:Species {species_id: row.`Species ID`})
MATCH (seasonality:Seasonality {s: row.Seasonality})
MERGE (species)-[:HAS]->(seasonality)
WITH row
MATCH (seasonality:Seasonality {s: row.Seasonality}), (park:Park {park_name: row.`Park Name`})
MERGE (seasonality)-[:IN]->(park)
WITH row
MATCH (species:Species {species_id: row.`Species ID`})
MATCH (abundance:Abundance {a: row.Abundance})
MERGE (species)-[:HAS]->(abundance)
WITH row
MATCH (species:Species {species_id: row.`Species ID`})
MATCH (native:Native {n: row.Nativeness})
MERGE (species)-[:HAS]->(native)
WITH row
MATCH (Scientific:ScientificName {scientific: row.`Scientific Name`})
MATCH (family:Family {f: row.Family})
MERGE (Scientific)-[:BELONGS_TO]->(family)
WITH row
MATCH (family:Family {f: row.Family})
MATCH (order:Order {ord: row.Order})
MERGE (family)-[:BELONGS_TO]->(order)
WITH row
MATCH (order:Order {ord: row.Order})
MATCH (category:Category {cat: row.Category})
MERGE (order)-[:BELONGS_TO]->(category)
WITH row
MATCH (native:Native {n: row.Nativeness})
MATCH (park:Park {park_name: row.`Park Name`})
MERGE (native)-[:IN]->(park)
WITH row
MATCH (abundance:Abundance {a: row.Abundance})
MATCH (park:Park {park_name: row.`Park Name`})
MERGE (abundance)-[:IN]->(park)
WITH row
MATCH (occurence:Occurrence {o: row.Occurrence})
MATCH (park:Park {park_name: row.`Park Name`})
MERGE (occurence)-[:IN]->(park)
WITH row
MATCH (recordStatus:RecordStatus {rs: row.`Record Status`})
MATCH (park:Park {park_name: row.`Park Name`})
MERGE (recordStatus)-[:IN]->(park)
WITH row
MATCH (conservationStatus:conservationStatus {cs: row.`Conservation Status`})
MATCH (park:Park {park_name: row.`Park Name`})
MERGE (conservationStatus)-[:IN]->(park)
WITH row
MATCH (species:Species {species_id: row.`Species ID`})
MATCH (park:Park {park_name: row.`Park Name`})
MERGE (species)-[:FOUND_IN {population: toInteger(row.`Species Population`)}]->(park)

WITH row
MATCH (species:Species {species_id: row.`Species ID`})
MERGE (common_name:CommonName {common_name: row.`Common Names`})
MERGE (species)-[:KNOWN_AS]->(common_name);



//B.2. Querying the Database:
//(i)

// First, we want to extract every single instance that is dictated by a general graph structure of our database
// species has a one to many relationship to common names, as one species can be known by multiple commonnames 
// we then want to match the species that are found in specific parks
// which species of which park they belong to etc, this will set up our query to find distinct parks using MATCH
// MATCH is a form of grep, pattern matching algorithm that is used for both nodes and relationships
// We want to use MATCH Neo4j iterates through rows one by one, as dictated in our LOAD CSV statement.
// Our next step, we will tell neo4j to iterate through each row and lookfor common names that contain 'Coyote'.
// Coyote species known as "Eastern Coyote" and also known as "Coyote Scat," both of these names
// using a WHERE clause, we can filters out all the rows that contain 'Coyote.'
// Understanding WITH is a important part of the operation, as variables need to use WITH clause in order to reference the variables that were accessed
// from the previous part of the query. We want to sum up all the populations of Coyotes for each park,
// and then we label the common_name AS `comName`. We will change that shortly when we return it, for readability we use a name instead of referencing c.common_name
// we will use another WHERE clause to filter out if comName contains a comma, this will indicate that it is known by two names rather than one 
// for an example, a species can go by "Eastern Coyote, Coyote SCAT", there is a comma to indicate that it is more than one name
// Now we want to return our results for Neo4j to display with our set name, using AS statement 
// then just order by descending order starting with population, this will give us the highest population first, second highest etc



//(i)

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



// For this question, we have to understand what exactly we are returning from our query, which is 3 columns/variables
// As long as we understand that, we can find those 3 variables from our CSV
// we need to find parks that contains endangered species, the closest parks near parks of endangered species and the distance
// So we will use a match statement again, if we look into our CSV we can find that there is a column species of concern
// which is a form of enumeration type that you cast on a variable to group, the concept feels the same here
// we have a group of endangered species that is labeled through the column species of concern that we can access with CSV
// we want to output the parks with the most species of that enumeration, this is not to have the population, but the specific
// this can be found using the relationship between species and conservationStatus, in order for us to access those variables
// then we want to know what species of endangered is found in what parks
// we use another relationship for species and parks, in which then we can use a COUNT clause
// to count the number of species that is endangered, this representation will then be called maxSpeciesCount
// again, WITH clause is used to access the variables, in this case all the columns from parks_a2.csv
// we will then order that first by descending with the area_in_acres, this area_in_acres = row.`Areas in Acre`
// we can then begin to gather parks that is not the current park, and we will call it nearby_park
// a point consists of latitude and longitude, to which one point to the other would be the distance
// from the parks, the longitude and latitude is described as the center point, so we can assume
// that the points are where the parks are located, and the distance between two points will be used for
// point distance calculation, of total distance then we refer that back using the WITH clause after we have
// completed all of our operations and simply return them As a specific name, to top 3 parks with LIMIT clause


MATCH (s:Species)-[:HAS]->(c:conservationStatus{cs:"Endangered"})
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



//B.3. Modifying the Database (After Modification Query):

MATCH (p:Park)-[r:LOCATED_IN]->(s:State)
WITH s.state_code AS `State Code`, 
     SUM(p.area_in_acres) AS `Total Area Covered (acres)`, 
     collect(p.park_code) AS `Park Codes`
ORDER BY `Total Area Covered (acres)` DESC
RETURN `State Code`, `Total Area Covered (acres)`, `Park Codes`
LIMIT 3;

