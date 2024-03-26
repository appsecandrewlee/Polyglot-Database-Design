//MERGE statements essentially creates a brand new record if that record does not exist 


//using merge we are able to create 12 records with their unique attributes
//the attributes are called a variable name for future use and reference,which is different than laoding CSV where it is loaded
//dynamically. this is essentially hardcoding records which is fine for the current particular use case

//each of the node represents a record that is stored using the appendix A database, for an example john_smith
//john_smith is the name of the customer, and is considered a label, therefore we just use john_smith to cast it as a label 
//then we specify what type of node it is, in this case it is customers, then we insert all the details relating to that customer 


MERGE (john_smith:Customer {id: "c_001", name: "John Smith", contact: "+61412567890", address: "4 Clayton Ave, Clayton, 3800, Victoria"})
MERGE (harry_brown:Agent {id: "AG_001", name: "Harry Brown", contact: "0412345678", address: "4 Wellington Ave, Clayton, 3800, Victoria"})
MERGE (b:Booking {id: "B_0001", booking_date: "2023-09-12",cost:1200})
MERGE (itinerary: Itinerary {id: "l_0001",description: "Monash Malaysia Campus & Perth",start_date:"2024-10-05",end_date:"2024-10-10",total_cost:1250,number_of_activities:3})
MERGE (activity_1:Activity {id: "A_0001",description: "Explore Perth",number_of_days:2, location: "Perth, Western Australia"})
MERGE (activity_2:Activity {id: "A_0002",description: "Visit Monash Malaysia Campus",number_of_days:2, location: "Subang Jaya, Malaysia"})
MERGE (activity_3:Activity {id: "A_0003",description: "Return to Melbourne",number_of_days:1, location: "Tullamarine, Victoria"})
MERGE (transportation_1:Transportation {linked_id: "A_0001",id: "VH03HB",type: "Hired Car",departure_date:"2024-10-05",arrival_date:"2024-10-07",departure_loc:"Perth Airport",arrival_loc:"Perth Airport",company: "Hertz"})
MERGE (transportation_2:Transportation {linked_id: "A_0001",id: "QF7405",type: "Flight",departure_date:"2024-10-05",arrival_date:"2024-10-05",departure_loc:"MEL",arrival_loc:"PER",company: "Qantas"})
MERGE (transportation_3:Transportation {linked_id: "A_0003",id: "MH232",type: "Flight",departure_date:"2024-10-09",arrival_date:"2024-10-10",departure_loc:"KUL",arrival_loc:"MEL",company: "Malaysian Airlines"})
MERGE (accommodation1:Accommodation {linked_id: "A_0001",name:"Hilton Perth",address:"14 Mill St, Perth 6000, Western Australia, Australia",check_in:"2024-10-05",check_out:"2024-10-07"})
MERGE (accommodation2:Accommodation {linked_id: "A_0002",name:"Shangri-La Kuala Lumpur",address:"11 Jalan Sultan Ismail,Kuala Lumpur,50250,Malaysia",check_in:"2024-10-07",check_out:"2024-10-09"})

// Relationship creation
//a relationship is ()-[:r]-(), a relationship for john_smith with booking that we  can refer to is b, because we specified that 
//booking is b and we create a relationship that is between those, and we can do call apoc.meta.graph() to visualise 
//in a relationship, we can also store attributes but in this use case it is not necessary. 
//each of these concepts can be considered an object with attributes, which aligns with OOP programming from how I understand it
MERGE (john_smith)-[:BOOKS]->(b)
MERGE (harry_brown)-[:MANAGES]->(b)
MERGE (b)-[:CONTAINS]->(itinerary)
MERGE(itinerary)-[:INCLUDES]->(activity_1)
MERGE(itinerary)-[:INCLUDES]->(activity_2)
MERGE(itinerary)-[:INCLUDES]->(activity_3)
MERGE (activity_1)-[:HAS_TRANSPORTATION]->(transportation_1)
MERGE (activity_1)-[:HAS_TRANSPORTATION]->(transportation_2)
MERGE (activity_3)-[:HAS_TRANSPORTATION]->(transportation_3)
MERGE(activity_1)-[:HAS_ACCOMODATION]->(accommodation1)
MERGE(activity_2)-[:HAS_ACCOMODATION]->(accommodation2)
