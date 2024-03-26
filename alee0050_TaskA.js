//Inserting Customer John Smith

db.Customer.insertOne(
{
    "_id": "C_0001",
    "name": "John Smith",
    
    "address": "4 Clayton Ave, Clayton, 3800, VIC",
    "contact": "0123456789"
}
)

//inserting Agent Harry Brown and the respective details
db.Agent.insertOne(
    {
        "_id": "A_0001",
        "name": "Harry Brown",
        "contact": "0123456788",
        "office_address": "4 Wellington Ave, Clayton, 3800, VIC"
    }
)
//inserting a booking done with the agent and the customer, what itinerary and its associated cost
db.Booking.insertOne(

    {
        "_id": "B_0001",
        "customer_id": "C_0001",
        "booking_date": "2023-09-12",
        "booking_agent": {
            "agent_id": "A_0001",
            "name": "Harry Brown"
        },
        "itinerary_id": "I_0001",
        "booking_cost": "$1200"
    }
    
)

//inserting the iterinary and its related activities.
db.Itinerary.insertOne(
    {
        "_id": "I_0001",
        "description": "Explore Malaysia and drop by Perth on the way",
        "startDate": "2024-09-12",
        "endDate": "2024-10-05",
        "totalCost": "$1250",
        "activities": [
            {
                "activity_id": "A_0001",
                "description": "Explore Perth",
                "numberOfDays": 3,
                "startDate": "2024-09-13",
                "endDate": "2024-09-16"
            },
            {
                "activity_id": "A_0002",
                "description": "Explore Malaysia",
                "numberOfDays": 7,
                "startDate": "2024-09-17",
                "endDate": "2024-10-04"
            }
        ]
    }
)

//inserting two documents regarding the activity, one is hired car and the other one is commerical flight
db.Transportation.insertMany([
    {
        "_id": "T_0001",
        "activity_id": "A_0002",
        "Type": "Hired Car",
        "bookingStartDate": "2024-09-12",
        "bookingEndDate": "2024-09-16",
        "usage": "Travel from melbourne to Perth and then travel around Perth"
    },
    {
        "_id": "T_0002",
        "activity_id": "A_0001",
        "Type": "Commercial Flight",
        "departureDate": "2024-09-16",
        "arrivalDate": "2024-09-17",
        "departureAirport": "Perth Airport",
        "arrivalAirport": "Malaysia Airport",
        "airline": "Malaysian Airlines"
    }
])


//inserting accomodation linked to the itinerary
db.Accommodation.insertOne(
{
    "_id": "ACC_001",
   	 "Accomodation_name": "Hilton Perth",
    	 "Accomodation_address": "27 Perth road, Perth 1111, WA",
   	 "Accomodation_check_in_date": "2024-09-13",
	"Accomodation_check_out_date": "2024-09-16",
	"itinerary_id": "I_0001"
})
