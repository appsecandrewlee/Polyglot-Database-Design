import pprint

import pymongo
from pymongo import MongoClient 



#Please run python3 alee0050_task_C2.py to view results 

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client.alee0050_MNC

try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

#B2.1

#pipeline1 indicates the primary logic of mongodb aggregate query 
pipeline1 = [ 

 {
        "$group": { #groups have been put in " "
            "_id": {"$year": "$parkInfo.dateEst"}, #_id is also in " "
            "totalParks": {"$sum": 1}
        }
    },
    {
        "$sort": {"_id": -1} #this is all the same logic as applicable in alee0050_task_B.js
    },
    {
        "$project": { 
            "year": "$_id",
            "totalParks": 1,
            "_id": 0
        }
    }
]

#after we have executed the block of code that was in previously, we now want to store that in a variable
#this variable is called result1 
result1 = db.wildlife.aggregate(pipeline1)
#run through the code from pymongo db.collection.aggregate(variable)

#iterate through each of the documents, as x stands for the amount of documents
for x in result1:
    pprint.pprint(x) ##pprint it. 


#B2.2
pipeline2 = [
    {
         "$match": {
            "speciesCharacteristics.category": "Bird" 
 

        }
    },
    {
        "$group": { 
            "_id": "$parkInfo.parkName",  
            "birdCount": {"$sum": 1 }  
        }
    },
    {
        "$project": { 
            "parkName": "$_id",
            "birdCount": 1, 
            "_id": 0
        }
    }
]

result2 = db.wildlife.aggregate(pipeline2)

for x in result2:
    pprint.pprint(x)



#B2.3 (couldn't get the previous question to work, but if it worked this is how it would execute)

pipeline3 = [
]

result3 = db.wildlife.aggregate(pipeline3)
for x in result3:
    pprint.pprint(x)



#B2.4
pipeline4 = [

{ "$unwind": "$speciesCharacteristics.commonNames" },

    {
        "$group": {
            "_id": {
                "speciesID": "$speciesID", 
                "category": "$speciesCharacteristics.category"
            },
            "CommonNames": {"$sum": 1} 
        }
    },

    { "$sort": { "_id.category": 1, "CommonNames": -1 } },


    {
        "$group": {
            "_id": "$_id.category",
            "speciesID": { "$first": "$_id.speciesID" },
            "CommonNames": { "$first": "$CommonNames" }
        }
    },
    {
         "$sort": { "CommonNames": -1 }  
    },
    {
        "$project": {
            "speciesID": "$speciesID",
            "category": "$_id",
            "CommonNames": 1,
            "_id": 0
        },
    }

]

result4 = db.wildlife.aggregate(pipeline4)

for x in result4:
    pprint.pprint(x)



#B2.5
pipeline5 = [
    {
        "$group": { 
            "_id": {
                "park": "$parkName",
                "category": "$speciesCharacteristics.category",
                "order": "$speciesCharacteristics.order",
                "family": "$speciesCharacteristics.family"
            }
        }
    },
    {
        "$group": {
            "_id": "$_id.park",
            "distinctCat": { "$addToSet": "$_id.category" }, 
            "distinctOrder": { "$addToSet": "$_id.order" }, 
            "distinctFamily": { "$addToSet": "$_id.family" }
        }
    },
    {
        "$project": {
            "distinctCatCounter": { "$size": "$distinctCat" },
            "distinctOrderCounter": { "$size": "$distinctOrder" },  
            "distinctFamilyCounter": { "$size": "$distinctFamily" },
        }
    }
]

result5 = db.wildlife.aggregate(pipeline5)

for x in result5:
    pprint.pprint(x)