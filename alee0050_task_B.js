//B1.1

db.wildlife.updateMany( //updateMany just modifies all documents
    {"speciesCharacteristics.commonNames": {$regex: ","}}, //regular expression 
    //is a form of pattern matching to check if the token is equals to ,
[
    {
        $set: { //$set operator is ussed to set the data, in this context the 
            //the subarray of splitted commas, will be displayed here 
            "speciesCharacteristics.commonNames": {
                $split: ["$speciesCharacteristics.commonNames", ","]
            }
        }
    }
]
)

//B1.2
db.wildlife.find().forEach(function(ts) { //iterate through each of the documents
    db.wildlife.updateMany( //update all for each of the documents taking in the id
        { _id: ts._id }, //_id reference to the each of the documents id in wildlife
        { $set: { ts: ts._id.getTimestamp() } } //get the timestamp from the id
    );
})


//B1.3
db.parks.updateMany(
    {}, // Match all documents in the collection
    [
        {$set: { //using set and unset to do main update operations
                dateEst: { //new field dateEst
                    $dateFromString: { //dateFromString will extract "YYYY-MM-DD"
                        dateString: { //parameter required for dateFromString
                            $concat: [ //concat
                                {$toString: "$parkEstYear"}, //year first
                                "-",
                                {$toString: "$parkEstMonth"}, //month
                                "-",
                                {$toString: "$parkEstDay"} //day
                            ]
                        }
                    }
                }
            }
        },
        {$unset: ["parkEstYear", "parkEstMonth", "parkEstDay"] //remove all 3 fields
        }
    ]
);


//B1.4
db.wildlife.aggregate([

    { $lookup: {
    from: "parks",  //using the collection parks
    localField: "parkName",  //this is like a foreign key concept
    foreignField: "parkName", //using left join needs a foreign key
    as: "parkInfo" //save that as parkinfo, this is saved manually as an array
    } 
},
    {
    $unwind: "$parkInfo"  //now we use $unwind to desconstruct an array field to output docuemnt for each element
    },
    {
    
    $addFields: { //using add fields to add new fields to our collection for wildlife
        "areaInAcres": "$parkInfo.areaInAcres", 
        "latitude": "$parkInfo.latitude", 
        "longitude": "$parkInfo.longitude",
        "parkName": "$parkInfo.parkName",
        "dateEst": "$parkInfo.dateEst",
        "stateCode": "$parkInfo.stateCode"
        }
    },
    {
    $project: {
        "parkInfo._id": 0,//exclude the id in the array that contains park collection of ._id 
        "areaInAcres": 0, //all these fields are not showing
        "latitude": 0, 
        "longitude": 0, 
        "dateEst": 0, 
        "stateCode": 0
    }

    },

    {
        $out: "wildlife" //using $out the results of aggregation of previous pipeline is saved into a new collection, or replaced.
    }

]);


//B1.5
db.runCommand({ //it uses the current DB to run whatever logic that is used below
    collMod: "wildlife",  //collMod is used to validate 
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["speciesParkRecords"],
            properties: {
                speciesParkRecords: {
                    bsonType: "object",
                    required: ["recordStatus","occurrence","nativeness","abundance"],
                    properties: {
                        recordStatus: {
                            bsonType: "string",
                            enum: [' *',
                            ' American Crow',
                            ' Bluebell',
                            ' Bushtit',
                            ' Cabezon',
                            ' Catbird',
                            ' Cenizo',
                            ' Chico',
                            ' Claret Cup',
                            ' Clover Bush',
                            ' Cocodrilo De Tumbes',
                            ' Common Mullein',
                            ' Common Poorwill',
                            ' Cranesbill',
                            ' Dames Rocket',
                            " Devil's Shoelaces",
                            ' Downy Chess',
                            ' Filaree',
                            ' Fringed Sage',
                            ' Golden Pea',
                            ' Goosefoot',
                            ' Grass-Leaf Loco',
                            ' Ground Daisy',
                            ' Kinnikinnick',
                            ' Leather Flower',
                            ' Liver Leaf*',
                            ' Manati',
                            ' Northern Goshawk',
                            ' Northern Pintail',
                            ' Osha',
                            ' Pigeon Hawk',
                            ' Purple Cockle',
                            " Ranchers' Fireweed",
                            ' Robin',
                            ' Rushpink',
                            ' Shadbush',
                            ' Short-Tailed Weasel',
                            ' Skunkbush',
                            ' Skyrocket Gilia',
                            ' Speckled Trout',
                            ' Speedwell',
                            ' Storksbill',
                            ' Verdolagas',
                            ' Wapiti',
                            ' White-Footed Mouse',
                            ' Whortleberry',
                            ' Wild Iris',
                            ' Wild Rose',
                            ' Willowherb',
                            ' Wiregrass',
                            'Approved',
                            'In Review',
                            'None',
                            'P.Nut Sedge'],
                            description: "recordStatus not valid"
                        },
                        occurence: {
                            bsonType: "string",
                            enum: ['Approved',
                            'In Review',
                            'Not Confirmed',
                            'Not Present',
                            'Not Present (False Report)',
                            'Not Present (Historical Report)',
                            'Present'],
                            description: "occurence not valid"
                        },
                        nativeness:{
                            bsonType: "string",
                            enum: ['Native', 
                            'Not Confirmed', 
                            'Not Native',
                            'Present', 
                            'Unknown'],
                            description: "nativeness not valid"

                        },
                        abundance:{
                            bsonType: "string",
                            enum: ['Abundant',
                            'Common',
                            'Native',
                            'Not Native',
                            'Occasional',
                            'Rare',
                            'Uncommon',
                            'Unknown'],
                            description: "abundance not valid"
                        }
                    }
                }
            }
        }
    },
    validationLevel: "moderate",
    validationAction: "error"
})


//B2.1
db.wildlife.aggregate([
    {
        $group: {
            _id: {$year: "$parkInfo.dateEst"}, //since we already moved dateEst into parkinfo
            totalParks: {$sum: 1} //1 for true to gather sum of all documents in group
        }
    },
    {
        $sort: {_id: -1} //sort by descending order, this indicates size() - 1, which is the last position
    },
    {
        $project: { 
            year: "$_id",  //display what year in the grouping
            totalParks: 1, //display totalparks
            _id: 0 //no need for id for the year, since year is already there
        }
        
    },
    
]).explain()


//B2.2
db.wildlife.aggregate([
    {
        $match: {
            "speciesCharacteristics.category": "Bird" 
            //match the speciescharacteristics category
            //it can be found using db.wildlife.distinct("speciesCharacteristics.category")
            //this will show all the categories that is in db wildlife

        }
    },
    {
        $group: { 
            _id: "$parkInfo.parkName",  //group them up by the parkName
            birdCount: {$sum: 1 } //count all the birds by adding one incremental, this would indicate for each document it iterates through
            //add 1 to a counter
        }
    },
    {
        $project: { 
            parkName: "$_id", //display id as its parkName
            birdCount: 1, //display birdCount
            _id: 0 //don't display ID as there is no need
        }
    }
]).explain()

//B2.3
db.wildlife.updateMany({},[ //given by the question that we are able to modify our collection

    {
        $set: { //I have opted in to create a field in parkInfo array of GeoJsonLocation
            "parkInfo.GeoJsonLocation": {
                type: "Point", //it is a point, of which longtitude and latitude of each record is stored
                coordinates: ["$parkInfo.longitude","$parkInfo.latitude"]
            }
        }
    }
]
)

db.wildlife.createIndex({"parkInfo.GeoJsonLocation": "2dsphere"}); //then we create an index for this "2dsphere"
//this will allow us to calculate the distance gathered at one point, and do calculations with radian to find out the distance in one point


//https://stackoverflow.com/questions/38112376/find-closest-entries-in-mongodb-to-coordinates
db.wildlife.find({  
    "parkInfo.GeoJsonLocation": { 
        $nearSphere: { 
            $geometry: { 
                type: "Point", 
                coordinates: [-95.8412, 42.1109] //couldn't get this to work
                //using the co-ordinates from example lab, and stack overflow for the query
            }, 
            $maxDistance: 400000  // 400km is the max distance from point of coords
        } 
    } 
});



//B2.4
db.wildlife.aggregate([
    // Deconstruct the array of speciesCharacteristics and creates a seperate document for commonNames
    { $unwind: "$speciesCharacteristics.commonNames" },

    {
        $group: { //group SpeciesID and category
            _id: {
                speciesID: "$speciesID", 
                category: "$speciesCharacteristics.category"
            },
            CommonNames: {$sum: 1} 
        }
    },


    //this sorts  by category first, and then by commonNames
    { $sort: { "_id.category": 1, CommonNames: -1 } },



    //group them together based on the categories
    //everytime a new group is created, we only keep the ones with the most commonNames
    {
        $group: {
            _id: "$_id.category",
            speciesID: { $first: "$_id.speciesID" },
            CommonNames: { $first: "$CommonNames" }
        }
    },
    {
        //sorting by descending order
         $sort: { CommonNames: -1 }  
    },
    {
        //display results
        $project: {
            speciesID: "$speciesID",
            category: "$_id",
            CommonNames: 1,
            _id: 0
        },
    },
    {
        //$out creates the results based on collection "C4"
        $out: "C4"
    }
]).pretty().explain();



//B2.5
db.wildlife.aggregate([
    {
        $match: {
            "speciesCharacteristics.commonNames": /^a/i //matches everything that starts with "a" case insensitive
        }
    },
    {
      $group: { //group them with parkName, category, order and family
        _id: {
          park: "$parkName",
          category: "$speciesCharacteristics.category",
          order: "$speciesCharacteristics.order",
          family: "$speciesCharacteristics.family"
        }
      }
    },
    {
    //https://stackoverflow.com/questions/16368638/mongodb-distinct-aggregation
    //"$addToSet is used to count the number of distinct objects"
      $group: {
        _id: "$_id.park",
        distinctCat: { $addToSet: "$_id.category" }, //addToSet functions like a hashset, where it takes in distinct values, if its duplicate
        //it will discard 
        distinctOrder: { $addToSet: "$_id.order" }, 
        distinctFamily: { $addToSet: "$_id.family" }
      }
    },
    {
        //display results
        $project: {
          "distinctCatCounter": { "$round": [{ "$size": "$distinctCat" }, 2] }, //round to 2 decimal places
          "distinctOrderCounter": { "$round": [{ "$size": "$distinctOrder" }, 2] }, //$size indicates the size of the 
          "distinctFamilyCounter": { "$round": [{ "$size": "$distinctFamily" }, 2] }
        }
      },
      {
        //create a collection based on aggregate pipeline in collection "C4"
        $out: "C5"
      }
    
  ]).pretty().explain();

  

  
