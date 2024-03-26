import pymongo 
from pymongo import MongoClient
from neo4j import GraphDatabase
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json
import pprint

#MongoDB code

#####START 
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client.FIT3176_VPCode_Group34
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

#Code to connect to and create the Polyglot persistence Data Model [MongoDB]
#CSV FILES that is imported in mongoDB compass
movies = db['Movies']
tags = db['Tags']

#SETUP, storing our tags in our movie collection
pipeline1 = [ 
   {
      '$lookup': { #we can perform a lookup which again uses the database 'Tags'
         'from': 'Tags', #which we have imported in our MongoCompass
         'localField': 'movieId', #This is the ID that we are using, this ID exists both in Movies and in Tags
         'foreignField': 'movieId',
         'as': 'movieTags' #make that as movieTags 
      }
   },
   {
      '$addFields':{ #movieTags will contain the tag from Tags.csv
         'tags': '$movieTags.tag',
         'timestamp': '$movieTags.timestamp' #and timestamp
      }
   },
   {
      '$out': 'Movies' #write that to Movies
   }
]

db.Movies.aggregate(pipeline1) #execute 

#making sure operation is being performed
check = db.Movies.find() #this is to check the operation that is currently performing
for records in check: #to iterate through each record
   pprint.pprint(records) #print that record out 



#Code to query and display query outputs using the Polyglot persistence Data Model [MongoDB]
#Look up the movie with the genre "Fantasy"
pipeline2 = [
   {
      '$match': {
         'genres' : 'Fantasy'
      }
   }
]

#We do this because we want to mimick a user lookup ability for "Fantasy"
res = movies.aggregate(pipeline2)
#if a normal user uses the platform, it should display all the movies that has the genre "Fantasy"
for r in res: 
   pprint.pprint(r)

#####END


####START 
URI = "bolt://localhost:7689"
AUTH = ("neo4j", "Pta59ypt123")
driver = GraphDatabase.driver(URI, auth=AUTH)




#Neo4j code
#Code to connect to and create the Polyglot persistence Data Model [Neo4j]

#SETUP NODES on neo4j ratings and movies
# LOAD CSV WITH HEADERS FROM "file:///ratings.csv" AS row 
# WITH row
# WHERE row.userId IS NOT NULL 
# AND row.movieId IS NOT NULL
# AND row.rating IS NOT NULL
# AND row.timestamp IS NOT NULL 

# MERGE(user: User {id: row.userId})
# MERGE(movie: Movie {id: row.movieId})
# MERGE(rating: Rating {value: row.rating})
# MERGE(timestamp: Timestamp {time: row.timestamp});
# LOAD CSV WITH HEADERS FROM "file:///movies.csv" AS row 
# WITH row
# WHERE row.movieId IS NOT NULL 
# AND row.title IS NOT NULL
# AND row.genres IS NOT NULL

# MATCH(movie: Movie {id: row.movieId})
# SET movie.title = row.title, movie.genres = row.genres
# MERGE(user)-[r:HAS_RATED {rating: coalesce(row.rating, 0), time: coalesce(row.timestamp, '2023-11-11T00:00:00Z')}]->(movie)

####END 

#Code to query and display query outputs using the Polyglot persistence Data Model [Neo4j]




#Cassandra code
###START

#given documentation to use AstraDB with python 
cloud_config= {
  'secure_connect_bundle': 'secure-connect-fit3176-vpcode-group34.zip'
}

with open("FIT3176_VPCode_Group34-token.json") as f:
    secrets = json.load(f)

CLIENT_ID = secrets["clientId"]
CLIENT_SECRET = secrets["secret"]

auth_provider = PlainTextAuthProvider(CLIENT_ID, CLIENT_SECRET)
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

row = session.execute("select release_version from system.local").one()
if row:
  print(row[0] + " " + "This has been successfully connected to AstraDB")
else:
  print("An error occurred.")



#Code for Managing Transactions [1%]

#Creating a movie with the appropriate details [MongoDB]
createNewMovieTransaction = {
   'movieId': '999999999999999', #fieldname
   'title': 'FIT3176 Last Presentation',
   'genres': 'Academic',
   'tags':['Data Analysis', 'Information Technology']
}
db.Movies.insert_one(createNewMovieTransaction) 

#Lookup the movie with the appropriate details [MongoDB]
movieLookup = {'movieId': '999999999999999'}
lookupRes = db.Movies.find(movieLookup)

for record in lookupRes:
   print(record)

#Create a user id that has not watched anything [neo4j]
#Merge query to make the user, if it doesnt exist create one
new, summary, keys = driver.execute_query("""MERGE (user:User {id: '999999'})""")


for x in new:
    print(x)

#Update User with a rating, that is given to the movie [Cassandra]


#Create a relationship between the user and movie to indicate that the user has watched the movie [neo4j]
#MERGE to create relationship if there isnt one
new1, summary, keys = driver.execute_query("""MERGE (user:User {id:'999999'})-[r:HAS_WATCHED]->(movie:Movie {id:'999999999999999'})""")

for y in new1:
    print(y)


#Update user and movie node relationship with the rating attribute stored inside it [neo4j]
#merge to store the ratings inside the relationship, as well as creating a HAS_RATED relationship if there isnt one 
new2, summary, keys = driver.execute_query("""MATCH (user:User {id:'999999'})-[r:HAS_RATED]->(movie:Movie {id:'999999999999999'})
                                          SET r.rating = '5.0' RETURN user, movie, r""")

for z in new2:
    print(z)


#hypothetical recommendation engine
#if closeness property between user1 and user2 with movie id 99999999
#user3 can be recommended movie id 99999999


#Transaction done








