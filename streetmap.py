import MySQLdb
import random
import pymongo
from pymongo import MongoClient


def output_path(way_nodes, way_id, outfile):
  output = ""
  for node_doc in way_nodes:
    output += "{a},{b}|".format(a=node_doc["lat"], b=node_doc["lon"])

  output = output + str(0) + "|"+str(way_id)
  outfile.write(output+"\n")

# CONNECT TO MongoDB
client = MongoClient()
db = client['bikemap']
ways = db['ways']
nodes = db['nodes']

# SETUP INDICES
print "Build index ways..."
ways.create_index([("id", pymongo.ASCENDING)])
ways.create_index([("nodes", pymongo.ASCENDING)])
print "Build index nodes..."
nodes.create_index([("id", pymongo.ASCENDING)])


# GET THE WAYS (Primary + secondard + tertiary + residential)
print "Total ways: " + str(ways.count())
way_ids = ways.find({"highway_type":"primary"}).distinct("id")
way_ids.extend(ways.find({"highway_type":"secondary"}).distinct("id"))
way_ids.extend(ways.find({"highway_type":"tertiary"}).distinct("id"))
way_ids.extend(ways.find({"highway_type":"residential"}).distinct("id"))
way_ids = way_ids
print "Filtered ways: " + str(len(way_ids))

outfile = open("plateau-ways.txt", "w")

counter= 0
for way_id in way_ids:
  counter = counter + 1
  print counter

  way_component_counter = 0

  for way_doc in ways.find({"id":way_id}):
    way_nodes = []

    should_output_way = True

    for node_id in way_doc["nodes"]:
      node_part = nodes.find({"id":node_id})[0]

      ## FILTER FOR A SECTION IN THE CITY.
      if node_part["lat"] > 45.528957 and node_part["lat"] < 45.533423 and node_part["lon"] > -73.614830 and node_part["lon"] < -73.584967:
        should_output_way = True


    if should_output_way:
      for node_id in way_doc["nodes"]:

        ## Sanity check
        if nodes.find({"id":node_id}).count() > 1:
          raise "wrong node count"

        node_doc = nodes.find({"id":node_id})[0]
        way_nodes.append(node_doc)

        neighbouring_ways_count = ways.find({"id":{"$ne":way_id}, "nodes":{"$in":[node_id]}, "highway_type":{"$in":["primary","secondary","tertiary","residential"]}}).count()

        if neighbouring_ways_count > 0:
          if len(way_nodes) > 1:
            way_component_counter = way_component_counter +1
            output_path(way_nodes, str(way_id)+"-"+str(way_component_counter), outfile)
            way_nodes = []
            way_nodes.append(node_doc)

outfile.close()
