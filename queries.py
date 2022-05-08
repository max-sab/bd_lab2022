from typing import Any, Mapping
import pymongo

from pymongo.database import Database


def get_haplogroups(db: Database[Mapping[str, Any]], regions=None, databases=None,):
  find_by = None
  if (regions is None) or (len(regions) == 0):
    find_by = {
      "$in": ["$database", databases]
    }
  if (databases is None) or (len(databases) == 0):
    find_by = {
      "$in": ["$region_cypher", regions]
    }
  return list(db['sequence'].aggregate([
    {
      "$match": {
        "$expr": find_by
      }
    },
    {
      "$group": {
        "_id": "$haplogroup",
        "count": {"$count": {}}
      }
    },
    {
      "$sort": {
        "_id": 1
      }
    }
  ]))


def each_with_each(db: Database[Mapping[str, Any]], regions=None, databases=None,):
  find_by = None
  if (regions is None) or (len(regions) == 0):
    find_by = {
      "$in": ["$database", databases]
    }
  if (databases is None) or (len(databases) == 0):
    find_by = {
      "$in": ["$region_cypher", regions]
    }

  print(find_by)
  sequence = db['sequence']
  data = list(sequence.aggregate([
    {
      "$match": {
        "$expr" : find_by
      }
    },
    {
      "$lookup": {
        "from": "fasta",
        "localField": "fasta_id",
        "foreignField": "_id",
        "as": "fasta_obj"
      }
    },
    {"$unwind": "$fasta_obj"},
    {
      "$project": {
        "_id": "$fasta_id",
        "fasta": "$fasta_obj.fasta"
      }
    },
    {
      "$lookup": {
        "from": "fasta",
        "let": {
          "seq_id": "$_id",
          "fasta2": "$fasta"
        },
        "pipeline": [
          {
            "$match": {
              "$expr": {
                "$gt": [
                  "$_id",
                  "$$seq_id"
                ]
              }
            }
          }
        ],
        "as": "fasta2"
      }
    },
    {
      "$match": {
        "fastas": {
          "$not": {
            "$size": 0
          }
        }
      }
    },
    {
      "$unwind": "$fasta2"
    },
    {
      "$project": {
        "_id": 1,
        "fasta": 1,
        "fasta2": "$fasta2.fasta",
        "length": {
          "$reduce": {
            "input": {
              "$range": [
                0,
                377,
                1
              ]
            },
            "initialValue": 0,
            "in": {
              "$sum": [
                "$$value",
                {
                  "$abs": {
                    "$strcasecmp": [
                      {
                        "$substr": [
                          "$fasta",
                          "$$this",
                          1
                        ]
                      },
                      {
                        "$substr": [
                          "$fasta2.fasta",
                          "$$this",
                          1
                        ]
                      }
                    ]
                  }
                }
              ],

            }
          }
        }
      }
    },
    {
      "$group": {
        "_id": "$length",
        "count": {
          "$count": {}
        }
      }
    },
    {
      "$sort": {
        "_id": 1
      }
    }
  ]))
  print(data)
  return data


if __name__ == '__main__':
  client = pymongo.MongoClient(
    "mongodb+srv://evo:evolutional@evolutional.aweop.mongodb.net/genes?retryWrites=true&w=majority")
  db = client['genes']
  each_with_each(db,[],["Ukraine"])
