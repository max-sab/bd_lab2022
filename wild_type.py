from typing import Any, Mapping
import pymongo

from pymongo.database import Database


def get_wild_type(db: Database[Mapping[str, Any]], regions=None, databases=None, ):
    find_by = None
    if (regions is None) or (len(regions) == 0):
        find_by = {
            "$in": ["$database", databases]
        }
    if (databases is None) or (len(databases) == 0):
        find_by = {
            "$in": ["$region_cypher", regions]
        }
        client = pymongo.MongoClient(
            "mongodb+srv://evo:evolutional@evolutional.aweop.mongodb.net/genes?retryWrites=true&w=majority")
        db = client['genes']
        sequence = db['sequence']

        return list(sequence.aggregate([
            {
                "$match":
                    {
                        "$expr": find_by
                    }
            },
            {
                "$lookup": {
                    "from": "fasta",
                    "localField": "fasta_id",
                    "foreignField": "_id",
                    "as": "positions"
                }
            },
            {"$unwind": "$positions"},
            {
                "$project": {
                    "letters": {
                        "$map": {
                            "input": {
                                "$range": [
                                    0,
                                    {
                                        "$strLenCP": "$positions.fasta"
                                    }
                                ]
                            },
                            "as": "position",
                            "in": {
                                "position": "$$position",
                                "letter": {
                                    "$substr": [
                                        "$positions.fasta",
                                        "$$position",
                                        1
                                    ]
                                }
                            }
                        }
                    }
                }
            },
            {
                "$unwind": "$letters"
            },
            {
                "$project": {
                    "letter": "$letters.letter",
                    "position": "$letters.position",
                    "_id": 0
                }
            },
            {
                "$match": {"$expr": {"$lt": ["$position", 377]}}
            },
            {
                "$group": {
                    "_id": {
                        "position": "$position",
                        "letter": "$letter"
                    },
                    "count": {
                        "$sum": 1
                    }
                }
            },
            {
                "$sort": {
                    "count": -1
                }
            },
            {
                "$group": {
                    "_id": "$_id.position",
                    "letter": {
                        "$first": "$_id.letter"
                    },
                },
            },
            {
                "$sort": {
                    "_id": 1
                }
            },
            {
                "$group": {
                    "_id": None,
                    "letters": {
                        "$push": "$letter"
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "letters": {
                        "$reduce": {
                            "input": "$letters",
                            "initialValue": "",
                            "in": {
                                "$concat": [
                                    "$$value",
                                    "$$this"
                                ]
                            }
                        }
                    }
                }
            },
        ]))


def population_to_base_poly(db: Database[Mapping[str, Any]], name: str, regions=None, databases=None, ):
    sequence = db['sequence']

    find_by = None
    if (regions is None) or (len(regions) == 0):
        find_by = {
            "$in": ["$database", databases]
        }
    if (databases is None) or (len(databases) == 0):
        find_by = {
            "$in": ["$region_cypher", regions]
        }

    base_sequence = db['base_sequence'].find_one({"name": name})["fasta"]
    return list(sequence.aggregate([
        {
            "$match":
                {
                    "$expr": find_by
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
            "$project": {
                "_id": 1,
                "indexes_of_difference": {
                    "$reduce": {
                        "input": {
                            "$range": [
                                0,
                                377,
                                1
                            ]
                        },
                        "initialValue": [],
                        "in": {
                            "$cond": {
                                "if": {
                                    "$ne":
                                        [{
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
                                                            base_sequence,
                                                            "$$this",
                                                            1
                                                        ]
                                                    }
                                                ]
                                        }, 0]
                                },
                                "then": {"$concatArrays": ["$$value", ["$$this"]]},
                                "else": {"$concatArrays": ["$$value", []]}},
                        }
                    }
                }
            }
        },
        {"$unwind": "$indexes_of_difference"},
        {"$group": {"_id": "$indexes_of_difference"}},
        {"$group": {"_id": None, "count": {"$count": {}}}},
        {
            "$project": {
                "_id": 0,
                "count": 1
            }
        }
    ]))

def wild_type_to_rCRS_poly():
    client = pymongo.MongoClient(
        "mongodb+srv://evo:evolutional@evolutional.aweop.mongodb.net/genes?retryWrites=true&w=majority")
    db = client['genes']
    sequence = db['sequence']

    wild_type = get_wild_type(db, ["BK"], [])[0]["letters"]
    print(wild_type)
    base_sequence = db['base_sequence'].find_one({"name": "ANDREWS"})["fasta"]
    res = sequence.aggregate([
        {
            "$project": {
                "_id": 1,
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
                                                    wild_type,
                                                    "$$this",
                                                    1
                                                ]
                                            },
                                            {
                                                "$substr": [
                                                    base_sequence,
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
            }
        },
        {
            "$sort": {
                "_id": 1
            }
        }
    ])

    print(list(res))
