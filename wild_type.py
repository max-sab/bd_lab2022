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

        res = sequence.aggregate([
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
            }
        ])
        print(list(res))


def calculate_general_wild_type():
    client = pymongo.MongoClient(
        "mongodb+srv://evo:evolutional@evolutional.aweop.mongodb.net/genes?retryWrites=true&w=majority")
    db = client['genes']
    fasta_collection = db['position_fasta']
    res = fasta_collection.aggregate([
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
        }
    ])
    print(list(res))


def wild_type_to_rCRS_poly():
    client = pymongo.MongoClient(
        "mongodb+srv://evo:evolutional@evolutional.aweop.mongodb.net/genes?retryWrites=true&w=majority")
    db = client['genes']
    fasta_collection = db['position_fasta']
    fasta_collection.aggregate([
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
    ]

    )

    return ""
