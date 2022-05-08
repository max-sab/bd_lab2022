import pymongo

def calculate_wild_type_region(region: str):
    client = pymongo.MongoClient(
        "mongodb+srv://evo:evolutional@evolutional.aweop.mongodb.net/genes?retryWrites=true&w=majority")
    db = client['genes']
    sequence = db['sequence']

    res = sequence.aggregate([
        {
            "$match":
                {
                    "region_cypher": region
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
