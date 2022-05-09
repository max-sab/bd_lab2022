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
    return list(db["sequence"].aggregate([
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


def wild_type_to_base_poly(db: Database[Mapping[str, Any]], name: str, regions=None, databases=None, ):
    sequence = db['sequence']

    wild_type = get_wild_type(db, regions, databases)[0]["letters"]
    base_sequence = db['base_sequence'].find_one({"name": name})["fasta"]
    print(base_sequence)
    print(wild_type)
    return list(sequence.aggregate([
        {
            "$project": {
                "_id": 0,
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
        }
    ]))


def find_percentage(db: Database[Mapping[str, Any]], task, distr_name):
    data = list(db['distributions'].aggregate([
        {
            "$match": {
                "name": distr_name,
                "task_id": task["number"]
            }
        },
        {
            "$unwind": "$distances"
        },
        {
            "$group": {
                "_id": None,
                "sum": {"$sum": "$distances.count"},
                "distances": {"$push": {"_id": "$distances._id", "count": "$distances.count"}}
            }
        },
        {
            "$unwind": {
                "path": "$distances",

            }
        },
        {
            "$project": {
                "_id": "$distances._id",
                "count": "$distances.count",
                "percent": {"$divide": ["$distances.count", "$sum"]},
            }
        }
    ]))
    print(data)
    return data


def calculate_formulas(db: Database[Mapping[str, Any]], task, distr_name):
    mat_spod = list(db['distributions'].aggregate([
        {
            "$match": {
                "name": distr_name,
                "task_id": task["number"]
            }
        }
    ]))
    return {
        mat_spod
    }


def get_haplogroups(db: Database[Mapping[str, Any]], regions=None, databases=None, ):
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


def each_with_each(db: Database[Mapping[str, Any]], task):
    find_by = None
    if (task['regions'] is None) or (len(task['regions']) == 0):
        find_by = {
            "$in": ["$database", task['databases']]
        }
    if (task['databases'] is None) or (len(task['databases']) == 0):
        find_by = {
            "$in": ["$region_cypher", task['regions']]
        }

        print(find_by)
        sequence = db['sequence']
        data = list(sequence.aggregate([
            {
                "$match": {
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
        db['distributions'].insert_one({
            'name': 'ewe',
            'distances': data,
            'task_id': task['number']
        })
        return data


if __name__ == '__main__':
    client = pymongo.MongoClient(
        "mongodb+srv://evo:evolutional@evolutional.aweop.mongodb.net/genes?retryWrites=true&w=majority")
    db = client['genes']
    print(wild_type_to_base_poly(db, "EVA", [], ["Ukraine"]))
