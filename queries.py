from typing import Any, Mapping

import pymongo
from pymongo.database import Database


def calc_wild_type(db: Database[Mapping[str, Any]], task):
    find_by = {
        "$or": [{"$in": ["$database", task['databases']]}, {"$in": ["$region_cypher", task['regions']]}]
    }
    wild_type = list(db["sequence"].aggregate([
        {
            "$match":
                {
                    "$expr": find_by
                }
        },
        {
            "$project": {
                "letters": {
                    "$map": {
                        "input": {
                            "$range": [
                                0,
                                {
                                    "$strLenCP": "$fasta"
                                }
                            ]
                        },
                        "as": "position",
                        "in": {
                            "position": "$$position",
                            "letter": {
                                "$substr": [
                                    "$fasta",
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
    db['tasks'].update_one({"name": task['name']}, {"$set": {"wild_fasta": wild_type[0]['letters']}})


def get_wild_type(db: Database[Mapping[str, Any]], task):
    return db["tasks"].find_one({'name': task['name']})['wild_fasta']


def population_to_base_poly(db: Database[Mapping[str, Any]], name: str, task):
    sequence = db['sequence']
    find_by = {
        "$or": [{"$in": ["$database", task['databases']]}, {"$in": ["$region_cypher", task['regions']]}]
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


def wild_type_to_base_poly(db: Database[Mapping[str, Any]], name: str, task):
    sequence = db['sequence']

    wild_type = get_wild_type(db, task)
    base_sequence = db['base_sequence'].find_one({"name": name})["fasta"]
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


def calculate_formulas(db: Database[Mapping[str, Any]], task, distr_name):
    mat_spod_max_min = list(db['distributions'].aggregate([
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
                "mat_spod": {
                    "$sum": {"$multiply": ["$distances._id", "$distances.percent"]}
                },
                "max": {
                    "$max": "$distances._id",
                },
                "min": {
                    "$min": "$distances._id",
                }
            }
        },
    ]))[0]
    stdDev_coeff = list(db['distributions'].aggregate([
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
                "stdDev": {
                    "$sum": {
                        "$multiply": [{"$pow": [{"$subtract": ["$distances._id", mat_spod_max_min["mat_spod"]]}, 2]},
                                      "$distances.percent"]}
                },
            }
        },
        {
            "$project": {
                "stdDevRes": {
                    "$sqrt": "$stdDev"
                },
                "variationCoef": {
                    "$divide": [{"$sqrt": "$stdDev"}, mat_spod_max_min["mat_spod"]]
                }
            }
        }
    ]))[0]
    mode = list(db['distributions'].aggregate([
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
            "$sort": {
                "distances.count": -1
            }
        },
        {
            "$limit": 1
        },
        {
            "$project": {
                "_id": 0,
                "mode": "$distances._id"
            }
        }
    ]))
    return {
        "mat_spod": mat_spod_max_min["mat_spod"],
        "max": mat_spod_max_min["max"],
        "min": mat_spod_max_min["min"],
        "std": stdDev_coeff['stdDevRes'],
        "variationCoef": stdDev_coeff['variationCoef'],
        "mode": mode[0]['mode']
    }


def get_haplogroups(db: Database[Mapping[str, Any]], task):
    find_by = {
        "$or": [{"$in": ["$database", task['databases']]}, {"$in": ["$region_cypher", task['regions']]}]
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


def distances_with_base(db: Database[Mapping[str, Any]], task, baseSequenceName: str = None):
    find_by = {
        "$or": [{"$in": ["$database", task['databases']]}, {"$in": ["$region_cypher", task['regions']]}]
    }
    compared_fasta = ''
    sequence = db['sequence']
    if baseSequenceName is None:
        compared_fasta = get_wild_type(db, task)
    else:
        compared_fasta = db['base_sequence'].find_one({"name": baseSequenceName})["fasta"]
    data = list(sequence.aggregate([
        {
            "$match": {
                "$expr": find_by
            }
        },
        {
            "$project": {
                "_id": 1,
                "fasta": 1
            }
        },
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
                                                    compared_fasta,
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
                },
            }
        },
        {
            "$group": {
                "_id": None,
                "sum": {"$sum": "$count"},
                "distances": {"$push": {"_id": "$_id", "count": "$count"}}
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
        },
        {
            "$sort": {
                "_id": 1
            }
        }
    ]))
    if baseSequenceName is None:
        baseSequenceName = "wild"
    db['distributions'].insert_one({
        'task_id': task['number'],
        'name': baseSequenceName,
        'distances': data,
    })
    return data


def each_with_each(db: Database[Mapping[str, Any]], task):
    find_by = {
        "$or": [{"$in": ["$database", task['databases']]}, {"$in": ["$region_cypher", task['regions']]}]
    }
    find_by_2 = {
        "$or": [{"$in": ["$$database", task['databases']]}, {"$in": ["$$region_cypher", task['regions']]}]
    }

    sequence = db['sequence']
    data = list(sequence.aggregate([
        {
            "$match": {
                "$expr": find_by
            }
        },
        {
            "$lookup": {
                "from": "sequence",
                "let": {
                    "seq_id": "$_id",
                    "fasta2": "$fasta",
                    "region_cypher": "$region_cypher",
                    "database": "$database"
                },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": find_by_2
                        }
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$gt": [
                                    "$$seq_id",
                                    "$_id",
                                ],
                            }
                        }
                    },
                    {
                        "$project": {
                            "fasta": 1,
                        }
                    },
                ],
                "as": "fasta2"
            }
        },
        {
            "$match": {
                "fasta2": {
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
                },
            }
        },
        {
            "$group": {
                "_id": None,
                "sum": {"$sum": "$count"},
                "distances": {"$push": {"_id": "$_id", "count": "$count"}}
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
