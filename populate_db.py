import pymongo

eva = {
    'fasta': 'TTCTTTCATGGGGAAGCAGATTTGGGTACCACCCAAGTATTGACTCACCCATCAACAACCGCTATGTATTTCGTACATTACTGCCAGCCACCATGAATATTGTACAGTACCATAAATACTTGACCACCTGTAGTACATAAAAACCCAATCCACATCAAAACCCTCCCCCCATGCTTACAAGCAAGTACAGCAATCAACCTTCAACTGTCACACATCAACTGCAACTCCAAAGCCACCCCTCACCCACTAGGATATCAACAAACCTACCCACCCTTAACAGTACATAGCACATAAAGCCATTTACCGTACATAGCACATTACAGTCAAATCCCTTCTCGTCCCCATGGATGACCCCCCTCAGATAGGGGTCCCTTGAC',
    'name': "EVA"}
andrews = {
    'fasta': 'TTCTTTCATGGGGAAGCAGATTTGGGTACCACCCAAGTATTGACTCACCCATCAACAACCGCTATGTATTTCGTACATTACTGCCAGCCACCATGAATATTGTACGGTACCATAAATACTTGACCACCTGTAGTACATAAAAACCCAATCCACATCAAAACCCCCTCCCCATGCTTACAAGCAAGTACAGCAATCAACCCTCAACTATCACACATCAACTGCAACTCCAAAGCCACCCCTCACCCACTAGGATACCAACAAACCTACCCACCCTTAACAGTACATAGTACATAAAGCCATTTACCGTACATAGCACATTACAGTCAAATCCCTTCTCGTCCCCATGGATGACCCCCCTCAGATAGGGGTCCCTTGAC',
    'name': "ANDREWS"}

def add_balto_slavic(elements):
    client = pymongo.MongoClient(
        "mongodb+srv://evo:evolutional@evolutional.aweop.mongodb.net/genes?retryWrites=true&w=majority")
    db = client['genes']
    location_collection = db['location']
    fasta_collection = db['fasta']
    general_collection = db['sequence']

    for element in elements:
        location = element.pop("location")
        location_id = location_collection.insert_one(location).inserted_id
        element["location_id"] = location_id
        fasta = element.pop("fasta")
        fasta_id = fasta_collection.insert_one({"fasta": fasta, "type": "general"}).inserted_id
        element["fasta_id"] = fasta_id
        general_collection.insert_one(element)

def populate_db(elements):
    client = pymongo.MongoClient(
        "mongodb+srv://evo:evolutional@evolutional.aweop.mongodb.net/genes?retryWrites=true&w=majority")
    db = client['genes']
    location_collection = db['location']
    fasta_collection = db['fasta']
    general_collection = db['sequence']
    seqCollection = db['base_sequence']

    db['location'].delete_many({})
    db['fasta'].delete_many({})
    db['sequence'].delete_many({})
    db['position_fasta'].delete_many({})

    if not seqCollection.find_one({'name':'EVA'}):
        seqCollection.insert_one(eva)
    if not seqCollection.find_one({'name': 'ANDREWS'}):
        seqCollection.insert_one(andrews)

    for element in elements:
       location = element.pop("location")
       location_id = location_collection.insert_one(location).inserted_id
       element["location_id"] = location_id
       fasta = element.pop("fasta")
       fasta_id = fasta_collection.insert_one({ "fasta" : fasta, "type" : "general" }).inserted_id
       element["fasta_id"] = fasta_id
       general_collection.insert_one(element)

    fasta_collection.aggregate([
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
                "seq_id": "$_id",
                "letter": "$letters.letter",
                "position": "$letters.position",
                "_id": 0
            }
        },
        {
            "$merge": {
                "into": "position_fasta",
                "on": "_id",
                "whenMatched": "replace",
                "whenNotMatched": "insert"
            }
        }
    ])

