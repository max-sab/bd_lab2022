import pymongo

eva = {
    'fasta': 'TTCTTTCATGGGGAAGCAGATTTGGGTACCACCCAAGTATTGACTCACCCATCAACAACCGCTATGTATTTCGTACATTACTGCCAGCCACCATGAATATTGTACAGTACCATAAATACTTGACCACCTGTAGTACATAAAAACCCAATCCACATCAAAACCCTCCCCCCATGCTTACAAGCAAGTACAGCAATCAACCTTCAACTGTCACACATCAACTGCAACTCCAAAGCCACCCCTCACCCACTAGGATATCAACAAACCTACCCACCCTTAACAGTACATAGCACATAAAGCCATTTACCGTACATAGCACATTACAGTCAAATCCCTTCTCGTCCCCATGGATGACCCCCCTCAGATAGGGGTCCCTTGAC',
    'name': "EVA"}
andrews = {
    'fasta': 'TTCTTTCATGGGGAAGCAGATTTGGGTACCACCCAAGTATTGACTCACCCATCAACAACCGCTATGTATTTCGTACATTACTGCCAGCCACCATGAATATTGTACGGTACCATAAATACTTGACCACCTGTAGTACATAAAAACCCAATCCACATCAAAACCCCCTCCCCATGCTTACAAGCAAGTACAGCAATCAACCCTCAACTATCACACATCAACTGCAACTCCAAAGCCACCCCTCACCCACTAGGATACCAACAAACCTACCCACCCTTAACAGTACATAGTACATAAAGCCATTTACCGTACATAGCACATTACAGTCAAATCCCTTCTCGTCCCCATGGATGACCCCCCTCAGATAGGGGTCCCTTGAC',
    'name': "ANDREWS"}

def populate_db(elements):
    # client = pymongo.MongoClient(
    #     "mongodb+srv://evo:evolutional@evolutional.aweop.mongodb.net/genes?retryWrites=true&w=majority")
    client = pymongo.MongoClient('localhost', 27017)
    db = client['genes']
    # fasta_collection = db['fasta']
    general_collection = db['sequence']
    seqCollection = db['base_sequence']

    db['fasta'].delete_many({})
    db['sequence'].delete_many({})
    db['position_fasta'].delete_many({})

    if not seqCollection.find_one({'name': 'EVA'}):
        seqCollection.insert_one(eva)
    if not seqCollection.find_one({'name': 'ANDREWS'}):
        seqCollection.insert_one(andrews)

    for element in elements:
        # fasta = element.pop("fasta")
        # fasta_id = fasta_collection.insert_one({"fasta": fasta, "type": "general"}).inserted_id
        # element["fasta_id"] = fasta_id
        general_collection.insert_one(element)
