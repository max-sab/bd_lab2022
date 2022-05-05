# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import pymongo
from parse import parse_files
from fasta_parser import parse_fasta



def combine_results(genes, fasta):
    for idx, gene in enumerate(genes):
        needed_fasta = fasta[gene["code"]]
        genes[idx].update(needed_fasta)


eva = {
    'fasta': 'TTCTTTCATGGGGAAGCAGATTTGGGTACCACCCAAGTATTGACTCACCCATCAACAACCGCTATGTATTTCGTACATTACTGCCAGCCACCATGAATATTGTACAGTACCATAAATACTTGACCACCTGTAGTACATAAAAACCCAATCCACATCAAAACCCTCCCCCCATGCTTACAAGCAAGTACAGCAATCAACCTTCAACTGTCACACATCAACTGCAACTCCAAAGCCACCCCTCACCCACTAGGATATCAACAAACCTACCCACCCTTAACAGTACATAGCACATAAAGCCATTTACCGTACATAGCACATTACAGTCAAATCCCTTCTCGTCCCCATGGATGACCCCCCTCAGATAGGGGTCCCTTGAC',
    'name': "EVA"}
andrews = {
    'fasta': 'TTCTTTCATGGGGAAGCAGATTTGGGTACCACCCAAGTATTGACTCACCCATCAACAACCGCTATGTATTTCGTACATTACTGCCAGCCACCATGAATATTGTACGGTACCATAAATACTTGACCACCTGTAGTACATAAAAACCCAATCCACATCAAAACCCCCTCCCCATGCTTACAAGCAAGTACAGCAATCAACCCTCAACTATCACACATCAACTGCAACTCCAAAGCCACCCCTCACCCACTAGGATACCAACAAACCTACCCACCCTTAACAGTACATAGTACATAAAGCCATTTACCGTACATAGCACATTACAGTCAAATCCCTTCTCGTCCCCATGGATGACCCCCCTCAGATAGGGGTCCCTTGAC',
    'name': "ANDREWS"}

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    genes = parse_files()
    fasta = parse_fasta()
    combine_results(genes, fasta)
    client = pymongo.MongoClient(
        "mongodb+srv://evo:evolutional@evolutional.aweop.mongodb.net/genes?retryWrites=true&w=majority")
    db = client['genes']
    seqCollection = db['sequence']
    if not seqCollection.find_one({'name':'EVA'}):
        seqCollection.insert_one(eva)
    if not seqCollection.find_one({'name': 'ANDREWS'}):
        seqCollection.insert_one(andrews)
    db['position_fasta'].delete_many({})
    seqCollection.aggregate([
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

    seqCollection.aggregate([{

    }])

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
