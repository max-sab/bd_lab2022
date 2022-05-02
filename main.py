# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import pymongo
from parse import parse_files
from fasta_parser import parse_fasta

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


def combine_results(genes, fasta):
    for idx, gene in enumerate(genes):
        needed_fasta = fasta[gene["code"]]
        genes[idx] = genes[idx] | needed_fasta


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    genes = parse_files()
    fasta = parse_fasta()
    combine_results(genes, fasta)
    print(genes[23])
    client = pymongo.MongoClient(
        "mongodb+srv://evo:evolutional@evolutional.aweop.mongodb.net/genes?retryWrites=true&w=majority")
    db = client['genes']
    collection = db['sequence']
    #print(collection.find_one())
    #print_hi('PyCharm')










# See PyCharm help at https://www.jetbrains.com/help/pycharm/
