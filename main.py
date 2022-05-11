# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import pymongo
from parse import parse_files
from fasta_parser import parse_fasta
from populate_db import populate_db

def combine_results(genes, fasta):
    for idx, gene in enumerate(genes):
        needed_fasta = fasta[gene["code"]]
        genes[idx].update(needed_fasta)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    genes = parse_files()
    fasta = parse_fasta()
    combine_results(genes, fasta)
    #print(genes[140])
    populate_db(genes)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
