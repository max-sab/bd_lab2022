import pymongo
from parse import parse_files
from fasta_parser import parse_fasta
from populate_db import populate_db

def combine_results(genes, fasta):
    final_genes = []
    for idx, gene in enumerate(genes):
        needed_fasta = fasta[gene["code"]]
        if len(needed_fasta["fasta"]) >= 312:
            genes[idx].update(needed_fasta)
            final_genes.append(genes[idx])
    return final_genes

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    genes = parse_files()
    fasta = parse_fasta()
    final_res = combine_results(genes, fasta)
    populate_db(final_res)
