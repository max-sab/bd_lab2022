ukr_fasta_file_location = "res/Ukraine.fasta"
balto_slavic_fasta_file_location = "res/Balto_Slavic.fasta"

words_black_list = ["Homo", "sapiens", "D-loop,", "partial", "sequence;", "isolate"]

def parse_fasta():
    file = open(ukr_fasta_file_location, "r")
    ukraine_fasta = read_fasta(file)
    file = open(balto_slavic_fasta_file_location, "r")
    balto_slavic_fasta = read_fasta(file)
    return {**ukraine_fasta, **balto_slavic_fasta}

def read_fasta(file):
    final_dict = {}
    file_text = file.read().replace(r"mitochondrial", "").split(" ")
    filter_data = list(filter(lambda x: x not in words_black_list, file_text))
    parsed_fasta = " ".join(filter_data).split(">")
    for fasta in parsed_fasta[1:]:
        split_fasta = fasta.split(" ")
        fasta_sequence = split_fasta[2:]
        cypher = split_fasta[1]
        key = split_fasta[0]
        region_cypher = split_fasta[1][:4].replace("-", "")
        final_dict[key] = {
            "cypher": cypher,
            "fasta": fasta_sequence[0].replace("\n", ""),
            "region_cypher": ''.join([i for i in region_cypher if not i.isdigit()])
        }

    return final_dict