fasta_file_location = "res/Balto_Slavic.fasta"

words_black_list = ["Homo", "sapiens", "D-loop,", "partial", "sequence;", "isolate"]

def parse_fasta():
    final_dict = {}
    file = open(fasta_file_location, "r")
    file_text = file.read().replace(r"mitochondrial", "").split(" ")
    filter_data = list(filter(lambda x : x not in words_black_list, file_text))
    parsed_fasta = " ".join(filter_data).split(">")
    for fasta in parsed_fasta[1:]:
        split_fasta = fasta.split(" ")
        fasta_sequence = split_fasta[2:]
        cypher = split_fasta[1]
        key = split_fasta[0]
        final_dict[key] = {
            "cypher": cypher,
            "fasta": fasta_sequence[0].replace("\n", ""),
            "region_cypher" : split_fasta[1][:2].replace("-", "")
        }

    return final_dict