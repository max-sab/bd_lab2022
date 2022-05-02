fasta_file_location = "res/Ukraine.fasta"

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
        final_dict[split_fasta[0]] = {
            "cypher": split_fasta[1],
            "fasta": fasta_sequence[0].replace("\n", "")
        }

    return final_dict