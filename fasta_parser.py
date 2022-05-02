fasta_file_location = "res/Ukraine.fasta"

words_black_list = ["Homo", "sapiens", "D-loop,", "partial", "sequence;", "isolate"]

existingDict = {
  "JX895517.1": {
      "id" : "11"
  }
}

def parse_fasta():
    file = open(fasta_file_location, "r")
    file_text = file.read().replace(r"mitochondrial", "").split(" ")
    filter_data = list(filter(lambda x : x not in words_black_list, file_text))
    parsed_fasta = " ".join(filter_data).split(">")
    for fasta in parsed_fasta[1:]:
        split_fasta = fasta.split(" ")
        if fasta[0] in existingDict:
            existingDict[split_fasta[0]].append({"cypher": split_fasta[1]})
        else:
            existingDict[split_fasta[0]] = {"cypher": split_fasta[1]}

        fasta_sequence = split_fasta[2:]
        existingDict[split_fasta[0]].update({"fasta": fasta_sequence[0].replace("\n", "")})

    print(existingDict["JX895517.1"])