def decide_country(country: str):
    if country.lower() == 'ukraine':
        return 'UKR'
    if country.lower() == 'russia':
        return 'RU'
    if country.lower() == 'belarus':
        return 'BEL'
    if country.lower() == 'czech republic':
        return 'CZ'
    return ''

version_black_list = ["KT262553.1", "KT262558.1"]

def read_file(file_name, db_name):
    with open(file_name, 'r', encoding='utf-8') as myfile:
        genes = []
        while True:
            line = myfile.readline()
            if not line:
                break
            if "LOCUS" in line:
                gene = {'length': (" ".join(line.split())).split()[2], 'number_of_carriers': 1, 'database': db_name}
                while True:
                    line2 = myfile.readline()
                    if line2.startswith('//'):
                        break
                    if line2.startswith('VERSION'):
                        version = (" ".join(line2.split())).split()[1]
                        gene['code'] = version
                    if 'haplogroup' in line2:
                        gene['haplogroup'] = line2.strip().split()[0].split("\"")[1]
                    if 'country' in line2:
                        if line2.count('"') == 1:
                            while True:
                                line3 = myfile.readline()
                                line2 += line3
                                if '"' in line3:
                                    break
                        country_string = " ".join(line2.strip().split('"')[1].split())
                        location = {'country': country_string.split(':')[0]}
                        if 'oblast' in country_string:
                            location['oblast'] = country_string.split()[1]
                            # gene['region_cypher'] = decide_region_short(location['oblast'])
                        if 'rayon' in country_string:
                            location['rayon'] = country_string.split()[3]
                        gene['location'] = location
                        gene['mark'] = decide_country(country_string.split(':')[0])
                    if 'COMMENT' in line2:
                        while True:
                            line3 = myfile.readline()
                            if line3.startswith('FEATURES'):
                                break
                            line2 += line3
                        gene['comment'] = " ".join(line2.replace('COMMENT', '').replace('\n', '').split())
                if gene["code"] not in version_black_list:
                    genes.append(gene)
        return genes

def parse_files():
    ukraine_files = read_file("res/Ukraine.gp", db_name="Ukraine")
    balto_slavic_files = read_file("res/Balto_Slavic.gp", db_name="Balto-Slavic")
    return ukraine_files + balto_slavic_files