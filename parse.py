
def decide_country(country: str):
    if country.lower() == 'ukraine':
        return 'UKR'
    if country.lower() == 'russia':
        return 'RU'
    if country.lower() == 'belarus':
        return 'BEL'
    return ''

def parse_files():
    with open('res/Ukraine.gp', 'r', encoding='utf-8') as myfile:
        genes = []
        while True:
            line = myfile.readline()
            if not line:
                break
            if "LOCUS" in line:
                gene = {'length': (" ".join(line.split())).split()[2], 'number_of_carriers': 1, 'database': 'Ukraine' }
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
                        if 'rayon' in country_string:
                            location['rayon'] = country_string.split()[3]
                        gene['location'] = location
                        gene['mark'] = decide_country(country_string)
                    if 'COMMENT' in line2:
                        while True:
                            line3 = myfile.readline()
                            if line3.startswith('FEATURES'):
                                break
                            line2 += line3
                        gene['comment'] = " ".join(line2.replace('COMMENT','').replace('\n','').split())
                genes.append(gene)
        print(genes)
