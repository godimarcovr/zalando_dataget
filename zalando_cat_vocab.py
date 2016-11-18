from zalando_downloader import *
from zalando_dataset import *
import operator

CAT_VOCAB = {}

ZALDOWN = ZalandoDownloader()
ZALDOWN.section = "categories"

def add_cat(catkey):
    ZALDOWN.parameters = []
    ZALDOWN.parameters.append(("key", catkey))
    res = ZALDOWN.get_json()
    if "content" in res and len(res['content']) == 1:
        cat = res['content'][0]
        assert cat['key'] == catkey
        CAT_VOCAB[catkey] = {}
        CAT_VOCAB[catkey]['name'] = cat['name']
        CAT_VOCAB[catkey]['key'] = cat['key']
        CAT_VOCAB[catkey]['parentKey'] = cat['parentKey'] if 'parentKey' in cat else ""
        CAT_VOCAB[catkey]['childKeys'] = cat['childKeys'] if 'childKeys' in cat else []

def get_nomi(catkeys):
    catnames = []
    for catkey in catkeys:
        if catkey not in CAT_VOCAB:
            add_cat(catkey)
        catnames.append(CAT_VOCAB[catkey]['name'])
    return catnames

def get_nome(catkey):
    return get_nomi([catkey])[0]


if __name__ == "__main__":
    ZALDATA = ZalandoDataset(datasetpath="datasets/felpe_tshirt", mode="r")
    maglieria_felpe_pairings = []
    #maglieria_felpe_count = 0
    tshirt_top_pairings = []
    #tshirt_top_count = 0
    for art_key, attributes in ZALDATA.dataset.items():
        if "Maglieria & Felpe" in get_nomi(attributes['categoryKeys']):
            maglieria_felpe_pairings.extend(attributes['pairings'])
            #maglieria_felpe_count += len(attributes['pairings'])
        elif "T-shirt & Top" in get_nomi(attributes['categoryKeys']):
            tshirt_top_pairings.extend(attributes['pairings'])
            #tshirt_top_count += len(attributes['pairings'])
    maglieria_felpe_pairing_stats = {}
    for maglieria_felpe_pairing in maglieria_felpe_pairings:
        for cat_key in ZALDATA.dataset[maglieria_felpe_pairing]['categoryKeys']:
            if get_nome(cat_key) not in maglieria_felpe_pairing_stats:
                maglieria_felpe_pairing_stats[get_nome(cat_key)] = 0
            maglieria_felpe_pairing_stats[get_nome(cat_key)] += 1
    mfps_sorted = sorted(maglieria_felpe_pairing_stats.items(), key=operator.itemgetter(1), reverse=True)
    tshirt_top_pairing_stats = {}
    for tshirt_top_pairing in tshirt_top_pairings:
        for cat_key in ZALDATA.dataset[tshirt_top_pairing]['categoryKeys']:
            if get_nome(cat_key) not in tshirt_top_pairing_stats:
                tshirt_top_pairing_stats[get_nome(cat_key)] = 0
            tshirt_top_pairing_stats[get_nome(cat_key)] += 1
    ttps_sorted = sorted(tshirt_top_pairing_stats.items(), key=operator.itemgetter(1), reverse=True)

    fout = open("report.txt", "w")
    fout.write("Maglieria & Felpe ("+str(len(maglieria_felpe_pairings))+"): \n")
    for cat_name, cat_count in mfps_sorted:
        fout.write(cat_name+":"+str(cat_count)+"("
                   +str(cat_count/len(maglieria_felpe_pairings))+")\n")
    fout.write("\n**********************************\n")
    fout.write("T-shirt & Top ("+str(len(tshirt_top_pairings))+"): \n")
    for cat_name, cat_count in ttps_sorted:
        fout.write(cat_name+":"+str(cat_count)+"("
                   +str(cat_count/len(tshirt_top_pairings))+")\n")
    fout.close()

