from zalando_downloader import *

CAT_VOCAB = {}

ZALDOWN = ZalandoDownloader()
ZALDOWN.section = "categories"

def get_nome(catkey):
    if catkey not in CAT_VOCAB:
        ZALDOWN.parameters = []
        ZALDOWN.parameters.append(("key", catkey))
        res = ZALDOWN.get_json()
        if "content" in res:
            for cats in res["content"]:
                #####



