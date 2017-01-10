from queue import Queue, Empty
from time import sleep
import random
import os
import urllib.request
from zalando_downloader import *
import httplib2
import zalando_cat_vocab as zcv
from shutil import copyfile


class ZalandoDataset:

    def __init__(self, datasetpath="default"
                 , columns=["id", "name", "shopUrl", "categoryKeys", "largeHdUrl", "pairings"]
                 , mode="w"):
        self.dataset = {}
        assert columns[0] == "id"
        self.colnames = columns
        self.datasetpath = datasetpath
        self.datasetname = os.path.basename(os.path.normpath(self.datasetpath))
        if not os.path.exists(self.datasetpath):
            os.makedirs(datasetpath)
        if mode == "r" or mode == "a":
            self.load_input(self.datasetpath+"/"+self.datasetname+".csv")
        elif mode == "w":
            fout = open(self.datasetpath+"/"+self.datasetname+".csv", "w")
            fout.close()
        # zcv.load_cache()
        # zcv.load_main_cat_names()

    def load_input(self, inf):
        fin = open(inf)
        for row in fin:
            elements = row.split(sep=";")
            art_id = elements[0]
            if len(elements) != len(self.colnames) and len(elements) > 0:
                print("Riga non valida o vuota, numero sbagliato di campi!")
                print(row)
            self.dataset[art_id] = {}
            for i in range(1, len(self.colnames)):
                field = elements[i].rstrip("\n")
                if self.colnames[i] == "categoryKeys" or self.colnames[i] == "pairings":
                    if field[1:-1] != '':
                        self.dataset[art_id][self.colnames[i]] = field[1:-1].split(sep=",")
                    else:
                        self.dataset[art_id][self.colnames[i]] = []
                else:
                    self.dataset[art_id][self.colnames[i]] = field
        fin.close()

    def add_articles_to_dataset(self, parameters, page_limit=10
                                , getpacks=False, filter_cat_name=[], filter_nonpicture=False, language="it-IT"):
        assert len(parameters) > 0
        get_dangling = getpacks or (len(filter_cat_name) > 0) or filter_nonpicture
        zaldown = ZalandoDownloader(language=language)
        zaldown.parameters = parameters
        zaldown.section = "articles"
        assert len(parameters) < 150
        res = zaldown.get_json()
        if "errors" in res:
            print("Errore nel JSON")
            print(zaldown.parameters)
            print(res)
            return
        if page_limit == -1:
            num_pages = res['totalPages']
        else:
            num_pages = page_limit if res['totalPages'] > page_limit else res['totalPages']
        packs = []
        for i in range(num_pages):
            if i > 0:
                zaldown.parameters = parameters[:]
                zaldown.parameters.append(("page", str(i+1)))
                res = zaldown.get_json()
            if "content" in res:
                articles = res["content"]
                for article in articles:
                    if article["id"] not in self.dataset:
                        #se c'é PACK nel nome, vuol dire che sono più vestiti insieme e
                        #quindi la foto non va più bene
                        if "pack" in article["name"].lower():
                            print(article["id"]+" è un pack, rimuovi dal dataset!")
                            if getpacks:
                                packs.append(article["id"])
                            continue
                        if filter_cat_name != [] and type(filter_cat_name) is set:
                            # se non c'è un'intersezione, ossia è rifiutato dal filtro
                            if not set(zcv.get_nomi(article["categoryKeys"], language=language)) & filter_cat_name:
                                packs.append(article["id"])
                                continue
                        self.dataset[article["id"]] = {}
                        for col in self.colnames:
                            if col == "largeHdUrl":
                                images = article['media']["images"]
                                self.dataset[article["id"]][col] = ""
                                for image in images:
                                    #if image["orderNumber"] == 1 and image['type'] == "NON_MODEL":
                                    #print(image['type'])
                                    if image['type'] == "NON_MODEL" or image['type'] == "UNSPECIFIED":
                                        self.dataset[article["id"]][col] = image[col]
                                        break
                                if self.dataset[article["id"]][col] == "":
                                    if filter_nonpicture:
                                        packs.append(article["id"])

                            elif col == "pairings":
                                self.dataset[article["id"]][col] = []
                            elif col == "catname":
                                self.dataset[article["id"]][col] = ""
                                for cat_key in article["categoryKeys"]:
                                    zcv.add_cat(cat_key)
                                    if zcv.get_nome(cat_key) in zcv.MAIN_CAT_NAMES:
                                        self.dataset[article["id"]][col] = zcv.get_nome(cat_key)
                                        break
                                if self.dataset[article["id"]][col] == "":
                                    pass
                                #if self.dataset[article["id"]][col] == "":
                                #    print("Attenzione, questo articolo ("+article["id"]+") non ha
                                # main_cat")
                                #    print(article["categoryKeys"])
                            elif col != "id":
                                self.dataset[article["id"]][col] = article[col]
        if get_dangling:
            #nel caso voglia rimuovere vestiti che hanno problemi con le immagini, li tolgo da qua
            for art_id in packs:
                if art_id in self.dataset:
                    del self.dataset[art_id]
            return packs



    def save_to_csv(self):
        fout = open(self.datasetpath+"/"+self.datasetname+".csv", "w")
        for art_id, attributes in self.dataset.items():
            to_write = art_id
            for i in range(1, len(self.colnames)):
                if i < len(self.colnames):
                    to_write += ";"
                col = self.colnames[i]
                if col == "pairings" or col == "categoryKeys":
                    to_write += "["
                    for j in range(len(attributes[col])):
                        elem = attributes[col][j]
                        to_write += elem
                        if j < len(attributes[col])-1:
                            to_write += ","
                    to_write += "]"
                else:
                    if col in attributes:
                        to_write += attributes[col]
                    else:
                        pass
            fout.write(to_write + "\n")

        fout.close()

    def fill_pairings(self, getpacks=True, get_notmain=True):
        assert "pairings" in self.colnames
        art_ids = []
        get_dangling = getpacks or get_notmain
        use_main_cat_names = "catname" in self.colnames
        for art_id, attributes in self.dataset.items():
            pairings = attributes["pairings"]
            for pairing in pairings:
                if pairing not in self.dataset:
                    art_ids.append(pairing)
            #art_ids.union(set(art_ids))
        art_ids = list(set(art_ids))
        parameters = []
        count = 0
        pack_ids = set([])
        for art_id in art_ids:
            parameters.append(("articleId", art_id))
            count += 1
            if count >= 50:
                retry = True
                while retry:
                    try:
                        pack_ids.update(self.add_articles_to_dataset(parameters, page_limit=-1
                                        , getpacks=getpacks
                                        , filter_cat_name=zcv.MAIN_CAT_NAMES if use_main_cat_names else []))
                        retry = False
                    except ConnectionResetError:
                        print("Il server mi ha chiuso fuori, riprovo tra due secondi...")
                        sleep(2)
                count = 0
                parameters = []
        if len(parameters) > 0:
            pack_ids.update(self.add_articles_to_dataset(parameters, page_limit=-1
                            , getpacks=getpacks
                            , filter_cat_name=zcv.MAIN_CAT_NAMES if use_main_cat_names else []))
        if get_dangling:
            for art_id, attributes in self.dataset.items():
                attributes["pairings"] = [x for x in attributes["pairings"] if x not in pack_ids]

    def count_dangling(self):
        assert "pairings" in self.colnames
        count = 0
        for art_id, attributes in self.dataset.items():
            pairings = attributes["pairings"]
            #art_ids.extend(pairings)
            for pairing in pairings:
                if pairing not in self.dataset:
                    count += 1
        return count


    def get_missing_pairings(self, lim=float('inf'), num_threads=1, remove_not_paired=False):
        #threads
        assert "pairings" in self.colnames and "shopUrl" in self.colnames
        count = 0
        urlqueue = []
        for art_id, attributes in self.dataset.items():
            pairings = attributes["pairings"]
            if len(pairings) == 0:
                shopurl = attributes["shopUrl"]
                self.dataset[art_id]["pairings"] = []
                urlqueue.append((art_id, shopurl))
                #zaldown = ZalandoDownloader()
                #recoss = zaldown.get_recos([shopurl])
                #for recos in recoss:
                #    self.dataset[art_id]["pairings"].extend(recos)
                count += 1

        if lim < float('inf'):
            urlsample = [urlqueue[i] for i in random.sample(range(len(urlqueue)), lim)]
        else:
            urlsample = urlqueue[:]

        urlqueue = Queue()
        for url in urlsample:
            urlqueue.put(url)
            count = len(urlsample)

        threads = []
        for i in range(num_threads):
            thr = ScrapeThread(urlqueue)
            thr.start()
            threads.append(thr)

        pairings = []
        for i in threads:
            i.join()
            pairings.extend(i.result)
        
        arts_to_remove = []

        for art_id, pairs in pairings:
            #zaldown = ZalandoDownloader()
            #recoss = zaldown.get_recos([shopurl])
            #for recos in recoss:
            #    self.dataset[art_id]["pairings"].extend(recos)
            self.dataset[art_id]["pairings"] = pairs
            if remove_not_paired:
                if len(pairs) == 0:
                    arts_to_remove.append(art_id)
        if remove_not_paired:
            for art_id_to_rem in arts_to_remove:
                del self.dataset[art_id_to_rem]
                #devo rimuovere anche tutti i riferimenti ad esso!
                for art_id, att in self.dataset.items():
                    att["pairings"] = [x for x in att["pairings"] if not x == art_id_to_rem]
        return count

    def download_images(self, infolder_catname = False):
        for art_id, attributes in self.dataset.items():
            keep = True
            while keep:
                try:
                    if not os.path.exists(self.datasetpath+"/"+art_id+".jpg"):
                        print("Downloading "+art_id+".jpg....")
                        urllib.request.urlretrieve(attributes['largeHdUrl']
                                                   , filename=self.datasetpath+"/"+art_id+".jpg")
                        if infolder_catname:
                            if not os.path.exists(self.datasetpath+"/"+attributes["catname"]):
                                os.mkdir(self.datasetpath+"/"+attributes["catname"])
                            copyfile(self.datasetpath+"/"+art_id+".jpg" \
                                    , self.datasetpath+"/"+attributes["catname"]+"/"+art_id+".jpg")
                        print("Downloaded "+art_id+".jpg....")
                    keep = False
                except (ConnectionResetError, httplib2.http.client.IncompleteRead):
                    print("Il server mi ha chiuso fuori, riprovo tra due secondi...")
                    try:
                        os.remove(self.datasetpath+"/"+art_id+".jpg")
                    except FileNotFoundError:
                        pass
                    sleep(2)
                except ValueError:
                    pass
        print("Finito il download.")

    def split_into_new_dataset(self, new_dataset_path="default2", new_dataset_part=0.5):
        newds = ZalandoDataset(datasetpath=new_dataset_path, columns=self.colnames)
        new_keys = random.sample(list(self.dataset.keys()), int(len(self.dataset)*new_dataset_part))
        for new_key in new_keys:
            newds.dataset[new_key] = self.dataset[new_key]
            del self.dataset[new_key]
        newds.save_to_csv()


class ScrapeThread(threading.Thread):
    def __init__(self, urlqueue):
        threading.Thread.__init__(self)
        self.urlqueue = urlqueue
        self.result = []

    def run(self):
        while not self.urlqueue.empty():
            try:
                art_id, url = self.urlqueue.get(block=False)
                print("Rimasti in coda: "+str(self.urlqueue.qsize()))
            except Empty:
                break
            self.urlqueue.task_done()
            zaldown = ZalandoDownloader()
            recoss = zaldown.get_recos([url])
            assert len(recoss) <= 1
            for recos in recoss:
                self.result.append((art_id, recos))
            if len(recoss) == 0:
                self.result.append((art_id, []))
            #metterci print di fine raccolta con numero thread

if __name__ == "__main__":
    zcv.load_cache()
    zcv.load_main_cat_names("maincatnames_morespecific.txt")
    ZALDATA = ZalandoDataset(datasetpath="datasets/balanced_specific_train"
                             , columns=["id", "name", "shopUrl", "categoryKeys", "largeHdUrl"
                                        , "pairings", "catname"])
    PARAMETERS = []
    #PARAMETERS.append(("sort", "popularity"))
    PARAMETERS.append(("pageSize", "20"))
    # CATS = ["promo-pullover-cardigan-donna", "maglieria-felpe-donna"
    #         , "premium-maglieria-felpe-donna"
    #         , "promo-t-shirt-top-donna", "t-shirt-top-donna", "premium-t-shirt-top-donna"]
    # CATS = ["jeans-donna", "camicie-donna", "pantaloni-donna", "maglieria-felpe-donna"
    #         , "giacche-donna", "borse-donna", "cappelli-donna", "cappotti-donna"
    #         , "cinture-donna", "foulard-sciarpe-donna", "gonne", "intimo-donna"
    #         , "t-shirt-top-donna", "vestiti-donna"]
    CATS = zcv.load_catkeys_from_namefile("maincatnames_morespecific.txt" \
        , additionalparams=[("targetGroup", "WOMEN")])
    for cat in CATS:
        PARAMETERS2 = []
        PARAMETERS2.extend(PARAMETERS)
        PARAMETERS2.append(("category", cat))
        #so per certo che hanno una categoria main
        ZALDATA.add_articles_to_dataset(PARAMETERS2, page_limit=5, filter_nonpicture=True)
    ZALDATA.save_to_csv()
    #ZALDATA.get_missing_pairings(num_threads=4)
    #ZALDATA.get_missing_pairings(num_threads=4, remove_not_paired=True)
    ZALDATA.save_to_csv()
    zcv.save_cache()
    ZALDATA.split_into_new_dataset(new_dataset_path="datasets/balanced_specific_test")
    ZALDATA.save_to_csv()

    print(ZALDATA.count_dangling())
    #ZALDATA.fill_pairings()
    print(ZALDATA.count_dangling())
    ZALDATA.save_to_csv()
    ZALDATA.download_images(infolder_catname = True)

    ZALDATA = ZalandoDataset(datasetpath="datasets/balanced_specific_test"
                             , columns=["id", "name", "shopUrl", "categoryKeys", "largeHdUrl"
                                        , "pairings", "catname"], mode="r")
    #ZALDATA.get_missing_pairings(num_threads=4, remove_not_paired=False)
    #ZALDATA.get_missing_pairings(num_threads=4, remove_not_paired=True)
    ZALDATA.save_to_csv()
    print(ZALDATA.count_dangling())
    #ZALDATA.fill_pairings()
    print(ZALDATA.count_dangling())
    ZALDATA.save_to_csv()
    zcv.save_cache()
    ZALDATA.download_images(infolder_catname = True)
