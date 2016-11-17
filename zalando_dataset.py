from zalando_downloader import *

class ZalandoDataset:

    def __init__(self, csvfile="default.csv"
                 , columns=["id", "name", "shopUrl", "categoryKeys", "largeHdUrl", "pairings"]
                 , mode="w"):
        self.dataset = {}
        assert(columns[0] == "id")
        self.colnames = columns
        self.csvfile = csvfile
        if mode == "r" or mode == "a":
            self.load_input(csvfile)
        elif mode == "w":
            fout = open(csvfile, "w")
            fout.close()

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

    def add_articles_to_dataset(self, parameters, page_limit=10):
        zaldown = ZalandoDownloader()
        zaldown.parameters = parameters
        zaldown.section = "articles"
        res = zaldown.get_json()
        if page_limit == -1:
            num_pages = res['totalPages']
        else:
            num_pages = page_limit if res['totalPages'] > page_limit else res['totalPages']
        for i in range(num_pages):
            if i > 0:
                zaldown.parameters = parameters
                zaldown.parameters.append(("page", str(i+1)))
                res = zaldown.get_json()
            if "content" in res:
                articles = res["content"]
                for article in articles:
                    if article["id"] not in self.dataset:
                        self.dataset[article["id"]] = {}
                        for col in self.colnames:
                            if col == "largeHdUrl":
                                images = article['media']["images"]
                                for image in images:
                                    if image["orderNumber"] == 1 and image['type'] == "NON_MODEL":
                                        self.dataset[article["id"]][col] = image[col]
                                        break
                            elif col == "pairings":
                                self.dataset[article["id"]][col] = []
                                #if get_pairings:
                                #    recoss = zaldown.get_recos([article["shopUrl"]])
                                #    self.dataset[article["id"]][col] = []
                                #    for recos in recoss:
                                #        self.dataset[article["id"]][col].extend(recos)
                            elif col != "id":
                                self.dataset[article["id"]][col] = article[col]
    def save_to_csv(self):
        fout = open(self.csvfile, "w")
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
                    to_write += attributes[col]
            fout.write(to_write + "\n")

        fout.close()

    def fill_pairings(self):
        assert "pairings" in self.colnames
        art_ids = []
        for art_id, attributes in self.dataset.items():
            pairings = attributes["pairings"]
            art_ids.extend(pairings)
        art_ids = list(set(art_ids))
        parameters = []
        for art_id in art_ids:
            parameters.append(("articleId", art_id))
        self.add_articles_to_dataset(parameters, page_limit=-1)

    def get_missing_pairings(self):
        #add howMany per salvare a pezzi
        #threads
        assert "pairings" in self.colnames and "shopUrl" in self.colnames
        for art_id, attributes in self.dataset.items():
            pairings = attributes["pairings"]
            if len(pairings) == 0:
                shopurl = attributes["shopUrl"]
                self.dataset[art_id]["pairings"] = []
                zaldown = ZalandoDownloader()
                recoss = zaldown.get_recos([shopurl])
                for recos in recoss:
                    self.dataset[art_id]["pairings"].extend(recos)


if __name__ == "__main__":
    ZALDATA = ZalandoDataset()
    PARAMETERS = []
    #CATS = ["promo-pullover-cardigan-donna", "maglieria-felpe-donna"
    #        , "premium-maglieria-felpe-donna"
    #        , "promo-t-shirt-top-donna", "t-shirt-top-donna", "premium-t-shirt-top-donna"]
    CATS = ["promo-pullover-cardigan-donna"]
    for cat in CATS:
        PARAMETERS.append(("category", cat))
    PARAMETERS.append(("sort", "popularity"))
    PARAMETERS.append(("pageSize", "3"))
    ZALDATA.add_articles_to_dataset(PARAMETERS, page_limit=1)
    ZALDATA.get_missing_pairings()
    #ZALDATA.fill_pairings()
    #ZALDATA.get_missing_pairings()
    ZALDATA.save_to_csv()
    ZALDATA = ZalandoDataset(mode="r")
    