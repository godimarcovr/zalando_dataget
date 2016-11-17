from urllib.parse import urlparse
import requests as req
from selenium import webdriver
#from recotest import *
#from selenium.webdriver.common.keys import Keys
from selenium.webdriver import ActionChains
#from selenium.webdriver import DesiredCapabilities
import threading
from timeoutcontext import timeout
from selenium.common.exceptions import TimeoutException
from retrying import retry

class ZalandoDownloader:

    def __init__(self):
        self.header = {"Accept-Language":"it-IT", "Accept-Encoding":"gzip"}
        self.baseurl = "https://api.zalando.com/"
        self.parameters = []
        self.section = ""
        self.driver = None
        #chromedriver = "/home/marco/pythoncode/zalando-downloader/chromedriver"
        #os.environ["webdriver.chrome.driver"] = chromedriver
        #opts = webdriver.ChromeOptions()
        #opts.add_argument("user-data-dir=/home/marco/.config/chromium/Default")
        #self.driver = webdriver.Chrome(chromedriver, chrome_options=opts)
        #*****
        #messo nel PATH di sistema il geckodriver
        #firefox_capabilities = DesiredCapabilities.FIREFOX
        #firefox_capabilities['marionette'] = True
        #self.driver = webdriver.Firefox(capabilities=firefox_capabilities)
        #****
        #self.driver = webdriver.PhantomJS()

    def get_json(self):
        comp = self.baseurl+self.section
        if len(self.parameters) > 0:
            comp += "?"
            first = True
            #magari usare urlparse invece di farlo a mano?
            for k, val in self.parameters:
                if first:
                    first = False
                else:
                    comp += "&"
                comp += k + "=" + val
        result = req.get(comp, headers=self.header)
        return result.json()

    def get_paired_ids(self, shopurl, timeout_secs=45):
        toret = []
        links = []
        #try:
        #    self.driver=get_browser()
        #except TimeoutException:
        #    print(0)
        #    return []
        print("Create Driver.... ("+shopurl+")")
        #self.driver = webdriver.PhantomJS()
        #print("...created, get page...("+shopurl+")")
        #self.driver.get(shopurl)
        self.driver = get_page(shopurl, 3, timeout_secs)
        if self.driver is None:
            print("... unable to get ("+shopurl+")")
            return toret
        print("... got it!("+shopurl+")")
        actcha = ActionChains(self.driver)
        attempts = 3
        while links == [] and attempts > 0:
            #html = self.driver.page_source
            #r=req.get(shopurl)
            #soup = BeautifulSoup(html)
            #print(html[:200])
            #sliders=soup.find_all("div",attrs={"data-reco-type":"CROSS_SELL"})
            #sliders = soup.find_all("a", attrs={"class":"productBox"})
            #for slide in sliders:
            #    #print(slide['href'])
            #    purl = urlparse(slide['href'])
            #    path = purl.path
            #    lista = path.split(sep="-")
            #    toret.append(lista[-2]+"-"+lista[-1].split(sep=".")[0])
            #sliders = soup.find_all("div", attrs={"class":"z-vegas-reco-scroller"})
            sliders = self.driver.find_elements_by_class_name("z-vegas-reco-scroller")
            if len(sliders) == 2:
                #sliders[1].send_keys(Keys.ARROW_DOWN)
                #sliders[1].renderContents()
                #links = sliders[1].find_all("a")
                links = sliders[1].find_elements_by_tag_name("a")
                if len(links) > 0:
                    print(len(links))
                    for link in links:
                        purl = urlparse(link.get_attribute('href'))
                        lista = purl.path.split(sep="-")
                        #dovrebbero essere sempre lunghi 13 gli id!
                        if len(lista[-2]+"-"+lista[-1].split(sep=".")[0]) < 13:
                            print("Errore in uno degli id trovati!")
                            print(link.get_attribute('href'))
                            print(lista[-2]+"-"+lista[-1].split(sep=".")[0])
                        else:
                            toret.append((lista[-2]+"-"+lista[-1].split(sep=".")[0]).upper())
                else:
                    attempts -= 1
                    actcha.move_to_element(sliders[1])
                    actcha.perform()
                    if attempts == 0:
                        print(0)
            else:
                print(0)
                break
        self.driver.quit()
        return toret

    def get_recos(self, shopurllist):
        recos_list = []
        for url in shopurllist:
            to_app = self.get_paired_ids(url)
            if len(to_app) > 0:
                recos_list.append(to_app)
        return recos_list

    def close_all(self):
        self.driver.quit()

def get_reco_url(shopurl):
    '''
    Non più necessario, vecchio metodo
    '''
    purl = urlparse(shopurl)
    purl = purl._replace(path="/reco"+purl.path)
    return purl.geturl()+"?t=cs"

def get_page(url,attempts,timeout):
    while attempts > 0:
        driver = webdriver.PhantomJS(service_args=['--load-images=no'])
        try:
            driver.set_page_load_timeout(timeout)
            driver.get(url)
            return driver
        except TimeoutException:
            attempts -= 1
            driver.quit()
            driver = None
    return driver


def analyse_cat(cats, n_per_cat):
    zd = ZalandoDownloader()
    zd.section = "articles"
    zd.parameters = []
    #zd.parameters.append(("ageGroup","adult"))
    #zd.parameters.append(("gender","female"))
    #cats = ["promo-pullover-cardigan-donna", "maglieria-felpe-donna", "premium-maglieria-felpe-donna", "promo-t-shirt-top-donna", "t-shirt-top-donna", "premium-t-shirt-top-donna"]
    # articoli per ciascuna categoria
    cat_ids = {}
    for cat in cats:
        zd.parameters = []
        zd.parameters.append(("category", cat))
        zd.parameters.append(("pageSize", str(n_per_cat)))
        zd.parameters.append(("sort", "popularity"))
        res = zd.get_json()
        cat_ids[cat] = []
        print("************************")
        print("Categoria: "+cat)
        for article in res['content']:
            print("Name: "+article['name'])
            print("id: "+article['id'])
            print("Shop: "+article['shopUrl'])
            print(article['categoryKeys'][0])
            #print(article['available'])
            #available = true?
            #if article['available']:
            cat_ids[cat].append((article['id'], article['shopUrl']))
    #articoli abbinati agli articoli di ciascuna categoria
    cat_recos = {}
    #quanti articoli di ciascuna categoria avevano abbinamenti disponibili
    cat_howmanyartwithrecos = {}
    print("************************")
    print("************************")
    print("Abbinamenti per categoria")
    for key in cat_ids:
        print(key)
        ids_shopurls = cat_ids[key]
        shop_urls = []
        cat_recos[key] = []
        for artid, shopurl in ids_shopurls:
            shop_urls.append(shopurl)
        recoslist = zd.get_recos(shop_urls)
        cat_howmanyartwithrecos[key] = len(recoslist)
        for recos in recoslist:
            cat_recos[key].extend(recos)
        #print(cat_recos[key])
        print("***")
    # per ciascuna categoria, quanti articoli di ogni categoria sono stati abbinati
    cat_stats = {}
    cache_catkeys = {}
    for cat in cat_recos:
        cat_stats[cat] = {}
        try:
            recos = cat_recos[cat]
            zd.section = "articles"
            zd.parameters = []
            #zd.parameters.append(("pageSize", str(len(recos))))
            for artid in recos:
                zd.parameters.append(("articleId", artid))
            res = zd.get_json()
            #vedo quante pagine ci sono
            n_pag = res['totalPages']
            for pag in range(n_pag):
                pag = pag + 1
                print(pag)
                zd.section = "articles"
                zd.parameters = []
                #zd.parameters.append(("pageSize", str(len(recos))))
                zd.parameters.append(("page", str(pag)))
                for artid in recos:
                    zd.parameters.append(("articleId", artid))
                res = zd.get_json()
                for article in res['content']:
                    for cat_key in article['categoryKeys']:
                        if cat_key not in cache_catkeys:
                            zd.section = "categories"
                            zd.parameters = [("key", cat_key)]
                            res = zd.get_json()
                            cache_catkeys[cat_key] = res['content'][0]['name']
                        if cache_catkeys[cat_key] not in cat_stats[cat]:
                            cat_stats[cat][cache_catkeys[cat_key]] = 0
                        cat_stats[cat][cache_catkeys[cat_key]] += 1
        except Exception as ex:
            print(ex)
        cat_stats[cat]['numValidi'] = cat_howmanyartwithrecos[cat]
    zd.close_all()
    return cat_stats

def aggregate_composite_stats(diz):
    res = {}
    for key1 in diz:
        for key2 in diz[key1]:
            if key2 not in res:
                res[key2] = 1
            else:
                res[key2] += diz[key1][key2]
    return res

class scrape_thread(threading.Thread):
    def __init__(self, cats):
        threading.Thread.__init__(self)
        self.cats = cats
        self.result = {}
    def run(self):
        self.result = analyse_cat(self.cats, 10)

if __name__ == "__main__":
    CATS = ["promo-pullover-cardigan-donna", "maglieria-felpe-donna"
            , "premium-maglieria-felpe-donna"]
    #MAGLIERIA_STATS = analyse_cat(CATS)
    SCRAPE1 = scrape_thread(CATS)
    SCRAPE1.start()
    CATS = ["promo-t-shirt-top-donna", "t-shirt-top-donna"
            , "premium-t-shirt-top-donna"]
    SCRAPE2 = scrape_thread(CATS)
    SCRAPE2.start()
    #TSHIRT_STATS = analyse_cat(CATS)
    SCRAPE1.join()
    SCRAPE2.join()
    MAGLIERIA_STATS = SCRAPE1.result
    TSHIRT_STATS = SCRAPE2.result
    MAGLIERIA_STATS = aggregate_composite_stats(MAGLIERIA_STATS)
    TSHIRT_STATS = aggregate_composite_stats(TSHIRT_STATS)
    print("maglieria********************")
    for key, value in MAGLIERIA_STATS.items():
        print(key+":"+str(value))
    print("tshirt***********************")
    for key, value in TSHIRT_STATS.items():
        print(key+":"+str(value))
