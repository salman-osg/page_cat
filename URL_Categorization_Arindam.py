import re
import requests
import pandas as pd
from bs4 import BeautifulSoup



def check_pages(row, pdp_key, search_key, cart_key):
    try:
        # if not pd.isna(row['URL_Category_new']):
        # # if row['URL_Categry_new'] != 'Not Found':
        #       return row['URL_Category_new']
        dom = row["domain"].lower()
        #print(row)
        #dom = row["Search Domain"].lower()
        #print(row["URL"])
        match_domain = re.search(f"https:\/\/(www.)?{dom}(\/)?$", row["URL"].lower())
        if match_domain is not None:
            return "Home Page"
        if all(x in row["URL"] for x in ['amazon','/dp']) or \
             all(x in row["URL"] for x in ['amazon','/gp/aw/d/']) or \
             all(x in row["URL"] for x in ['target','/p/']) or \
             all(x in row["URL"] for x in ['walmart','/ip']) or \
             all(x in row["URL"] for x in ['walmart','track']) or \
             all(x in row["URL"] for x in ['costco','.product']) or \
             all(x in row["URL"] for x in ['cvs','-prodid']) or \
             all(x in row["URL"] for x in ['aldi','/p/']) or \
             all(x in row["URL"] for x in ['dollargeneral','/product-page']) or \
             all(x in row["URL"] for x in ['ebay','/itm/']) or \
             all(x in row["URL"] for x in ['kroger','/p/']) or \
             all(x in row["URL"] for x in ['samsclub','/p/']) or \
             all(x in row["URL"] for x in ['walgreens','product']) or \
             all(x in row["URL"] for x in ['dm','-p']) or \
             all(x in row["URL"] for x in ['mediamarkt','product']) or \
             all(x in row["URL"] for x in ['otto','/p/']) or \
             all(x in row["URL"] for x in ['rossmann','/p/']) or \
             all(x in row["URL"] for x in ['saturn','product']) or \
             all(x in row["URL"] for x in ['biccamera','/item/']) or \
             all(x in row["URL"] for x in ['cosme','/product/']) or \
             all(x in row["URL"] for x in ['edion','/detail.html?']) or \
             all(x in row["URL"] for x in ['bestbuy','skuId']) or \
             all(x in row["URL"] for x in ['noon','/p/']) or \
             all(x in row["URL"] for x in ['noon','/p?o']) or \
             all(x in row["URL"] for x in ['carrefouruae','/p/']) or \
             any(x in row["URL"] for x in ['/p/']):
            return 'PDP'
        elif all(x in row["URL"] for x in ['amazon','/stores/']) or \
             all(x in row["URL"] for x in ['target','/b/']) or \
             all(x in row["URL"] for x in ['carrefouruae','&filter=brand_name']):
            return "Brand Page"
        elif all(x in row["URL"] for x in ['amazon','i=']) and \
             all(x not in row["URL"] for x in ['k='] ):
            return "Category Page"
        elif all(x in row["URL"] for x in ['target','/c/']) or \
             all(x in row["URL"] for x in ['walmart','browse']) or \
             all(x in row["URL"] for x in ['walmart','/cp']) or \
             all(x in row["URL"] for x in ['costco','/baby-kids']) or \
             all(x in row["URL"] for x in ['costco','/diapers-wipes']) or \
             all(x in row["URL"] for x in ['cvs','/baby-child']) or \
             all(x in row["URL"] for x in ['dollargeneral','/category-page']) or \
             all(x in row["URL"] for x in ['samsclub','/c/']) or \
             all(x in row["URL"] for x in ['samsclub','/b/']) or \
             all(x in row["URL"] for x in ['mediamarkt','/category/']) or \
             all(x in row["URL"] for x in ['rossmann','/c/']) or \
             all(x in row["URL"] for x in ['saturn','/category/']) or \
             all(x in row["URL"] for x in ['biccamera','/category/']) or \
             all(x in row["URL"] for x in ['edion','/item_list.html?c']) or \
             all(x in row["URL"] for x in ['rakuten','/category/']) or \
             all(x in row["URL"] for x in ['bestbuy','abcat']):
            return 'Category Page'
        elif all(x in row["URL"] for x in ['amazon','k=']) or \
             all(x in row["URL"] for x in ['target','searchTerm']) or \
             all(x in row["URL"] for x in ['walmart','search']) or \
             all(x in row["URL"] for x in ['walmart','query']) or \
             all(x in row["URL"] for x in ['bestbuy','searchpage']) or \
             all(x in row["URL"] for x in ['costco','CatalogSearch']) or \
             all(x in row["URL"] for x in ['cvs','/search']) or \
             all(x in row["URL"] for x in ['dollargeneral','/search-results']) or \
             all(x in row["URL"] for x in ['ebay','/sch/']) or \
             all(x in row["URL"] for x in ['kroger','/search']) or \
             all(x in row["URL"] for x in ['samsclub','/s/']) or \
             all(x in row["URL"] for x in ['walgreens','/search']) or \
             all(x in row["URL"] for x in ['dm','/search?query']) or \
             all(x in row["URL"] for x in ['mediamarkt','/search.html?']) or \
             all(x in row["URL"] for x in ['otto','/suche/']) or \
             all(x in row["URL"] for x in ['rossmann','/search?text']) or \
             all(x in row["URL"] for x in ['saturn','/search.html?query']) or \
             all(x in row["URL"] for x in ['biccamera','/search/']) or \
             all(x in row["URL"] for x in ['edion','/item_list.html?','keyword']) or \
             all(x in row["URL"] for x in ['rakuten','/search/']) or \
             all(x in row["URL"] for x in ['bestbuy','?st'])or \
             all(x in row["URL"] for x in ['noon','/search/']) or \
             all(x in row["URL"] for x in ['noon','/search?q']) or \
             all(x in row["URL"] for x in ['instashop','/search/']) or \
             all(x in row["URL"] for x in ['carrefouruae','/search?']) or \
             any(x in row["URL"] for x in ['/search','/s?','/suche','keyword']):
            return 'Search Page'
        
        elif all(x in row["URL"] for x in ['amazon','/goldbox']) or \
             all(x in row["URL"] for x in ['amazon','/offers']) or \
             all(x in row["URL"] for x in ['amazon','/angebot']) or \
             all(x in row["URL"] for x in ['amazon','/coupon']) or \
             all(x in row["URL"] for x in ['target','/top-deals']) or \
             all(x in row["URL"] for x in ['target','/circle']) or \
             all(x in row["URL"] for x in ['walmart','/m']) or \
             all(x in row["URL"] for x in ['walmart','/deals']) or \
             all(x in row["URL"] for x in ['bestbuy','sale-page']) or \
             all(x in row["URL"] for x in ['bestbuy','top-deals']) or \
             all(x in row["URL"] for x in ['amazon','bestsellers','Best-Sellers']) or \
             all(x in row["URL"] for x in ['aldi','/weekly-specials']) or \
             all(x in row["URL"] for x in ['kroger','/savings/']) or \
             all(x in row["URL"] for x in ['kroger','weekly-ad']) or \
             all(x in row["URL"] for x in ['kroger','/page/']) or \
             all(x in row["URL"] for x in ['walgreens','/offers']) or \
             all(x in row["URL"] for x in ['amazon','Best-Sellers']) or \
             all(x in row["URL"] for x in ['carrefouruae','/c/clp_online-deals-promotion']) or \
                 any(x in row["URL"] for x in ['deals']):
            return 'Deals Page'
        elif all(x in row["URL"] for x in ['amazon','/ask','/questions']) or \
             all(x in row["URL"] for x in ['amazon','/product-reviews/']) or \
                 all(x in row["URL"] for x in ['amazon','/gp/aw/cr/']) or \
            all(x in row["URL"] for x in ['amazon','/gp/aw/reviews/']):
            return 'Reviews Page'
    
        elif all(x in row["URL"] for x in ['amazon','/c/ref=mw_dp_buy_crt']) or \
             all(x in row["URL"] for x in ['amazon','/cart/']) or \
             all(x in row["URL"] for x in ['amazon','/huc/view.html?']) or \
            all(x in row["URL"] for x in ['otto','/basket/']) or \
             any(x in row["URL"] for x in ['cart', 'checkout']):
            return 'Cart Page'
        #else:
            # return 'Not Found'
    
        else:
                HEADERS = ({'User-Agent':'Mozilla/5.0 (X11; Linux x86_64)AppleWebKit/537.36 (KHTML, like Gecko)Chrome/44.0.2403.157 Safari/537.36','Accept-Language': 'en-US, en;q=0.5'})
                webpage = requests.get(row["URL"], headers=HEADERS, timeout=25)
                soup = BeautifulSoup(webpage.content, "html5lib")
                text = soup.find_all(text=True)
                if text == None:
                    return "No Data"
                else:
                    new = []
                    for i in text:
                        new.append(i.lower())
                    pdp = [i for i in new if i in pdp_key]
                    if len(pdp) > 0 and len(pdp)< 10:
                        return 'PDP html'
                    
                    # cart = []    
                    # for k in cart_key:
                    #     if k in new:
                    #         cart.append(k)
                    #     # if k in str(new):
                    # if len(cart)>0:
                    #     print(row["URL"])
                    #     print(cart)
                    #     return 'Cart Page html'
                        
                    # for j in search_key:
                    #     if j in str(new):
                    #         print(j)
                    #         return 'Search Page html'
                    
                    search = []    
                    for j in search_key:
                        if j in str(new):
                            search.append(j)
                    if len(search)>1:
                        print(row["URL"])
                        print(search)
                        return 'Search Page html'
                        #class_pdp = "YES"
                    # else:
                    #     class_pdp = "NO"
    
        # elif any(x in row["URL"] for x in ['amzn','amazon','walmart','target','costco','cvs','aldi','dollargeneral','ebay',
        #                                             'kroger','samsclub','walgreens','dm', 'mediamarkt','otto', 'rossmann',
        #                                   'saturn', 'biccamera','cosme', 'edion','rakuten','noon','carrefouruae','instashop']):
        #     return 'Others'
    except Exception as e:
        print(e)
        return "error or timeout"
        
            