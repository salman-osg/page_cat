
import pandas as pd
import re
import os

def check_pages(row):
    if not pd.isna(row['URL_Cat']):
        return row['URL_Cat']
    dom = row["domain"].lower()
    match_domain = re.search(f"https:\/\/(www.)?{dom}(\/)?$", row["URL"].lower())
    if match_domain is not None:
        return "Home Page"
    elif all(x in row["URL"] for x in ['amazon','/gp']) or \
         all(x in row["URL"] for x in ['amazon','/dp']) or \
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
         all(x in row["URL"] for x in ['edion','/detail.html?']) or \
         all(x in row["URL"] for x in ['rakuten','product']) or \
         all(x in row["URL"] for x in ['rakuten','item']) or \
         all(x in row["URL"] for x in ['bestbuy','skuId']):
        return 'PDP'
    elif all(x in row["URL"] for x in ['amazon','/stores/']) or \
         all(x in row["URL"] for x in ['target','/b/']):
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
         all(x in row["URL"] for x in ['edion','/item_list.html?']) or \
         all(x in row["URL"] for x in ['rakuten','/search/']) or \
         all(x in row["URL"] for x in ['bestbuy','st']):
        return 'Search Page'
    
    elif all(x in row["URL"] for x in ['amazon','/goldbox']) or \
         all(x in row["URL"] for x in ['amazon','/offers']) or \
         all(x in row["URL"] for x in ['amazon','/angebot']) or \
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
         all(x in row["URL"] for x in ['amazon','Best-Sellers']):
        return 'Deals Page'
    elif all(x in row["URL"] for x in ['amazon','/ask','/questions']) or \
         all(x in row["URL"] for x in ['amazon','/product-reviews/']):
        return 'Reviews Page'
    elif all(x in row["URL"] for x in ['amazon','/c/ref=mw_dp_buy_crt']) or \
         all(x in row["URL"] for x in ['amazon','/cart/']) or \
         all(x in row["URL"] for x in ['amazon','/huc/view.html?']) or \
         all(x in row["URL"] for x in ['target','/co-cart']) or \
         all(x in row["URL"] for x in ['target','/cart']) or \
         all(x in row["URL"] for x in ['dm','/cart']) or \
         all(x in row["URL"] for x in ['otto','/basket/']) or \
         all(x in row["URL"] for x in ['rossmann','/cart/']) or \
         all(x in row["URL"] for x in ['rakuten','/cartall/']) or \
         any(x in row["URL"] for x in ['cart', 'checkout']) :
        return 'Cart Page'
    
    elif any(x in row["URL"] for x in ['amzn','amazon','walmart','target','costco','cvs','aldi','dollargeneral','ebay',
                                                'kroger','samsclub','walgreens','dm', 'mediamarkt','otto', 'rossmann',
                                      'saturn', 'biccamera', 'edion','rakuten']):
        return 'Others'
        
file = pd.read_csv("New_Data_4Feb_prod.csv")
input_file=file
input_file["URL_Cat"] = input_file.apply(check_pages, axis=1)
# assign PDP page when PDP Flag is set to 1
input_file.loc[(input_file["PDP Flag"] == 1)& (input_file['Domain_Type']=='eCommerce'), "URL_Cat"] = "PDP"
# assign search page when searchTerm value exists
input_file.loc[ ((~ input_file["searchTerm"].isnull() ) & input_file['Domain_Type']=='eCommerce'), "URL_Cat"] = "Search Page"
#input_file['Url_cat'].loc[(input_file['PDP Flag'] == 0) & (input_file['Country']=='US') & (input_file['Url_cat']=='PDP')] = 'Others'
input_file['PDP Flag'].loc[(input_file['URL_Cat']=='PDP') & (input_file['Country']!='US')] = 1
input_file.to_csv('New_Data_4Feb_prod.csv',index=False)