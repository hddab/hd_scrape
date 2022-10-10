# hd_scrape
Home depot - scrape by category &amp; store, save to .csv

>scraper_async.py 
- Run to scrape Appliances-Dishwashers-LG,Samsung/Appliances-Refridgerators-Whirlpool,GE/Furniture-Mattresses-Sealy for Manhattan 59th Street and Lemmon Ave stores.

To add brands/categories/stores more easily, below script needs to be finished & passed to scraper.
Alternatively, dictionaries it returns can be used to easily find new arguments for scraper.

>select_scrape(WIP).py
- Pulls sitemap to get list of stores
- Pulls categories and subcategories. Knowing these, can further navigate to get navigation parameter for brands.
