import httpx
import asyncio
import pandas as pd
import itertools
from datetime import datetime
import json

'''
Currently, scraper is pulling requests on startIndex = 0 to get the number of products. 
Based on the number of products from the initial response, it creates the list of new requests. 

Alternatively, could request searchReport only without product data, & therefore reduce the size of next requests.
This would however increase the number of requests sent to the server at the same time (more likely to block).

Didn't encounter any rate limiting, but US-based proxies could be introduced in case that's an issue.
'''

async def get_data(client, store, navParam, startIndex):

    '''
    Get data from the website API using async 'session'.
    Arguments:
        :client : async session,
        :store : (int) store ID from the sitemap
        :navParam : (str) parameter, distinct value for different subcategories & brands
        :startIndex : (int) API responds with approx. 40 products (0-720), therefore we might need to send multiple requests
    Returns response with arguments passed to the function:
        {
            store: store_id,
            navParam: navigation parameter used to identify subcategory/brand,
            startIndex: starting product,
            response: json data returned by the request
        }: dict
    '''

    url = "https://www.homedepot.com/federation-gateway/graphql"

    querystring = {"opname":"searchModel"}

    payload = {
        "operationName": "searchModel",
        "variables": {
            "storefilter": "ALL",
            "channel": "DESKTOP",
            "skipInstallServices": True,
            "skipKPF": True,
            "skipSpecificationGroup": True,
            "skipSubscribeAndSave": True,
            "filter": {},
            "navParam": navParam,
            "orderBy": {
                "field": "PRICE",
                "order": "ASC"
            },                  # Default "Top Sellers" might change more dynamically than price, and that might affect startIndex
            "pageSize": 48,     # over 48 doesn't return any data, or returns inconsistent data
            "startIndex": startIndex,
            "storeId": store
        },
    #-----GraphQL Query starts here
        "query": """query searchModel($keyword: String, $navParam: String, $storefilter: StoreFilter = ALL, $storeId: String, $itemIds: [String], $channel: Channel = DESKTOP, $additionalSearchParams: AdditionalParams, $loyaltyMembershipInput: LoyaltyMembershipInput, $startIndex: Int, $pageSize: Int, $orderBy: ProductSort, $filter: ProductFilter, $zipCode: String, $skipInstallServices: Boolean = true, $skipKPF: Boolean = false, $skipSpecificationGroup: Boolean = false, $skipSubscribeAndSave: Boolean = false) {
    searchModel(keyword: $keyword, navParam: $navParam, storefilter: $storefilter, storeId: $storeId, itemIds: $itemIds, channel: $channel, additionalSearchParams: $additionalSearchParams, loyaltyMembershipInput: $loyaltyMembershipInput) {
        metadata {
        productCount {
            inStore
        }
        }
        id
        searchReport {
        totalProducts
        }
        products(startIndex: $startIndex, pageSize: $pageSize, orderBy: $orderBy, filter: $filter) {
        itemId
        dataSources
        identifiers {
            brandName
            itemId
            productLabel
            modelNumber
            productType
            storeSkuNumber
            parentId
        }
        pricing(storeId: $storeId) {
            value
            original
        }
        availabilityType {
            discontinued
            type
        }
        badges(storeId: $storeId) {
            name
            __typename
        }
        fulfillment(storeId: $storeId, zipCode: $zipCode) {
            backordered
            fulfillmentOptions {
            type
            services {
                locations {
                inventory {
                    quantity
                }
                }
            }
            }
        }
        info {
            categoryHierarchy
            quantityLimit
        }
        installServices(storeId: $storeId, zipCode: $zipCode) @skip(if: $skipInstallServices) {
            scheduleAMeasure
            gccCarpetDesignAndOrderEligible
            __typename
        }
        keyProductFeatures @skip(if: $skipKPF) {
            keyProductFeaturesItems {
            features {
                name
                refinementId
                refinementUrl
                value
                __typename
            }
            __typename
            }
            __typename
        }
        specificationGroup @skip(if: $skipSpecificationGroup) {
            specifications {
            specName
            specValue
            __typename
            }
            specTitle
            __typename
        }
        subscription @skip(if: $skipSubscribeAndSave) {
            defaultfrequency
            discountPercentage
            subscriptionEnabled
            __typename
        }
        }
    }
    }
    """
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:105.0) Gecko/20100101 Firefox/105.0",
        "Accept":"application/json",
        "Accept-Encoding": "gzip, deflate, br",
        "content-type": "application/json",
        "X-Experience-Name": "hd-home",
    }

    response = await client.post(url, headers=headers, json=payload, params=querystring)

    if response.status_code != 200:
        response_dict = {'store':store, 'navParam':navParam, 'startIndex':startIndex, 'response':{'errors':{'message':response.status_code}}} 
    else:
        response_dict = {'store':store, 'navParam':navParam, 'startIndex':startIndex, 'response':response.json()} #Store response in a dictionary along with parameters
    
    return response_dict

async def async_request(stores=None, categories=None, start_indices=None, run_list=[]):
    '''
    Extract data from Home Depot API using get_data. Function creates async session/client, queues the tasks, executes ansd combines them into list of dicts.  
    
    You can pass arguments to get_data in two ways:
        As lists of stores, categories, and start indices: 
            :stores :list[int], 
            :categories :list[str], 
            :start_indices :list[int],
        As a list of tuples, ready to be queued to function pulling data:
            :run_list : list[tuple(store_id::int, navParam::str, start_index::int)]

    Returns:
            [
                {
                    store: int, 
                    navParam: str, 
                    startIndex: int,
                    response: dict
                }: dict
            ]: list
    ''' 
    if stores!=None and categories!=None and start_indices!=None:
        run_list = run_list + list(itertools.product(stores, categories, start_indices))


    async with httpx.AsyncClient() as client:
        tasks = []

        for params in run_list:

            store = int(params[0])
            category = str(params[1])
            start_index = int(params[2])
            tasks.append(asyncio.create_task(get_data(client, store, category, start_index)))

        results = await asyncio.gather(*tasks)

    return results

def combine_results(stores, categories):
    '''
    Pass the list of store IDs and category 'navParams' to get combined results.
    Arguments:
        :stores :list[int], 
        :categories :list[str]
    Returns:
        [
            {
                store: int, 
                navParam: str, 
                startIndex: int,
                response: dict
            }: dict
        ]: list
    '''
    
#run first page (startIndex=0) to get the total number of products
    start_index = [0]

    start_results = asyncio.run(async_request(stores, categories, start_index))

    new_runlist = []

#extract all available results
    for result in start_results:
        if 'errors' in result['response']: # error
            continue
        if len(result['response']['data']['searchModel']['products'])==0: # other error (graphql?)
            continue
        total_products = int(result['response']['data']['searchModel']['searchReport']['totalProducts'])
        if total_products <= 40:
            continue
        elif total_products >= 720:
            print("Too many products. Will pull first 760.")
            total_products = 720 #home depot query errors when trying to get startIndex above 720

        start_indices = list(range(48, total_products, 48))
        new_runlist = new_runlist + list(itertools.product([result['store']], [result['navParam']], start_indices))
    
    if len(new_runlist)>0:
        main_result = asyncio.run(async_request(run_list = new_runlist))
        combined_results = start_results + main_result
    else:
        combined_results = start_results
    
    return combined_results

def format_results(results):
    '''
    Create two dataframes from the response jsons - merge to add quantity available per fulfillment type. 
    ''' 
    availability_df = pd.json_normalize(
                        results, 
                        record_path=['response','data','searchModel','products', 'fulfillment', 'fulfillmentOptions','services','locations'], 
                        meta = [    
                                'store',
                                'navParam',
                                'startIndex',
                                ['response','data', 'searchModel', 'products','itemId'],
                                ['response','data', 'searchModel', 'products','fulfilment','fulfillmentOptions','type']
                                ],
                        errors='ignore'
                        ).rename(columns={ 'response.data.searchModel.products.itemId':'itemId',
                                            'response.data.searchModel.products.fulfillment.fulfillmentOptions.type':'fulfillmentType'}
                                            )

    results_df = pd.json_normalize(
                    results, 
                    record_path=['response','data','searchModel','products'], 
                    meta = [    
                            'store',
                            'navParam',
                            'startIndex',
                            ],
                    errors='ignore'
                    ).drop(columns=['fulfillment.fulfillmentOptions', 
                                    'badges']
                                        )

    combined_df = pd.merge(results_df, availability_df, how='left', on=['store', 'navParam','startIndex','itemId'])
    updated_at = datetime.now()
    combined_df['updatedDate'] = updated_at

    return combined_df

def main(stores, categories):

    run_start = datetime.now()
    results = combine_results(stores, categories)
    results_df = format_results(results)

    print('Extract + transform time:')
    print(datetime.now()-run_start)

    with open('homedepot_data.csv', 'a') as f:
        results_df.to_csv(f, header=f.tell()==0)

    print('Total time:')
    print(datetime.now()-run_start)

if __name__ == "__main__":
    #testing
    stores = ['589', '6177']
    categories = ['5yc1vZc3poZ21j', '5yc1vZc3poZgt7', '5yc1vZc3poZgt8','5yc1vZc3poZa0f','5yc1vZc3piZ4l4','5yc1vZc3piZ30d','5yc1vZc3piZlo','5yc1vZc7oeZf98']
    main(stores,categories)