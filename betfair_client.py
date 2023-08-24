import json
import datetime
import urllib
import urllib.request
import urllib.error
import requests
import pandas as pd
from config import betfair_configuration as configuration
from pandas.io.json._normalize import nested_to_record  
from datetime import datetime

#I now only have the ssoid being generated once in it init func and the variable created is now being called where the func once was
#This should fix the request errors 

class betfair_api:

    bet_url : str = configuration['api']['bet_url']
    password : str = configuration['auth']['password']
    username : str = configuration['auth']['username']
    app_key : str = configuration['auth']['app_key']
    

    def __init__(self):
        self.ssoid = self.generate_ssoid()
    
    def generate_ssoid(self) -> str :
        headers : dict = {'X-Application': self.app_key, 'Content-Type': 'application/x-www-form-urlencoded'}
        payload : str = f'username={self.username}&password={self.password}'
        resp = requests.post('https://identitysso-cert.betfair.com/api/certlogin',data=payload,cert=('betfair.crt','betfair.pem'),headers=headers)
        json_resp : dict = resp.json()
        try:
            SSOID : str = json_resp['sessionToken']
        except KeyError:
            raise Exception('Data limit hit')
        return SSOID
        
        
    def event_req(self):
        """use this to concat the event_req str vars are:
            the method bit and params. Params can be filters, max results, market projection. Filters can be
            event ids, market types, market times etc etc etc"""
        pass

    def callApi(self,event_req) -> dict :
        headers : dict = {'X-Application': self.app_key, 'X-Authentication': self.ssoid, 'content-type': 'application/json'}
        try:
            req = requests.post(self.bet_url, data=event_req.encode('utf-8'), headers=headers) 
            response : dict = req.json()
            return response['result']
        except Exception as error:
            print(f'Error occured: {error}')
            return response['error']
            
    def event_type_id_mapping(self):
        """ used to populate EventType_ID_Mapping table"""
        event_req : str = '{"jsonrpc": "2.0", "method": "SportsAPING/v1.0/listEventTypes", "params": {"filter":{ }}, "id": 1}'
        data : dict = self.callApi(event_req)
        
        mapping : list = [element['eventType'] for element in data]
        
        return pd.DataFrame(mapping)
        
    def events(self,event_type_ids,time_from=0,time_to=0) -> pd.DataFrame :
        '''maybe let it take event type name as arg and sort mapping in func
        this defaults to inplay but if you give datetime args for time_from and time_to it will give 
        events in that timeframe'''
        
        inplay='true'
        event_type_ids : str = str(event_type_ids).replace("'",'"')
        
        if time_from !=0:
            time_from : str = time_from.strftime('%Y-%m-%dT%H:%M:%SZ')
            time_to : str = time_to.strftime('%Y-%m-%dT%H:%M:%SZ')
            
            req : str = '''{
            "jsonrpc": "2.0",
            "method": "SportsAPING/v1.0/listEvents",
            "params": {
            "filter": {
            "eventTypeIds": '''+event_type_ids+''',
            "marketStartTime": {
            "from": "'''+time_from+'''",
            "to": "'''+time_to+'''"
            }
            }
            },
            "id": 1
            }'''
        else:
            req : str = '''{
            "jsonrpc": "2.0",
            "method": "SportsAPING/v1.0/listEvents",
            "params": {
            "filter": {
            "eventTypeIds": '''+event_type_ids+''',
            "inPlayOnly":'''+inplay+'''
            
            }
            }
            },
            "id": 1
            }'''
            
        return pd.DataFrame(nested_to_record(self.callApi(req), sep='_'))
            
    
    def market_info(self,event_ids) -> dict :
        '''this can be filtered in the json for specific market types or you could do this yourelf'''
        
        event_ids : str = str(event_ids).replace("'",'"')
    
        req : str = '''{
        "jsonrpc": "2.0",
        "method": "SportsAPING/v1.0/listMarketCatalogue",
        "params": {
        "filter": {
        "eventIds": '''+event_ids+'''
        },
        "maxResults": "200",
        "marketProjection": [
        "COMPETITION",
        "EVENT",
        "EVENT_TYPE",
        "RUNNER_DESCRIPTION",
        "RUNNER_METADATA",
        "MARKET_START_TIME"
        ]
        },
        "id": 1
        }'''
    
        return self.callApi(req)
    
    def market_types(self,event_ids) -> pd.DataFrame :
        
        event_ids : str = str(event_ids).replace("'",'"')
    
        req : str = '''{"jsonrpc": "2.0", 
        "method": "SportsAPING/v1.0/listMarketTypes", 
        "params": {
            "filter":{
                "eventIds":'''+event_ids+'''}},
        "id": 1
        }'''
    
        return pd.DataFrame(nested_to_record(self.callApi(req), sep='_'))
    
    
    def competitions(self,event_type_ids) -> dict :
        '''this can be filtered in the json for specific market types or you could do this yourelf'''
        
        event_type_ids : str = str(event_type_ids).replace("'",'"')
    
        req : str = '''{
           "params": {
          "filter": {
         "eventTypeIds": '''+event_type_ids+'''
          }
           },
           "jsonrpc": "2.0",
           "method": "SportsAPING/v1.0/listCompetitions",
           "id": 1
        }'''
    
        return self.callApi(req)
    
    def price_data(self,market_ids) -> dict :
        
        market_ids : str = str(market_ids).replace("'",'"')
        
        req : str = '''{
        "jsonrpc": "2.0",
        "method": "SportsAPING/v1.0/listMarketBook",
        "params": {
            "marketIds": '''+market_ids+''',
            "priceProjection": {
                "priceData": ["EX_BEST_OFFERS", "EX_TRADED"],
                "virtualise": "true"
            }
        },
        "id": 1
        }'''
        return self.callApi(req)
    
    def place_order(self,market_id,selection_id,side,size,price = 'Last',handicap = 0) -> str :
        
        market_id : str = str(market_id)
        selection_id : str = str(selection_id)
        handicap : str = str(handicap)
        side : str = str(side)
        if side not in ("BACK","LAY"):
            print('Invalid side, BACK/LAY')
        size : str = str(size)
        price : str = str(price)
        
        if price == 'Last':    
            market_prices  = self.price_data([market_id])[0]
            try:
                price = str([runner['ex'][f'availableTo{side.lower().capitalize()}'][0]['price']  \
                         for runner in market_prices['runners'] if str(runner['selectionId']) == selection_id][0])
            except:
                price : str = '2'
                print('override price used')
        
        req : str = '''{
        "jsonrpc": "2.0",
        "method": "SportsAPING/v1.0/placeOrders",
        "params": {
        "marketId": "'''+market_id+'''",
        "instructions": [
        {
        "selectionId": "'''+selection_id+'''",
        "handicap": "'''+handicap+'''",
        "side": "'''+side+'''",
        "orderType": "LIMIT",
        "limitOrder": {
        "size": "'''+size+'''",
        "price": "'''+price+'''",
        "persistenceType": "LAPSE"
        }
        }
        ]
        },
        "id": 1
        }'''
    
        return self.callApi(req)
    
    def current_orders(self):
       
        event_req : str = '{"jsonrpc": "2.0", "method": "SportsAPING/v1.0/listCurrentOrders", "params": {"filter":{ }}, "id": 1}'
        data : dict = self.callApi(event_req)
        
       
        return data
    
    
    def todays_settled_pnl(self):
        
        today = datetime.now().strftime('%Y-%m-%d')
        

        req = '''{
        "jsonrpc": "2.0", 
         "method": "SportsAPING/v1.0/listClearedOrders", 
         "params": {"betStatus":"SETTLED","settledDateRange":{"from":"'''+today+'''T00:00:00Z"}}, "id": 1}
        '''
        
        return self.callApi(req)
    
    
    def market_ids(self,event_ids,market_name,matched_lower_bound) -> pd.DataFrame :
        '''this can be filtered in the json for specific market types or you could do this yourelf'''
        
        event_ids : str = str(event_ids).replace("'",'"')
    
        req : str = '''{
        "jsonrpc": "2.0",
        "method": "SportsAPING/v1.0/listMarketCatalogue",
        "params": {
        "filter": {
        "eventIds": '''+event_ids+'''
        },
        "maxResults": "200",
        "marketProjection": [
            "EVENT"
            
        ]
        },
        "id": 1
        }'''
    
        data : pd.DataFrame = pd.DataFrame(nested_to_record(self.callApi(req), sep='_'))
        
        data_filtered : pd.DataFrame = data.loc[(data['marketName']==market_name)&(data['totalMatched']>matched_lower_bound)][['event_name','marketId','event_id']]
        data_filtered.columns=['event_name','Betfair_market_id','Betfair_event_id']
        return data_filtered
        
    
    def cancel_order(self,market_id = 'None'):
        
        if market_id == 'None':
            req = '''{
            "jsonrpc": "2.0", 
            "method": "SportsAPING/v1.0/cancelOrders", 
            "params": {}, "id": 1
            }'''
        
        else:
            req = '''
            {"jsonrpc": "2.0", 
             "method": "SportsAPING/v1.0/cancelOrders", 
             "params": {"marketId":"'''+market_id+'''"}, "id": 1
             }'''
        
        return self.callApi(req)
            
        
        
    def selection_id_player_name(self,market_ids : list) -> pd.DataFrame:
        
        market_ids_ : str = str(market_ids).replace("'",'"')
        
        
        req : str = '''{
        "jsonrpc": "2.0", 
        "method": "SportsAPING/v1.0/listMarketCatalogue", 
        "params": {
            "filter":{
                "marketIds":'''+market_ids_+'''},
            "maxResults":"'''+str(len(market_ids))+'''",
            "marketProjection":["RUNNER_DESCRIPTION"]}, 
        "id": 1}
        '''
        
        data : dict = self.callApi(req)
        
        runner_name_data : list = []
        for i in range(len(data)):
            market_id : str = data[i]['marketId']
            runners :list = data[i]['runners']
            for j in range(len(runners)):
              selection_id : int = int(runners[j]['selectionId'])
              runner_name : str = runners[j]['runnerName']
              runner_name_data.append([market_id,selection_id,runner_name])
              
        return pd.DataFrame(runner_name_data,columns = ['market_id','selection_id','name'])
              
               
    
        
      
            
            
            