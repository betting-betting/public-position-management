import pandas as pd
import time

from sql import sqlDF,df_to_sql
from betfair_client import betfair_api
from datetime import datetime
from log_notify import Logger,notify

from imp import reload
import config
reload(config)
from config import strategy_cash_out_limits
from sounds import sounds

sounds = sounds()
betfair = betfair_api()



class place_orders:
    
    
    def __init__(self,sport):
        self.sport = sport
        self.notify = notify()
        
    
    def read_sql(self,path) -> pd.DataFrame:
        file = open(path,'r')
        query : str = file.read()
        data : pd.DataFrame = sqlDF(query)
        return data

    def check_for_new_orders(self) -> pd.DataFrame:
        current_orders : pd.DataFrame = self.read_sql(rf'current_{self.sport}_orders.txt')
        
        placed_orders : pd.DataFrame = self.read_sql(rf'placed_{self.sport}_orders.txt')
        
        if len(current_orders)>0 and len(placed_orders)>0:
            last_placed_time = max(placed_orders['placed_ts'])
            new_orders = current_orders.loc[current_orders['created_ts'] > last_placed_time]
        else:
            new_orders = current_orders
        
        return new_orders
    
    def place_orders(self):
        new_orders : pd.DataFrame = self.check_for_new_orders().reset_index(drop=True)
        order_confirmation : list = []
        if len(new_orders)>0:
            for i in range(len(new_orders)):
                market_id : str = new_orders.loc[i,'market_id']
                selection_id : str = new_orders.loc[i,'selection_id']
                size : str = new_orders.loc[i,'size']
                price : str = new_orders.loc[i,'price']
                direction : str = new_orders.loc[i,'direction']
                strategy : str = new_orders.loc[i,'strategy']
                try:
                    resp = betfair.place_order(market_id,selection_id,direction,size,price)
                    executed_price : str = str(resp['instructionReports'][0]['instruction']['limitOrder']['price'])
                    status : str  = resp['status']
                    if status != 'FAILURE':
                        self.notify.send_message(f'Order placed by {strategy}: a {direction} bet of {size} euro was placed on {selection_id} in {market_id} at odds of {price} @Nathan Clarke')
                    else:
                        self.notify.send_message(f'Error occured with order placed by {strategy}: a {direction} bet of {size} euro failed on {selection_id} in {market_id} at odds of {price}')
                 
                        
                except:
                    executed_price = '999'
                    status : str = 'FAILURE'
                    self.notify.send_message(f'Error occured with order placed by {strategy}: a {direction} bet of {size} euro failed on {selection_id} in {market_id} at odds of {price}')
                    self.notify.send_message(resp)
                
                
                
                if status == 'SUCCESS':
                    sounds.play('order')
                elif status == 'FAILURE':
                    sounds.play('cancel')
                    
                print(f'Order Placed with status: {status}')
                
                order_confirmation.append([strategy,status,market_id,selection_id,size,executed_price,direction,datetime.now()])
            
            placed_orders = pd.DataFrame(order_confirmation,columns = \
                         ['strategy','order_status','market_id','selection_id','size','executed_price','direction','placed_ts'])
                
            df_to_sql(f'placed_{self.sport}_orders', placed_orders)
        else:
            print('No new orders')


class monitor_open_positions(place_orders):

    def __init__(self,sport):
        super().__init__(sport)
        
    def cash_out(self,position):
        market_id = position['market_id']
        selection_id = position['selection_id']
        size = position['size']
        price = 'Last'
        strategy = position['strategy']
        if position['direction'] == 'BACK':
            direction = 'LAY'
        elif position['direction'] == 'LAY':
            direction = 'BACK'
        strategy = 'hedge'
        created_ts = datetime.now()
        order = pd.DataFrame([[market_id,selection_id,size,price,direction,created_ts,strategy]],\
                             columns = ['market_id','selection_id','size','price','direction','created_ts','strategy'])
        df_to_sql('tennis_orders',order) 
        
        
        
    def cash_out_current_positions(self):
        positions : pd.DataFrame = super().read_sql(rf'current_{self.sport}_positions.txt')
        positions['size_adj'] = [float('-'+size) if direction == 'LAY' else float(size) for size,direction in zip(positions['size'],positions['direction'])]
        selection_id_delta_table = pd.DataFrame(positions[['market_id','selection_id','size_adj']]\
                                                .groupby(['market_id','selection_id'])['size_adj'].sum()).reset_index()
        if len(positions)>0:
            for i in range(len(positions)):
                position = positions.loc[i]
                market_id = position['market_id']
                selection_id = position['selection_id']
                current_delta = list(selection_id_delta_table.loc[(selection_id_delta_table['market_id'] == market_id)&\
                        (selection_id_delta_table['selection_id'] == selection_id),'size_adj'])[0]
                
                if current_delta != 0:
                
                    strategy = position['strategy']
                    upper_level = strategy_cash_out_limits[self.sport][strategy]['upper']
                    lower_level = strategy_cash_out_limits[self.sport][strategy]['lower']
                    if position['direction'] == 'BACK':
                        max_profit = float(position['size'])*(float(position['executed_price'])-1)
                        max_loss = float(position['size'])
                    elif position['direction'] == 'LAY':
                        max_loss = float(position['size'])*(float(position['executed_price'])-1)
                        max_profit = float(position['size'])
                    
                    notional_take_profit = upper_level*max_profit
                    notional_stop_loss = lower_level*max_loss*(-1)
                    
                    
                    if position['PnL'] > notional_take_profit and strategy != 'hedge':
                        print('profit taken')
                        self.notify.send_message(f"Taking {position['PnL']} euro expected profit on {selection_id} in {market_id}")
                        self.cash_out(position) #this will add an order to the order table
                    elif position['PnL'] < notional_stop_loss and strategy != 'hedge':
                        print('Loss stopped')
                        print(position['PnL'])
                        self.notify.send_message(f"Stopping expected loss at {position['PnL']} euro on {selection_id} in {market_id}")
                        self.cash_out(position)
      
        
class record_settled_positions(place_orders):
    
    def __init__(self,sport):
        super().__init__(sport)
        
        
    def settled_positions(self) -> pd.DataFrame :
        settled : dict = betfair.todays_settled_pnl()
       
        settled = settled['clearedOrders']
        settled_orders : list = []
        for order in settled:
            market_id : str = order['marketId']
            price_requested : str = str(order['priceRequested'])
            price_matched : str = str(order['priceMatched'])
            selection_id : int = order['selectionId']
            direction : str = order['side']
            size : str = order['sizeSettled']
            profit : str = str(order['profit'])
            settled_date : datetime = datetime.strptime(order['settledDate'],'%Y-%m-%dT%H:%M:%S.000Z')
            settled_orders.append([market_id,selection_id,direction,price_requested,price_matched,size,profit,settled_date])
        settled_orders :pd.DataFrame = pd.DataFrame(settled_orders,columns = ['market_id','selection_id','direction',\
                                                                             'price_requested','price_matched','size','profit','settled_ts'])   
        
        return settled_orders
    
    def check_for_new_settled_positions(self):
        current_settled : pd.DataFrame = super().read_sql(rf'settled_{self.sport}_positions.txt')
        new_settled : pd.DataFrame = self.settled_positions() 
        new_settled['settled_ts'] = new_settled['settled_ts']
        if len(current_settled)>0 and len(new_settled)>0:
            last_settled_time = max(current_settled['settled_ts'])
            
        
            new_settled = new_settled.loc[new_settled['settled_ts'] > last_settled_time]
        
        if len(new_settled)>0:
            df_to_sql(f'settled_{self.sport}_positions',new_settled)
            for i in range(len(new_settled)):
                new_settled = new_settled.reset_index(drop = True)
                market_id = new_settled.loc[i,'market_id']
                profit = new_settled.loc[i,'profit']
                self.notify.send_message(f'{market_id} has settled for a PnL of {profit}')
            
            
        
        #so the hedging seems to work now but seems to do it almost everytime so need to increase the tols
        # every now and again the settled positions betfair seems to not work so see wy that is by looking at the print
        #also need to filter out any settled postions from live PnL do this by joining in the main PnL query
        #also change limit or type of event for smarkets get available events call to prevent data limits being hit                                           
        #need to sort out unmatched positions
                                                    
      
                                                    
      
if __name__ == '__main__':
    sport = 'tennis'
    place_orders = place_orders(sport)
    record_settled_positions = record_settled_positions(sport)
    monitor_open_positions = monitor_open_positions(sport)
    refresh = 10
    try:
        while True:
            place_orders.place_orders()
            if datetime.now().minute%refresh == 0:       
                record_settled_positions.check_for_new_settled_positions()
            monitor_open_positions.cash_out_current_positions()
            time.sleep(5)
    except Exception as e:
        place_orders.notify.send_message(e,'order placer')