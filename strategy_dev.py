'''
(0-A) Check if market if open?
    (0-A-A) Orders not eligible for extended hours submitted after 4:00pm ET will be queued up for release the next trading day
    (0-A-B) eligible for extended hours - https://docs.alpaca.markets/docs/orders-at-alpaca#extended-hours-trading
(0-B) Do we need to filter before market, on open, after close?
(0-C) US-Close+DE-Open, etc. Change in Daylight-time
(0-D) Do we need to close some/all positions before market close, normal/long weekends-holidays, eom, eoq? 
        Probably depends on strategy 

(1-A) Alpaca screen of total assets in scope => alp_stocks_all
(1-B) Trade often? Size of bid-ask spread? Screen alp_stocks_all for spread => alp_stocks_all_small_spread
(1-C) Add filter from Refinitiv list from GH?
(1-D) Filter stocks which were growing smoothly? on day time frame, on hour time frame on minute time frame
(1-E) Check if a stock is a contitute of certain interesting ETF?
(1-F) Check top movers for certain time frame?

=> filtered_stocks_final

(2-A) Check if there is a position for the filtered_stocks_final
    (2-A-A) If there is a position, does it have a stop-loss? Is the stop-loss moving?
        (2-A-A-A) If no stop-loss, check the P&L% of the position:
            (2-A-A-A-A) if ok - put a moving stop-loss
            (2-A-A-A-B) if not - sell
    (2-A-B) If there is a position, we do not filter it out, as the volume should be adjusted    
(2-B) Check if there was a position for the filtered_stocks_final which was recently closed
    (2-B-A) What is "recently"? 
    (2-B-B) Why the position was closed?
(2-C) Filter out only recently-closed from filtered_stocks_final

=> stocks_to_buy

(3-A) How large is the stocks_to_buy list?:
    (3-A-A) Less than X stocks? => go to (4)
    (3-A-B) More than X stocks?:
        (3-A-B-A) Pick random X?
        (3-A-B-B) Filter by smth else to top-X?

=> X_stocks_to_buy

How much money from total is devoted to this strategy? 
Which accounts: only alpaca or manual trading as well? Shall I ahve separate file with accounts size?
Strategy positions could be opened from different ways (function1, function2, container1, container2, manually, what else?)
Is the strategy have already positions? How much is available to invest for this strategy?

=> Volume_available_for_strategy

How to allocate Volume_available_for_strategy across X_stocks_to_buy?
Stock_1 - To-be-volume_1 - As-is-volume_1
Equally?
Kelly?

=> Stock-Volume = {Stock_1: New-volume_1, ...}

Again check if market is open?
Sending order:
    - Symbol available
    - Num-of-stocks: get current price

    - Direction: Buy or sell
    - Stop-loss: (a) function of price, (b) fixed % of buy price
    - Take profit
    - TimeInForce
    - OrderClass: always bracket?

Check if order was executed (or if it is in pipeline): dir(OrderStatus)


'''