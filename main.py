import os
import asyncio
import pandas as pd
import websockets
import json
import logging
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict, Set, Tuple
from dhanhq.marketfeed import DhanFeed

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DHAN_CLIENT_ID = os.environ.get("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.environ.get("DHAN_ACCESS_TOKEN")
print(f"--- USING DHAN_CLIENT_ID ---: {DHAN_CLIENT_ID}")
print(f"--- USING DHAN_ACCESS_TOKEN ---: {DHAN_ACCESS_TOKEN}")

# --- Exchange Segment Mapping ---
exchange_segment_map = {
    "NSE_EQ": 1,
    "NSE_FNO": 2,
    "NSE_CURRENCY": 3,
    "BSE_EQ": 11,
    "BSE_CURRENCY": 12,
    "MCX_COMM": 21,
}

# Load the CSV file and create lookup dictionaries
try:
    script_df = pd.read_csv('api-scrip-master.csv')
    script_df.set_index('tradingSymbol', inplace=True)
    symbol_to_security_id = script_df['securityId'].to_dict()
    security_id_to_symbol = {v: k for k, v in symbol_to_security_id.items()}
    symbol_to_exchange_segment = script_df['exchangeSegment'].to_dict()
except FileNotFoundError:
    raise RuntimeError("Error: api-scrip-master.csv not found. Please make sure the file is in the same directory.")
except Exception as e:
    raise RuntimeError(f"Error loading api-scrip-master.csv: {e}")

app = FastAPI()

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# --- Connection Manager for Clients ---
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, List[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, subscription_id: str):
        if subscription_id not in self.active_connections:
            self.active_connections[subscription_id] = []
        self.active_connections[subscription_id].append(websocket)
        logging.info(f"Client {websocket.client} connected and subscribed to {subscription_id}")

    def disconnect(self, websocket: WebSocket, subscription_id: str):
        if subscription_id in self.active_connections:
            self.active_connections[subscription_id].remove(websocket)
            logging.info(f"Client {websocket.client} disconnected from {subscription_id}")
            if not self.active_connections[subscription_id]:
                logging.info(f"No clients left for {subscription_id}. Deleting entry.")
                del self.active_connections[subscription_id]

    def get_clients_for_subscription(self, subscription_id: str) -> List[WebSocket]:
        return self.active_connections.get(subscription_id, [])

    async def broadcast(self, message: dict, subscription_id: str):
        connections = self.active_connections.get(subscription_id, [])[:]
        logging.info(f"Broadcasting to {len(connections)} clients for subscription {subscription_id}")
        for connection in connections:
            logging.info(f"Sending to client {connection.client}: {message}")
            print(f"--- SENDING TO CLIENT ---: {message}")
            await connection.send_json(message)

# --- DhanHQ Feed Manager ---
class DhanFeedManager:
    def __init__(self, client_id: str, access_token: str, connection_manager: ConnectionManager, loop: asyncio.AbstractEventLoop):
        self.client_id = client_id
        self.access_token = access_token
        self.connection_manager = connection_manager
        self.dhan: DhanFeed | None = None
        self.subscribed_instruments: Set[Tuple[int, int]] = set() # (exchange_segment, security_id)
        self.is_running = False
        self.loop = loop

    def on_connect(self, instance):
        logging.info("Connected to DhanHQ WebSocket.")
        self.is_running = True
        if self.subscribed_instruments:
            logging.info(f"Subscribing to all instruments on reconnect: {self.subscribed_instruments}")
            self.dhan.subscribe_symbols(list(self.subscribed_instruments))

    def on_message(self, instance, message):
        print(f"--- DHANHQ RAW RESPONSE ---: {message}")
        logging.info(f"Received message from DhanHQ: {message}")
        print(f"--- DHANHQ RESPONSE ---: {message}")
        feed_code = message.get('type')
        security_id = message.get('securityId')

        if feed_code == 2: # Ticker Packet
            ltp = message.get('ltp')
            ltt = message.get('last_trade_time')
            trading_symbol = security_id_to_symbol.get(int(security_id))
            if trading_symbol:
                binance_like_data = {
                    "e": "kline",
                    "E": ltt,
                    "s": trading_symbol,
                    "k": {
                        "t": ltt,
                        "o": ltp,
                        "h": ltp,
                        "l": ltp,
                        "c": ltp,
                        "v": 0,
                    }
                }
                asyncio.run_coroutine_threadsafe(self.connection_manager.broadcast(binance_like_data, security_id), self.loop)
        elif feed_code == 6:
            logging.info(f"Received heartbeat from DhanHQ: {message}")
        else:
            logging.warning(f"Received unknown message from DhanHQ: {message}")


    def on_error(self, instance, error):
        logging.error(f"An error occurred in DhanHQ listener: {error}")

    def on_close(self, instance):
        logging.warning("DhanHQ WebSocket connection closed. Reconnecting...")
        self.is_running = False
        self.run()


    def run(self):
        if not self.is_running:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self.dhan = DhanFeed(
                self.client_id,
                self.access_token,
                list(self.subscribed_instruments),
                version='v2'
            )
            self.dhan.on_connect = self.on_connect
            self.dhan.on_message = self.on_message
            self.dhan.on_error = self.on_error
            self.dhan.on_close = self.on_close
            self.dhan.run_forever()


    def subscribe(self, instruments: List[Tuple[int, int]]):
        new_instruments = [inst for inst in instruments if inst not in self.subscribed_instruments]
        if not new_instruments:
            return
        logging.info(f"Sending subscription request to DhanHQ for: {new_instruments}")
        self.subscribed_instruments.update(new_instruments)
        if self.dhan and self.is_running:
            self.dhan.subscribe_symbols(list(self.subscribed_instruments))

    def unsubscribe(self, instruments: List[Tuple[int, int]]):
        logging.info(f"Sending unsubscription request to DhanHQ for: {instruments}")
        self.subscribed_instruments.difference_update(instruments)
        if self.dhan and self.is_running:
            self.dhan.unsubscribe_symbols(list(instruments))


# --- FastAPI Setup ---
connection_manager = ConnectionManager()

@app.on_event("startup")
async def startup_event():
    if not DHAN_CLIENT_ID or not DHAN_ACCESS_TOKEN:
        raise RuntimeError("DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN must be set as environment variables.")
    loop = asyncio.get_event_loop()
    global dhan_manager
    dhan_manager = DhanFeedManager(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN, connection_manager, loop)
    loop.run_in_executor(None, dhan_manager.run)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    client_subscriptions: List[str] = []
    try:
        while True:
            data = await websocket.receive_json()
            logging.info(f"Received message from client {websocket.client}: {data}")
            method = data.get('method')
            params = data.get('params', [])

            if not method or not params:
                await websocket.send_json({"error": "Invalid message format"})
                continue

            instrument_tuples = []
            for param in params:
                instrument_name = param.split('@')[0]

                security_id = symbol_to_security_id.get(instrument_name)
                exchange_segment_str = symbol_to_exchange_segment.get(instrument_name)
                exchange_segment_int = exchange_segment_map.get(exchange_segment_str)

                if not security_id or not exchange_segment_int:
                    await websocket.send_json({"error": f"Symbol {instrument_name} not found or missing data"})
                    continue

                subscription_id = str(security_id)
                instrument_tuples.append((exchange_segment_int, int(security_id)))


                if method == 'SUBSCRIBE':
                    await connection_manager.connect(websocket, subscription_id)
                    client_subscriptions.append(subscription_id)

                elif method == 'UNSUBSCRIBE':
                    if subscription_id in client_subscriptions:
                        client_subscriptions.remove(subscription_id)
                        connection_manager.disconnect(websocket, subscription_id)
                        if not connection_manager.get_clients_for_subscription(subscription_id):
                            dhan_manager.unsubscribe([(exchange_segment_int, int(security_id))])

            if method == 'SUBSCRIBE':
                dhan_manager.subscribe(instrument_tuples)
                response = {"result": None, "id": data.get('id')}
                print(f"--- CUSTOM WEBSOCKET RESPONSE TO CLIENT ---: {response}")
                logging.info(f"Sending subscription confirmation to client {websocket.client}: {response}")
                await websocket.send_json(response)
            elif method == 'UNSUBSCRIBE':
                response = {"result": None, "id": data.get('id')}
                print(f"--- CUSTOM WEBSOCKET RESPONSE TO CLIENT ---: {response}")
                logging.info(f"Sending unsubscription confirmation to client {websocket.client}: {response}")
                await websocket.send_json(response)


    except WebSocketDisconnect:
        logging.info(f"Client disconnected: {websocket.client}")
        for sub_id in client_subscriptions:
            connection_manager.disconnect(websocket, sub_id)
            if not connection_manager.get_clients_for_subscription(sub_id):
                for symbol, sec_id in symbol_to_security_id.items():
                    if str(sec_id) == sub_id:
                        exchange_str = symbol_to_exchange_segment.get(symbol)
                        exchange_int = exchange_segment_map.get(exchange_str)
                        instrument_tuple = (exchange_int, int(sub_id))
                        dhan_manager.unsubscribe([instrument_tuple])
                        break

    except Exception as e:
        logging.error(f"An error occurred in websocket endpoint: {e}")

@app.get("/")
def read_root():
    return {"message": "Custom WebSocket Server is running."}
