import os
import asyncio
import pandas as pd
import logging
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict
from dhanhq import marketfeed

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DHAN_CLIENT_ID = os.environ.get("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.environ.get("DHAN_ACCESS_TOKEN")

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
        logging.info(f"Broadcasting to {len(connections)} clients for subscription {subscription_id}: {message}")
        for connection in connections:
            await connection.send_json(message)

# --- DhanHQ Client Setup ---
connection_manager = ConnectionManager()

def on_message(message):
    logging.info(f"Received message from DhanHQ: {message}")
    security_id = message['security_id']
    trading_symbol = security_id_to_symbol.get(int(security_id))
    if trading_symbol:
        binance_like_data = {
            "e": "kline",
            "E": message['exchange_timestamp'],
            "s": trading_symbol,
            "k": {
                "t": message['exchange_timestamp'],
                "o": message['ltp'],
                "h": message['ltp'],
                "l": message['ltp'],
                "c": message['ltp'],
                "v": 0,
            }
        }
        asyncio.run(connection_manager.broadcast(binance_like_data, security_id))

def on_error(error):
    logging.error(f"Received error from DhanHQ: {error}")

def on_close(close_status_code, close_msg):
    logging.info(f"Connection closed with status code: {close_status_code}, message: {close_msg}")

def on_open():
    logging.info("Connected to DhanHQ WebSocket.")

feed = marketfeed(
    client_id=DHAN_CLIENT_ID,
    access_token=DHAN_ACCESS_TOKEN,
    instruments=[],
    on_tick=on_message,
    on_error=on_error,
    on_close=on_close,
    on_open=on_open
)

@app.on_event("startup")
async def startup_event():
    if not DHAN_CLIENT_ID or not DHAN_ACCESS_TOKEN:
        raise RuntimeError("DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN must be set as environment variables.")
    asyncio.create_task(feed.run_forever())

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
                exchange_segment = symbol_to_exchange_segment.get(instrument_name)

                if not security_id or not exchange_segment:
                    await websocket.send_json({"error": f"Symbol {instrument_name} not found or missing data"})
                    continue

                subscription_id = str(security_id)
                instrument_tuples.append((exchange_segment, subscription_id))

                if method == 'SUBSCRIBE':
                    await connection_manager.connect(websocket, subscription_id)
                    client_subscriptions.append(subscription_id)

                elif method == 'UNSUBSCRIBE':
                    if subscription_id in client_subscriptions:
                        client_subscriptions.remove(subscription_id)
                        connection_manager.disconnect(websocket, subscription_id)
            
            if method == 'SUBSCRIBE':
                feed.subscribe_symbols(instrument_tuples)
                await websocket.send_json({"result": None, "id": data.get('id')})
            elif method == 'UNSUBSCRIBE':
                feed.unsubscribe_symbols(instrument_tuples)
                await websocket.send_json({"result": None, "id": data.get('id')})


    except WebSocketDisconnect:
        logging.info(f"Client disconnected: {websocket.client}")
        for sub_id in client_subscriptions:
            connection_manager.disconnect(websocket, sub_id)

    except Exception as e:
        logging.error(f"An error occurred in websocket endpoint: {e}")

@app.get("/")
def read_root():
    return {"message": "Custom WebSocket Server is running."}
