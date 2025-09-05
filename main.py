import os
import asyncio
import pandas as pd
import websockets
import struct
import json
import logging
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict, Set, Tuple

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

# --- DhanHQ Feed Manager ---
class DhanFeedManager:
    def __init__(self, client_id: str, access_token: str, connection_manager: ConnectionManager):
        self.client_id = client_id
        self.access_token = access_token
        self.connection_manager = connection_manager
        self.websocket: websockets.WebSocketClientProtocol | None = None
        self.subscribed_instruments: Set[Tuple[str, str]] = set() # (exchange_segment, security_id)
        self.is_running = False

    async def connect(self):
        uri = f"wss://api-feed.dhan.co?version=2&token={self.access_token}&clientId={self.client_id}&authType=2"
        try:
            self.websocket = await websockets.connect(uri)
            logging.info("Connected to DhanHQ WebSocket.")
            self.is_running = True
            if self.subscribed_instruments:
                await self.subscribe(list(self.subscribed_instruments))
        except Exception as e:
            logging.error(f"Failed to connect to DhanHQ WebSocket: {e}")
            self.is_running = False

    async def subscribe(self, instruments: List[Tuple[str, str]]):
        if not self.websocket:
            return

        new_instruments = [inst for inst in instruments if inst not in self.subscribed_instruments]
        if not new_instruments:
            return

        self.subscribed_instruments.update(new_instruments)

        # Group instruments by exchange segment
        grouped_instruments: Dict[str, List[Dict[str, str]]] = {}
        for ex, sec_id in new_instruments:
            if ex not in grouped_instruments:
                grouped_instruments[ex] = []
            grouped_instruments[ex].append({"securityId": str(sec_id)})

        # Create and send a subscription message for each group
        for ex, inst_list in grouped_instruments.items():
            subscription_message = {
                "RequestCode": 15,
                "InstrumentCount": len(inst_list),
                "InstrumentList": [{"exchangeSegment": ex, "securityId": inst["securityId"]} for inst in inst_list]

            }
            logging.info(f"Sending subscription request to DhanHQ for {ex}: {subscription_message}")
            await self.websocket.send(json.dumps(subscription_message))

    async def unsubscribe(self, instruments: List[Tuple[str, str]]):
        if not self.websocket:
            return

        self.subscribed_instruments.difference_update(instruments)

        # Group instruments by exchange segment
        grouped_instruments: Dict[str, List[Dict[str, str]]] = {}
        for ex, sec_id in instruments:
            if ex not in grouped_instruments:
                grouped_instruments[ex] = []
            grouped_instruments[ex].append({"securityId": str(sec_id)})

        # Create and send an unsubscription message for each group
        for ex, inst_list in grouped_instruments.items():
            unsubscription_message = {
                "RequestCode": 16,
                "InstrumentCount": len(inst_list),
                "InstrumentList": [{"exchangeSegment": ex, "securityId": inst["securityId"]} for inst in inst_list]
            }
            logging.info(f"Sending unsubscription request to DhanHQ for {ex}: {unsubscription_message}")
            await self.websocket.send(json.dumps(unsubscription_message))

    async def listen(self):
        while self.is_running:
            try:
                message = await self.websocket.recv()
                logging.info(f"Received message from DhanHQ: {message}")
                buffer = message
                while buffer:
                    header = struct.unpack('<BHBI', buffer[:8])
                    message_length = header[1]
                    self.parse_message(buffer[:8+message_length])
                    buffer = buffer[8+message_length:]

            except websockets.exceptions.ConnectionClosed:
                logging.warning("DhanHQ WebSocket connection closed. Reconnecting...")
                await self.connect()
            except Exception as e:
                logging.error(f"An error occurred in DhanHQ listener: {e}")

    def parse_message(self, message: bytes):
        if len(message) < 8:
            return

        header = struct.unpack('<BHBI', message[:8])
        feed_code = header[0]
        message_length = header[1]
        security_id = str(header[3])

        logging.info(f"Parsing message with feed_code: {feed_code}, message_length: {message_length}, security_id: {security_id}")

        if feed_code == 2:
            if message_length == 16:
                if len(message) - 8 >= 8:
                    try:
                        payload = struct.unpack('<fi', message[8:])
                        ltp = payload[0]
                        ltt = payload[1]

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
                            asyncio.create_task(self.connection_manager.broadcast(binance_like_data, security_id))

                    except struct.error as e:
                        logging.error(f"[ERROR] Failed to unpack Ticker packet for SID {security_id}: {e}")
                else:
                    logging.error(f"[ERROR] Ticker packet for SID {security_id} has insufficient length: {len(message)}")
            else:
                logging.warning(f"Received Ticker packet with unexpected length for SID {security_id}: {message_length}")

        elif feed_code == 6:
            logging.info(f"Received heartbeat from DhanHQ: {message}")

        else:
            logging.warning(f"Received unknown message from DhanHQ: {message}")

    async def run(self):
        await self.connect()
        if self.is_running:
            await self.listen()

# --- FastAPI Setup ---
connection_manager = ConnectionManager()
dhan_manager = DhanFeedManager(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN, connection_manager)

@app.on_event("startup")
async def startup_event():
    if not DHAN_CLIENT_ID or not DHAN_ACCESS_TOKEN:
        raise RuntimeError("DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN must be set as environment variables.")
    asyncio.create_task(dhan_manager.run())

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
                        if not connection_manager.get_clients_for_subscription(subscription_id):
                            await dhan_manager.unsubscribe([(exchange_segment, subscription_id)])

            if method == 'SUBSCRIBE':
                await dhan_manager.subscribe(instrument_tuples)
                await websocket.send_json({"result": None, "id": data.get('id')})
            elif method == 'UNSUBSCRIBE':
                await websocket.send_json({"result": None, "id": data.get('id')})


    except WebSocketDisconnect:
        logging.info(f"Client disconnected: {websocket.client}")
        for sub_id in client_subscriptions:
            connection_manager.disconnect(websocket, sub_id)
            if not connection_manager.get_clients_for_subscription(sub_id):
                for symbol, sec_id in symbol_to_security_id.items():
                    if str(sec_id) == sub_id:
                        exchange = symbol_to_exchange_segment.get(symbol)
                        instrument_tuple = (exchange, sub_id)
                        await dhan_manager.unsubscribe([instrument_tuple])
                        break

    except Exception as e:
        logging.error(f"An error occurred in websocket endpoint: {e}")

@app.get("/")
def read_root():
    return {"message": "Custom WebSocket Server is running."}
