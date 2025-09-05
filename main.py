import os
import asyncio
import pandas as pd
import websockets
import struct
import json
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict, Set, Tuple

# --- Configuration ---
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
        print(f"Client {websocket.client} connected and subscribed to {subscription_id}")

    def disconnect(self, websocket: WebSocket, subscription_id: str):
        if subscription_id in self.active_connections:
            self.active_connections[subscription_id].remove(websocket)
            print(f"Client {websocket.client} disconnected from {subscription_id}")
            if not self.active_connections[subscription_id]:
                print(f"No clients left for {subscription_id}. Deleting entry.")
                del self.active_connections[subscription_id]

    def get_clients_for_subscription(self, subscription_id: str) -> List[WebSocket]:
        return self.active_connections.get(subscription_id, [])

    async def broadcast(self, message: dict, subscription_id: str):
        connections = self.active_connections.get(subscription_id, [])[:]
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
            print("Connected to DhanHQ WebSocket.")
            self.is_running = True
            if self.subscribed_instruments:
                await self.subscribe(list(self.subscribed_instruments))
        except Exception as e:
            print(f"Failed to connect to DhanHQ WebSocket: {e}")
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
            await self.websocket.send(json.dumps(subscription_message))
            print(f"Sent subscription request for {ex}: {subscription_message}")

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
            await self.websocket.send(json.dumps(unsubscription_message))
            print(f"Sent unsubscription request for {ex}: {unsubscription_message}")

    async def listen(self):
        while self.is_running:
            try:
                message = await self.websocket.recv()
                self.parse_message(message)
            except websockets.exceptions.ConnectionClosed:
                print("DhanHQ WebSocket connection closed. Reconnecting...")
                await self.connect()
            except Exception as e:
                print(f"An error occurred in DhanHQ listener: {e}")

    def parse_message(self, message: bytes):
        if len(message) < 8:
            return

        header = struct.unpack('<BHBI', message[:8])
        feed_code = header[0]
        message_length = header[1]
        security_id = str(header[3])

        if feed_code == 2 and message_length == 16:
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
                print(f"[ERROR] Failed to unpack Ticker packet for SID {security_id}: {e}")
        
        elif feed_code == 6:
            pass
        
        else:
            pass

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
            method = data.get('method')
            params = data.get('params', [])

            if not method or not params:
                await websocket.send_json({"error": "Invalid message format"})
                continue

            instrument_name = params[0].split('@')[0]
            
            security_id = symbol_to_security_id.get(instrument_name)
            exchange_segment = symbol_to_exchange_segment.get(instrument_name)

            if not security_id or not exchange_segment:
                await websocket.send_json({"error": f"Symbol {instrument_name} not found or missing data"})
                continue
            
            subscription_id = str(security_id)
            instrument_tuple = (exchange_segment, subscription_id)

            if method == 'SUBSCRIBE':
                await connection_manager.connect(websocket, subscription_id)
                client_subscriptions.append(subscription_id)
                await dhan_manager.subscribe([instrument_tuple])
                await websocket.send_json({"result": None, "id": data.get('id')})

            elif method == 'UNSUBSCRIBE':
                if subscription_id in client_subscriptions:
                    client_subscriptions.remove(subscription_id)
                    connection_manager.disconnect(websocket, subscription_id)
                    if not connection_manager.get_clients_for_subscription(subscription_id):
                        await dhan_manager.unsubscribe([instrument_tuple])
                await websocket.send_json({"result": None, "id": data.get('id')})

    except WebSocketDisconnect:
        print(f"Client disconnected: {websocket.client}")
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
        print(f"An error occurred in websocket endpoint: {e}")

@app.get("/")
def read_root():
    return {"message": "Custom WebSocket Server is running."}
