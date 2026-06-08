import asyncio
import motor.motor_asyncio
from pymongo.errors import CollectionInvalid

MONGO_URI = "mongodb://192.168.1.126:27017"
DB_NAME = "trading_data"

COLLECTION_NAMES = [
    "nasdaq_production_tickers",
    
]

async def create_collections():
    client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
    try:
        db = client[DB_NAME]

        existing = set(await db.list_collection_names())
        created = []

        for name in COLLECTION_NAMES:
            if name in existing:
                continue
            try:
                await db.create_collection(name)
                created.append(name)
            except CollectionInvalid:
                # In case of race conditions / already created elsewhere
                pass

        print("Existing:", ", ".join(sorted(existing & set(COLLECTION_NAMES))) or "(none)")
        print("Created:", ", ".join(created) or "(none)")

    finally:
        client.close()

asyncio.run(create_collections())
