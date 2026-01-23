import os
import asyncio
import logging
import warnings
import threading
from datetime import datetime, timezone

from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message

from web3 import Web3
import json
import aiohttp

warnings.filterwarnings('ignore')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ================== ENV ==================

BOT_TOKEN = os.getenv("BOT_TOKEN")
TELEGRAM_USER_ID = int(os.getenv("TELEGRAM_USER_ID"))
BNB_RPC = os.getenv("BNB_RPC", "https://bsc-dataseed.binance.org/")

# ================== TELEGRAM ==================

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ================== WEB3 ==================

w3 = Web3(Web3.HTTPProvider(BNB_RPC))

try:
    from web3.middleware import geth_poa_middleware
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)
except:
    pass

# ================== TOKENS ==================

TOKENS = {
    "BNB": {"address": None, "decimals": 18, "coingecko_id": "binancecoin"},
    "USDT": {"address": "0x55d398326f99059fF775485246999027B3197955", "decimals": 18, "coingecko_id": "tether"},
    "USDC": {"address": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d", "decimals": 18, "coingecko_id": "usd-coin"},
    "BTCB": {"address": "0x7130d2A12B9BCbFAe4f2634d864A1Ee1Ce3Ead9c", "decimals": 18, "coingecko_id": "bitcoin"},
    "MEC": {"address": "0x9a79D9C9e521cb900D2584c74bb41997EB7BF49f", "decimals": 18, "coingecko_id": None},
}

TRANSFER_EVENT_SIGNATURE = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# ================== PRICE CACHE ==================

price_cache = {}
price_cache_time = 0

async def get_token_prices():
    global price_cache, price_cache_time
    now = asyncio.get_event_loop().time()
    if price_cache and now - price_cache_time < 300:
        return price_cache

    ids = [t["coingecko_id"] for t in TOKENS.values() if t["coingecko_id"]]
    if not ids:
        return {}

    url = f"https://api.coingecko.com/api/v3/simple/price?ids={','.join(ids)}&vs_currencies=usd"
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url) as r:
                data = await r.json()
                price_cache = {}
                for k, v in TOKENS.items():
                    if v["coingecko_id"] in data:
                        price_cache[k] = data[v["coingecko_id"]]["usd"]
                price_cache_time = now
    except:
        pass

    return price_cache

# ================== SIMPLE DB ==================

class SimpleDB:
    def __init__(self):
        self.wallets = []
        self.processed = {}
        self.last_block = None
        self.load()

    def load(self):
        if os.path.exists("data.json"):
            with open("data.json") as f:
                d = json.load(f)
                self.wallets = d.get("wallets", [])
                self.processed = {k: True for k in d.get("processed", [])}
                self.last_block = d.get("last_block")

    def save(self):
        with open("data.json", "w") as f:
            json.dump({
                "wallets": self.wallets,
                "processed": list(self.processed.keys()),
                "last_block": self.last_block
            }, f)

    def mark(self, tx, wallet):
        self.processed[f"{tx}:{wallet.lower()}"] = True

    def seen(self, tx, wallet):
        return f"{tx}:{wallet.lower()}" in self.processed

db = SimpleDB()

# ================== HELPERS ==================

def is_authorized(uid):
    return uid == TELEGRAM_USER_ID

def short(addr):
    return f"{addr[:6]}...{addr[-4:]}"

def get_balance(addr, token):
    addr = Web3.to_checksum_address(addr)
    if token == "BNB":
        return float(w3.from_wei(w3.eth.get_balance(addr), "ether"))
    info = TOKENS[token]
    c = w3.eth.contract(address=Web3.to_checksum_address(info["address"]), abi=[
        {"name":"balanceOf","inputs":[{"name":"_owner","type":"address"}],"outputs":[{"type":"uint256"}],"type":"function"}
    ])
    return c.functions.balanceOf(addr).call() / (10 ** info["decimals"])

# ================== TELEGRAM COMMANDS ==================

@dp.message(Command("add_wallet"))
async def add_wallet(m: Message):
    if not is_authorized(m.from_user.id):
        return
    addr = m.text.split()[-1]
    addr = Web3.to_checksum_address(addr)
    db.wallets.append({"address": addr, "name": f"Wallet {len(db.wallets)+1}"})
    db.save()
    await m.answer(f"Кошелёк добавлен:\n{short(addr)}")

@dp.message(Command("balance"))
async def balance(m: Message):
    if not is_authorized(m.from_user.id):
        return
    await get_token_prices()
    for w in db.wallets:
        text = f"{w['name']}\n{short(w['address'])}\n"
        for t in TOKENS:
            b = get_balance(w["address"], t)
            text += f"{t}: {b:.6f}\n"
        await m.answer(text)

# ================== ALERT ==================

async def alert(w, token, amount, direction, tx):
    emoji = "🟢" if direction == "IN" else "🔴"
    await bot.send_message(
        TELEGRAM_USER_ID,
        f"{emoji} {direction} {amount:.6f} {token}\n{w['name']}\nhttps://bscscan.com/tx/{tx}"
    )

# ================== MONITOR ==================

def monitor():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    last = db.last_block or w3.eth.block_number
    token_map = {v["address"].lower(): k for k,v in TOKENS.items() if v["address"]}

    while True:
        try:
            cur = w3.eth.block_number
            if cur > last:
                for bn in range(last+1, min(cur, last+5)+1):
                    block = w3.eth.get_block(bn, full_transactions=True)
                    for tx in block.transactions:
                        txh = tx.hash.hex()
                        for w in db.wallets:
                            addr = w["address"].lower()
                            if tx.value > 0:
                                if tx.to and tx.to.lower() == addr and not db.seen(txh, addr):
                                    loop.run_until_complete(alert(w, "BNB", w3.from_wei(tx.value,"ether"), "IN", txh))
                                    db.mark(txh, addr)
                    receipt = w3.eth.get_transaction_receipt(txh)
                    for log in receipt.logs:
                        if log.topics and log.topics[0].hex() == TRANSFER_EVENT_SIGNATURE:
                            token = token_map.get(log.address.lower())
                            if not token:
                                continue
                            to_addr = "0x" + log.topics[2].hex()[-40:]
                            for w in db.wallets:
                                if to_addr.lower() == w["address"].lower():
                                    amt = int(log.data.hex(),16)/(10**TOKENS[token]["decimals"])
                                    loop.run_until_complete(alert(w, token, amt, "IN", txh))
                last = bn
                db.last_block = last
                db.save()
            asyncio.sleep(10)
        except:
            asyncio.sleep(5)

# ================== MAIN ==================

async def main():
    t = threading.Thread(target=monitor, daemon=True)
    t.start()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
