import os
import asyncio
import logging
from datetime import datetime
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import Message
from web3 import Web3
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
TELEGRAM_USER_ID = int(os.getenv("TELEGRAM_USER_ID"))
BNB_RPC = os.getenv("BNB_RPC", "https://bsc-dataseed.binance.org/")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
w3 = Web3(Web3.HTTPProvider(BNB_RPC))

TOKENS = {
    "BNB": {
        "address": None,
        "decimals": 18
    },
    "USDT": {
        "address": "0x55d398326f99059fF775485246999027B3197955",
        "decimals": 18
    },
    "USDC": {
        "address": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
        "decimals": 18
    },
    "BTCB": {
        "address": "0x7130d2A12B9BCbFAe4f2634d864A1Ee1Ce3Ead9c",
        "decimals": 18
    }
}

ERC20_ABI = json.loads('[{"constant":true,"inputs":[{"name":"_owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"balance","type":"uint256"}],"type":"function"},{"anonymous":false,"inputs":[{"indexed":true,"name":"from","type":"address"},{"indexed":true,"name":"to","type":"address"},{"indexed":false,"name":"value","type":"uint256"}],"name":"Transfer","type":"event"}]')

class SimpleDB:
    def __init__(self):
        self.wallets = []
        self.processed_txs = set()
        self.last_block = 0
        self.load()
    
    def load(self):
        try:
            if os.path.exists("data.json"):
                with open("data.json", "r") as f:
                    data = json.load(f)
                    self.wallets = data.get("wallets", [])
                    self.processed_txs = set(data.get("processed_txs", []))
                    self.last_block = data.get("last_block", 0)
        except Exception as e:
            logger.error(f"Ошибка загрузки БД: {e}")
    
    def save(self):
        try:
            with open("data.json", "w") as f:
                json.dump({
                    "wallets": self.wallets,
                    "processed_txs": list(self.processed_txs),
                    "last_block": self.last_block
                }, f, indent=2)
        except Exception as e:
            logger.error(f"Ошибка сохранения БД: {e}")
    
    def add_wallet(self, address, name="Main"):
        wallet = {"address": address, "name": name}
        if wallet not in self.wallets:
            self.wallets.append(wallet)
            self.save()
            return True
        return False
    
    def mark_processed(self, tx_hash):
        self.processed_txs.add(tx_hash)
        self.save()
    
    def is_processed(self, tx_hash):
        return tx_hash in self.processed_txs
    
    def update_last_block(self, block_num):
        self.last_block = block_num
        self.save()

db = SimpleDB()

def get_balance(address, token_symbol):
    try:
        address = Web3.to_checksum_address(address)
        
        if token_symbol == "BNB":
            balance_wei = w3.eth.get_balance(address)
            balance = w3.from_wei(balance_wei, 'ether')
        else:
            token_info = TOKENS[token_symbol]
            token_address = Web3.to_checksum_address(token_info["address"])
            contract = w3.eth.contract(address=token_address, abi=ERC20_ABI)
            balance_raw = contract.functions.balanceOf(address).call()
            balance = balance_raw / (10 ** token_info["decimals"])
        
        return float(balance)
    except Exception as e:
        logger.error(f"Ошибка получения баланса {token_symbol}: {e}")
        return 0.0

def format_address(address):
    if not address:
        return ""
    return f"{address[:6]}…{address[-4:]}"

def format_balance(amount):
    if amount >= 1:
        return f"{amount:,.2f}"
    else:
        return f"{amount:.4f}"

def is_authorized(user_id: int) -> bool:
    return user_id == TELEGRAM_USER_ID

@dp.message(Command("start"))
async def cmd_start(message: Message):
    if not is_authorized(message.from_user.id):
        await message.answer("❌ Доступ запрещён")
        return
    
    await message.answer(
        "👋 БНБ Бухгалтер запущен!\n\n"
        "Команды:\n"
        "/balance — текущие балансы\n"
        "/add_wallet <адрес> — добавить кошелёк\n"
        "/wallets — список кошельков"
    )

@dp.message(Command("balance"))
async def cmd_balance(message: Message):
    if not is_authorized(message.from_user.id):
        await message.answer("❌ Доступ запрещён")
        return
    
    if not db.wallets:
        await message.answer("ℹ️ Нет добавленных кошельков.\nИспользуй /add_wallet")
        return
    
    for wallet in db.wallets:
        address = wallet["address"]
        name = wallet["name"]
        
        balances = {}
        for token in TOKENS.keys():
            balances[token] = get_balance(address, token)
        
        msg = f"📊 Баланс: {name}\n"
        msg += f"{format_address(address)}\n\n"
        
        for token, amount in balances.items():
            msg += f"{token:4} · {format_balance(amount)}\n"
        
        now_utc = datetime.utcnow().strftime("%H:%M UTC")
        msg += f"\n🕒 обновлено: {now_utc}"
        
        await message.answer(msg)

@dp.message(Command("add_wallet"))
async def cmd_add_wallet(message: Message):
    if not is_authorized(message.from_user.id):
        await message.answer("❌ Доступ запрещён")
        return
    
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer("Использование: /add_wallet <адрес> [название]")
        return
    
    parts = args[1].split(maxsplit=1)
    address = parts[0]
    name = parts[1] if len(parts) > 1 else f"Wallet {len(db.wallets) + 1}"
    
    try:
        address = Web3.to_checksum_address(address)
    except:
        await message.answer("❌ Невалидный адрес BSC")
        return
    
    if db.add_wallet(address, name):
        await message.answer(f"✅ Кошелёк добавлен: {name}\n{format_address(address)}")
    else:
        await message.answer("ℹ️ Этот кошелёк уже добавлен")

@dp.message(Command("wallets"))
async def cmd_wallets(message: Message):
    if not is_authorized(message.from_user.id):
        await message.answer("❌ Доступ запрещён")
        return
    
    if not db.wallets:
        await message.answer("ℹ️ Нет добавленных кошельков")
        return
    
    msg = "💼 Мои кошельки:\n\n"
    for i, wallet in enumerate(db.wallets, 1):
        msg += f"{i}. {wallet['name']}\n"
        msg += f"   {format_address(wallet['address'])}\n\n"
    
    await message.answer(msg)

async def send_transaction_alert(wallet_name, wallet_address, token_symbol, amount, direction, from_addr, to_addr, tx_hash):
    try:
        if direction == "IN":
            emoji = "🟢"
        else:
            emoji = "🔴"
        
        new_balance = get_balance(wallet_address, token_symbol)
        
        msg = f"{emoji} {direction} | {format_balance(amount)} {token_symbol}\n"
        
        if direction == "IN":
            msg += f"From: {format_address(from_addr)}\n"
        else:
            msg += f"To: {format_address(to_addr)}\n"
        
        msg += f"💰 Новый баланс: {format_balance(new_balance)} {token_symbol}\n"
        msg += f"🔗 <a href='https://bscscan.com/tx/{tx_hash}'>Tx</a>"
        
        await bot.send_message(
            chat_id=TELEGRAM_USER_ID,
            text=msg,
            parse_mode="HTML",
            disable_web_page_preview=True
        )
        
        logger.info(f"Отправлено уведомление: {direction} {amount} {token_symbol}")
        
    except Exception as e:
        logger.error(f"Ошибка отправки уведомления: {e}")

async def check_token_transfers(wallet_address, token_symbol, token_address, from_block, to_block):
    try:
        wallet_address = Web3.to_checksum_address(wallet_address)
        token_address = Web3.to_checksum_address(token_address)
        
        contract = w3.eth.contract(address=token_address, abi=ERC20_ABI)
        
        transfer_filter = contract.events.Transfer.create_filter(
            fromBlock=from_block,
            toBlock=to_block
        )
        
        events = transfer_filter.get_all_entries()
        
        for event in events:
            from_addr = event['args']['from']
            to_addr = event['args']['to']
            value = event['args']['value']
            tx_hash = event['transactionHash'].hex()
            
            if db.is_processed(tx_hash):
                continue
            
            is_incoming = to_addr.lower() == wallet_address.lower()
            is_outgoing = from_addr.lower() == wallet_address.lower()
            
            if not (is_incoming or is_outgoing):
                continue
            
            decimals = TOKENS[token_symbol]["decimals"]
            amount = value / (10 ** decimals)
            
            direction = "IN" if is_incoming else "OUT"
            
            wallet_name = "Main"
            for wallet in db.wallets:
                if wallet["address"].lower() == wallet_address.lower():
                    wallet_name = wallet["name"]
                    break
            
            await send_transaction_alert(
                wallet_name=wallet_name,
                wallet_address=wallet_address,
                token_symbol=token_symbol,
                amount=amount,
                direction=direction,
                from_addr=from_addr,
                to_addr=to_addr,
                tx_hash=tx_hash
            )
            
            db.mark_processed(tx_hash)
            
    except Exception as e:
        logger.error(f"Ошибка проверки {token_symbol} transfers: {e}")

async def monitor_blockchain():
    logger.info("🔍 Мониторинг блокчейна запущен")
    
    if db.last_block == 0:
        db.last_block = w3.eth.block_number
        db.save()
    
    while True:
        try:
            if not db.wallets:
                await asyncio.sleep(30)
                continue
            
            current_block = w3.eth.block_number
            
            if current_block <= db.last_block:
                await asyncio.sleep(30)
                continue
            
            logger.info(f"Проверка блоков {db.last_block + 1} - {current_block}")
            
            for wallet in db.wallets:
                wallet_address = wallet["address"]
                
                for token_symbol, token_info in TOKENS.items():
                    if token_symbol == "BNB":
                        continue
                    
                    token_address = token_info["address"]
                    
                    await check_token_transfers(
                        wallet_address=wallet_address,
                        token_symbol=token_symbol,
                        token_address=token_address,
                        from_block=db.last_block + 1,
                        to_block=current_block
                    )
            
            db.update_last_block(current_block)
            
            await asyncio.sleep(30)
            
        except Exception as e:
            logger.error(f"Ошибка мониторинга: {e}")
            await asyncio.sleep(60)

async def main():
    logger.info("🚀 Бот запускается...")
    
    if w3.is_connected():
        logger.info(f"✅ Подключен к BSC (блок: {w3.eth.block_number})")
    else:
        logger.error("❌ Не удалось подключиться к BSC RPC")
    
    asyncio.create_task(monitor_blockchain())
    
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
