import os
import asyncio
import logging
import warnings
import time
from datetime import datetime, timezone
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message
from web3 import Web3
import json
import aiohttp

warnings.filterwarnings('ignore', message='.*MismatchedABI.*')
warnings.filterwarnings('ignore', category=UserWarning)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
TELEGRAM_USER_ID = int(os.getenv("TELEGRAM_USER_ID"))
BNB_RPC = os.getenv("BNB_RPC", "https://bsc-dataseed.binance.org/")

# Настройки безопасности
CONFIRMATIONS = 3  # Защита от reorg
PROCESSED_TX_TTL = 86400  # 24 часа хранения

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
w3 = Web3(Web3.HTTPProvider(BNB_RPC))

try:
    from web3.middleware import ExtraDataToPOAMiddleware
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
except ImportError:
    try:
        from web3.middleware import geth_poa_middleware
        w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    except ImportError:
        pass

TOKENS = {
    "BNB": {
        "address": None,
        "decimals": 18,
        "coingecko_id": "binancecoin"
    },
    "USDT": {
        "address": "0x55d398326f99059fF775485246999027B3197955",
        "decimals": 18,
        "coingecko_id": "tether"
    },
    "USDC": {
        "address": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
        "decimals": 18,
        "coingecko_id": "usd-coin"
    },
    "BTCB": {
        "address": "0x7130d2A12B9BCbFAe4f2634d864A1Ee1Ce3Ead9c",
        "decimals": 18,
        "coingecko_id": "bitcoin"
    },
    "MEC": {
        "address": "0x9a79D9C9e521cb900D2584c74bb41997EB7BF49f",
        "decimals": 18,
        "coingecko_id": None
    }
}

# Transfer event signature
TRANSFER_EVENT_SIGNATURE = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

ERC20_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function"
    }
]

price_cache = {}
price_cache_time = 0

async def get_token_prices():
    global price_cache, price_cache_time
    
    current_time = asyncio.get_event_loop().time()
    
    if current_time - price_cache_time < 300 and price_cache:
        return price_cache
    
    try:
        coin_ids = []
        for token_info in TOKENS.values():
            if token_info.get("coingecko_id"):
                coin_ids.append(token_info["coingecko_id"])
        
        if not coin_ids:
            return {}
        
        ids_string = ",".join(coin_ids)
        url = f"https://api.coingecko.com/api/v3/simple/price?ids={ids_string}&vs_currencies=usd"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    new_cache = {}
                    for token_symbol, token_info in TOKENS.items():
                        coingecko_id = token_info.get("coingecko_id")
                        if coingecko_id and coingecko_id in data:
                            new_cache[token_symbol] = data[coingecko_id]["usd"]
                    
                    price_cache = new_cache
                    price_cache_time = current_time
                    logger.info(f"Цены обновлены")
                    return price_cache
    
    except Exception as e:
        logger.error(f"Ошибка получения цен: {e}")
    
    return price_cache

def format_usd(amount, token_symbol):
    if token_symbol in price_cache:
        usd_value = amount * price_cache[token_symbol]
        return f" (${usd_value:,.2f})"
    return ""

class SimpleDB:
    def __init__(self):
        self.wallets = []
        self.processed_txs = {}  # УЛУЧШЕНИЕ: теперь с timestamp
        self.last_block = None
        self.load()
    
    def load(self):
        try:
            if os.path.exists("data.json"):
                with open("data.json", "r") as f:
                    data = json.load(f)
                    self.wallets = data.get("wallets", [])
                    self.last_block = data.get("last_block", None)
                    
                    # Загружаем processed_txs с очисткой старых
                    processed_data = data.get("processed_txs", {})
                    current_time = time.time()
                    
                    # Фильтруем только свежие (последние 24 часа)
                    self.processed_txs = {
                        k: v for k, v in processed_data.items()
                        if isinstance(v, dict) and current_time - v.get("ts", 0) < PROCESSED_TX_TTL
                    }
                    
                    logger.info(f"Загружено {len(self.processed_txs)} обработанных tx")
        except Exception as e:
            logger.error(f"Ошибка загрузки БД: {e}")
    
    def save(self):
        try:
            with open("data.json", "w") as f:
                json.dump({
                    "wallets": self.wallets,
                    "processed_txs": self.processed_txs,
                    "last_block": self.last_block
                }, f, indent=2)
        except Exception as e:
            logger.error(f"Ошибка сохранения БД: {e}")
    
    def update_last_block(self, block_num):
        self.last_block = block_num
        if block_num % 10 == 0:
            self.save()
    
    def add_wallet(self, address, name="Main"):
        current_block = w3.eth.block_number
        wallet = {
            "address": address,
            "name": name,
            "added_at_block": current_block
        }
        
        for existing_wallet in self.wallets:
            if existing_wallet["address"].lower() == address.lower():
                return False
        
        self.wallets.append(wallet)
        self.save()
        logger.info(f"Кошелёк добавлен с блока {current_block}")
        return True
    
    def remove_wallet(self, index):
        try:
            if 0 <= index < len(self.wallets):
                removed = self.wallets.pop(index)
                self.save()
                logger.info(f"Кошелёк удалён: {removed['name']}")
                return True, removed
            return False, None
        except Exception as e:
            logger.error(f"Ошибка удаления кошелька: {e}")
            return False, None
    
    def mark_processed(self, tx_hash, wallet_address, block_num):
        """УЛУЧШЕНИЕ: сохраняем с timestamp и block"""
        key = f"{tx_hash}:{wallet_address.lower()}"
        self.processed_txs[key] = {
            "block": block_num,
            "ts": time.time()
        }
        
        # Очистка старых записей
        if len(self.processed_txs) > 10000:
            current_time = time.time()
            self.processed_txs = {
                k: v for k, v in self.processed_txs.items()
                if current_time - v["ts"] < PROCESSED_TX_TTL
            }
    
    def is_processed(self, tx_hash, wallet_address):
        key = f"{tx_hash}:{wallet_address.lower()}"
        return key in self.processed_txs

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
    return f"{address[:6]}...{address[-4:]}"

def format_balance(amount):
    if amount == 0:
        return "0.0000"
    elif amount >= 1:
        return f"{amount:,.2f}"
    elif amount >= 0.0001:
        return f"{amount:.4f}"
    else:
        return f"{amount:.8f}"

def is_authorized(user_id: int) -> bool:
    return user_id == TELEGRAM_USER_ID

@dp.message(Command("start"))
async def cmd_start(message: Message):
    if not is_authorized(message.from_user.id):
        await message.answer("Доступ запрещён")
        return
    
    await message.answer(
        "БНБ Бухгалтер запущен\n\n"
        "Команды:\n"
        "/balance — текущие балансы\n"
        "/add_wallet <адрес> — добавить кошелёк\n"
        "/wallets — список кошельков\n"
        "/remove_wallet — удалить кошелёк"
    )

@dp.message(Command("balance"))
async def cmd_balance(message: Message):
    if not is_authorized(message.from_user.id):
        await message.answer("Доступ запрещён")
        return
    
    if not db.wallets:
        await message.answer("Нет добавленных кошельков\nИспользуй /add_wallet")
        return
    
    await get_token_prices()
    
    for wallet in db.wallets:
        address = wallet["address"]
        name = wallet["name"]
        
        balances = {}
        for token in TOKENS.keys():
            balances[token] = get_balance(address, token)
        
        msg = f"Баланс: {name}\n"
        msg += f"{format_address(address)}\n\n"
        
        for token, amount in balances.items():
            usd_str = format_usd(amount, token)
            msg += f"{token}: {format_balance(amount)}{usd_str}\n"
        
        now_utc = datetime.now(timezone.utc).strftime("%H:%M UTC")
        msg += f"\nобновлено: {now_utc}"
        
        await message.answer(msg)

@dp.message(Command("add_wallet"))
async def cmd_add_wallet(message: Message):
    if not is_authorized(message.from_user.id):
        await message.answer("Доступ запрещён")
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
        await message.answer("Невалидный адрес BSC")
        return
    
    if db.add_wallet(address, name):
        await message.answer(
            f"Кошелёк добавлен: {name}\n"
            f"{format_address(address)}\n\n"
            f"Мониторинг начат с текущего блока"
        )
    else:
        await message.answer("Этот кошелёк уже добавлен")

@dp.message(Command("wallets"))
async def cmd_wallets(message: Message):
    if not is_authorized(message.from_user.id):
        await message.answer("Доступ запрещён")
        return
    
    if not db.wallets:
        await message.answer("Нет добавленных кошельков")
        return
    
    msg = "Мои кошельки:\n\n"
    for i, wallet in enumerate(db.wallets, 1):
        msg += f"{i}. {wallet['name']}\n"
        msg += f"   {format_address(wallet['address'])}\n\n"
    
    msg += "Для удаления используй:\n/remove_wallet <номер>"
    
    await message.answer(msg)

@dp.message(Command("remove_wallet"))
async def cmd_remove_wallet(message: Message):
    if not is_authorized(message.from_user.id):
        await message.answer("Доступ запрещён")
        return
    
    if not db.wallets:
        await message.answer("Нет кошельков для удаления")
        return
    
    args = message.text.split(maxsplit=1)
    
    if len(args) < 2:
        msg = "Выбери номер кошелька для удаления:\n\n"
        for i, wallet in enumerate(db.wallets, 1):
            msg += f"{i}. {wallet['name']}\n"
            msg += f"   {format_address(wallet['address'])}\n\n"
        msg += "Используй: /remove_wallet <номер>"
        await message.answer(msg)
        return
    
    try:
        wallet_num = int(args[1])
        success, removed_wallet = db.remove_wallet(wallet_num - 1)
        
        if success:
            await message.answer(
                f"✅ Кошелёк удалён:\n"
                f"{removed_wallet['name']}\n"
                f"{format_address(removed_wallet['address'])}"
            )
        else:
            await message.answer("❌ Неверный номер кошелька")
    
    except ValueError:
        await message.answer("❌ Укажи номер кошелька (число)")

async def send_transaction_alert(wallet_name, wallet_address, token_symbol, amount, direction, from_addr, to_addr, tx_hash):
    try:
        await get_token_prices()
        
        if direction == "IN":
            emoji = "🟢"
        else:
            emoji = "🔴"
        
        new_balance = get_balance(wallet_address, token_symbol)
        usd_amount = format_usd(amount, token_symbol)
        usd_balance = format_usd(new_balance, token_symbol)
        
        msg = f"{emoji} {direction} | {format_balance(amount)} {token_symbol}{usd_amount}\n"
        msg += f"Кошелёк: {wallet_name}\n"
        
        if direction == "IN":
            msg += f"From: {format_address(from_addr)}\n"
        else:
            msg += f"To: {format_address(to_addr)}\n"
        
        msg += f"Новый баланс: {format_balance(new_balance)} {token_symbol}{usd_balance}\n"
        msg += f"<a href='https://bscscan.com/tx/{tx_hash}'>Tx</a>"
        
        await bot.send_message(
            chat_id=TELEGRAM_USER_ID,
            text=msg,
            parse_mode="HTML",
            disable_web_page_preview=True
        )
        
        logger.info(f"Уведомление: {wallet_name} {direction} {amount} {token_symbol}")
        
    except Exception as e:
        logger.error(f"Ошибка отправки: {e}")

def has_relevant_logs(logs, token_addresses_set):
    """УЛУЧШЕНИЕ #1: быстрая проверка перед парсингом"""
    for log in logs:
        if log['address'].lower() in token_addresses_set:
            return True
    return False

def parse_transfer_events_from_logs(logs, wallet_addresses_dict, token_addresses_reverse):
    """Парсим Transfer события напрямую из logs за ОДИН проход"""
    transfers = []
    
    for log in logs:
        try:
            if len(log['topics']) != 3:
                continue
            
            if log['topics'][0].hex() != TRANSFER_EVENT_SIGNATURE:
                continue
            
            token_address = log['address'].lower()
            if token_address not in token_addresses_reverse:
                continue
            
            token_symbol = token_addresses_reverse[token_address]
            token_info = TOKENS[token_symbol]
            
            from_addr = '0x' + log['topics'][1].hex()[-40:]
            to_addr = '0x' + log['topics'][2].hex()[-40:]
            
            from_addr = from_addr.lower()
            to_addr = to_addr.lower()
            
            value = int(log['data'].hex(), 16)
            amount = value / (10 ** token_info["decimals"])
            
            if to_addr in wallet_addresses_dict:
                wallet_data = wallet_addresses_dict[to_addr]
                transfers.append({
                    "wallet_address": wallet_data["address"],
                    "wallet_name": wallet_data["name"],
                    "token_symbol": token_symbol,
                    "amount": amount,
                    "direction": "IN",
                    "from_addr": '0x' + log['topics'][1].hex()[-40:],
                    "to_addr": wallet_data["address"]
                })
            
            elif from_addr in wallet_addresses_dict:
                wallet_data = wallet_addresses_dict[from_addr]
                transfers.append({
                    "wallet_address": wallet_data["address"],
                    "wallet_name": wallet_data["name"],
                    "token_symbol": token_symbol,
                    "amount": amount,
                    "direction": "OUT",
                    "from_addr": wallet_data["address"],
                    "to_addr": '0x' + log['topics'][2].hex()[-40:]
                })
        except:
            continue
    
    return transfers

async def monitor_new_blocks():
    """ФИНАЛЬНАЯ ВЕРСИЯ мониторинга"""
    logger.info("Мониторинг запущен")
    
    if db.last_block:
        last_block = db.last_block
        logger.info(f"Восстановлен последний блок: {last_block}")
    else:
        last_block = w3.eth.block_number
        logger.info(f"Начальный блок: {last_block}")
    
    # Подготовка маппингов
    token_addresses_reverse = {}
    token_addresses_set = set()
    
    for token_symbol, token_info in TOKENS.items():
        if token_info["address"]:
            addr_lower = token_info["address"].lower()
            token_addresses_reverse[addr_lower] = token_symbol
            token_addresses_set.add(addr_lower)
    
    while True:
        try:
            current_block = w3.eth.block_number
            
            # УЛУЧШЕНИЕ: защита от reorg - не обрабатываем последние 3 блока
            safe_block = current_block - CONFIRMATIONS
            
            if safe_block > last_block:
                blocks_count = safe_block - last_block
                blocks_to_process = min(blocks_count, 5)
                
                if blocks_count > 5:
                    logger.info(f"Новых блоков: {blocks_count}, обрабатываем: {blocks_to_process}")
                else:
                    logger.info(f"Новых блоков: {blocks_to_process}")
                
                for block_num in range(last_block + 1, last_block + blocks_to_process + 1):
                    block = w3.eth.get_block(block_num, full_transactions=True)
                    
                    wallet_addresses_dict = {
                        w["address"].lower(): w for w in db.wallets
                    }
                    
                    if not wallet_addresses_dict:
                        db.update_last_block(block_num)
                        continue
                    
                    for tx in block.transactions:
                        tx_hash = tx.hash.hex()
                        tx_from = tx['from'].lower()
                        tx_to = tx['to'].lower() if tx['to'] else ""
                        
                        # ========== BNB ==========
                        if tx.value > 0:
                            for wallet_addr, wallet_data in wallet_addresses_dict.items():
                                if db.is_processed(tx_hash, wallet_data["address"]):
                                    continue
                                
                                if tx_to == wallet_addr:
                                    amount = w3.from_wei(tx.value, 'ether')
                                    
                                    await send_transaction_alert(
                                        wallet_name=wallet_data["name"],
                                        wallet_address=wallet_data["address"],
                                        token_symbol="BNB",
                                        amount=float(amount),
                                        direction="IN",
                                        from_addr=tx['from'],
                                        to_addr=wallet_data["address"],
                                        tx_hash=tx_hash
                                    )
                                    
                                    db.mark_processed(tx_hash, wallet_data["address"], block_num)
                                
                                elif tx_from == wallet_addr:
                                    amount = w3.from_wei(tx.value, 'ether')
                                    
                                    await send_transaction_alert(
                                        wallet_name=wallet_data["name"],
                                        wallet_address=wallet_data["address"],
                                        token_symbol="BNB",
                                        amount=float(amount),
                                        direction="OUT",
                                        from_addr=wallet_data["address"],
                                        to_addr=tx['to'],
                                        tx_hash=tx_hash
                                    )
                                    
                                    db.mark_processed(tx_hash, wallet_data["address"], block_num)
                        
                        # ========== ERC20 ==========
                        if not tx_to:
                            continue
                        
                        try:
                            tx_receipt = w3.eth.get_transaction_receipt(tx_hash)
                            
                            if tx_receipt.status == 0 or not tx_receipt.logs:
                                continue
                            
                            # УЛУЧШЕНИЕ #1: быстрая проверка перед парсингом
                            if not has_relevant_logs(tx_receipt.logs, token_addresses_set):
                                continue
                            
                            transfers = parse_transfer_events_from_logs(
                                tx_receipt.logs,
                                wallet_addresses_dict,
                                token_addresses_reverse
                            )
                            
                            for transfer in transfers:
                                if db.is_processed(tx_hash, transfer["wallet_address"]):
                                    continue
                                
                                await send_transaction_alert(
                                    wallet_name=transfer["wallet_name"],
                                    wallet_address=transfer["wallet_address"],
                                    token_symbol=transfer["token_symbol"],
                                    amount=transfer["amount"],
                                    direction=transfer["direction"],
                                    from_addr=transfer["from_addr"],
                                    to_addr=transfer["to_addr"],
                                    tx_hash=tx_hash
                                )
                                
                                db.mark_processed(tx_hash, transfer["wallet_address"], block_num)
                                
                        except Exception as e:
                            continue
                    
                    db.update_last_block(block_num)
                
                last_block = last_block + blocks_to_process
                db.save()
            
            await asyncio.sleep(10)
            
        except Exception as e:
            logger.error(f"Ошибка мониторинга: {e}")
            await asyncio.sleep(30)

async def main():
    logger.info("Бот запускается")
    
    if w3.is_connected():
        logger.info(f"BSC подключен (блок: {w3.eth.block_number})")
    else:
        logger.error("Ошибка подключения к BSC")
        return
    
    await get_token_prices()
    
    logger.info(f"Загружено кошельков: {len(db.wallets)}")
    
    asyncio.create_task(monitor_new_blocks())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
