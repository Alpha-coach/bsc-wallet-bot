import os
import asyncio
import logging
from datetime import datetime, timezone
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message
from web3 import Web3
import json
import aiohttp

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
TELEGRAM_USER_ID = int(os.getenv("TELEGRAM_USER_ID"))
BNB_RPC = os.getenv("BNB_RPC", "https://bsc-dataseed.binance.org/")
BSCSCAN_API_KEY = os.getenv("BSCSCAN_API_KEY", "YourApiKeyToken")  # Бесплатный без регистрации

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
        self.balances = {}  # {wallet_address: {token: balance}}
        self.load()
    
    def load(self):
        try:
            if os.path.exists("data.json"):
                with open("data.json", "r") as f:
                    data = json.load(f)
                    self.wallets = data.get("wallets", [])
                    self.balances = data.get("balances", {})
        except Exception as e:
            logger.error(f"Ошибка загрузки БД: {e}")
    
    def save(self):
        try:
            with open("data.json", "w") as f:
                json.dump({
                    "wallets": self.wallets,
                    "balances": self.balances
                }, f, indent=2)
        except Exception as e:
            logger.error(f"Ошибка сохранения БД: {e}")
    
    def add_wallet(self, address, name="Main"):
        wallet = {
            "address": address,
            "name": name
        }
        
        for existing_wallet in self.wallets:
            if existing_wallet["address"].lower() == address.lower():
                return False
        
        self.wallets.append(wallet)
        self.save()
        logger.info(f"Кошелёк добавлен: {name}")
        return True
    
    def remove_wallet(self, index):
        try:
            if 0 <= index < len(self.wallets):
                removed = self.wallets.pop(index)
                addr_lower = removed["address"].lower()
                if addr_lower in self.balances:
                    del self.balances[addr_lower]
                self.save()
                logger.info(f"Кошелёк удалён: {removed['name']}")
                return True, removed
            return False, None
        except Exception as e:
            logger.error(f"Ошибка удаления кошелька: {e}")
            return False, None
    
    def get_balance(self, wallet_address, token_symbol):
        addr_lower = wallet_address.lower()
        if addr_lower not in self.balances:
            return None
        return self.balances[addr_lower].get(token_symbol)
    
    def set_balance(self, wallet_address, token_symbol, balance):
        addr_lower = wallet_address.lower()
        if addr_lower not in self.balances:
            self.balances[addr_lower] = {}
        self.balances[addr_lower][token_symbol] = balance
        self.save()

db = SimpleDB()

def get_balance_sync(address, token_symbol):
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

async def get_balance(address, token_symbol):
    return await asyncio.to_thread(get_balance_sync, address, token_symbol)

async def get_recent_transactions_bscscan(wallet_address, token_symbol):
    """Получаем последние транзакции через BSCScan API"""
    try:
        wallet_address = wallet_address.lower()
        
        if token_symbol == "BNB":
            # BNB транзакции
            url = f"https://api.bscscan.com/api?module=account&action=txlist&address={wallet_address}&startblock=0&endblock=99999999&page=1&offset=10&sort=desc&apikey={BSCSCAN_API_KEY}"
        else:
            # ERC-20 транзакции
            token_address = TOKENS[token_symbol]["address"]
            if not token_address:
                return []
            url = f"https://api.bscscan.com/api?module=account&action=tokentx&contractaddress={token_address}&address={wallet_address}&page=1&offset=10&sort=desc&apikey={BSCSCAN_API_KEY}"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    if data["status"] == "1" and data["message"] == "OK":
                        return data["result"][:5]  # Последние 5 транзакций
        
        return []
    
    except Exception as e:
        logger.error(f"Ошибка получения транзакций из BSCScan: {e}")
        return []

async def find_matching_transaction(wallet_address, token_symbol, expected_amount, direction):
    """Ищем транзакцию которая соответствует изменению баланса"""
    try:
        transactions = await get_recent_transactions_bscscan(wallet_address, token_symbol)
        
        if not transactions:
            return None
        
        wallet_lower = wallet_address.lower()
        
        for tx in transactions:
            if token_symbol == "BNB":
                # BNB транзакция
                tx_from = tx["from"].lower()
                tx_to = tx["to"].lower()
                amount = float(w3.from_wei(int(tx["value"]), 'ether'))
                
                # Проверяем направление и сумму
                if direction == "IN" and tx_to == wallet_lower:
                    if abs(amount - expected_amount) < 0.0001:
                        return {
                            "from": tx["from"],
                            "to": tx["to"],
                            "hash": tx["hash"],
                            "amount": amount
                        }
                elif direction == "OUT" and tx_from == wallet_lower:
                    if abs(amount - expected_amount) < 0.0001:
                        return {
                            "from": tx["from"],
                            "to": tx["to"],
                            "hash": tx["hash"],
                            "amount": amount
                        }
            else:
                # ERC-20 транзакция
                tx_from = tx["from"].lower()
                tx_to = tx["to"].lower()
                decimals = TOKENS[token_symbol]["decimals"]
                amount = int(tx["value"]) / (10 ** decimals)
                
                # Проверяем направление и сумму
                if direction == "IN" and tx_to == wallet_lower:
                    if abs(amount - expected_amount) < 0.001:  # Больше погрешность для токенов
                        return {
                            "from": tx["from"],
                            "to": tx["to"],
                            "hash": tx["hash"],
                            "amount": amount
                        }
                elif direction == "OUT" and tx_from == wallet_lower:
                    if abs(amount - expected_amount) < 0.001:
                        return {
                            "from": tx["from"],
                            "to": tx["to"],
                            "hash": tx["hash"],
                            "amount": amount
                        }
        
        return None
    
    except Exception as e:
        logger.error(f"Ошибка поиска транзакции: {e}")
        return None

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
        "🔍 Проверка балансов каждые 30 секунд\n"
        "💬 Алерты при любом изменении\n\n"
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
            balances[token] = await get_balance(address, token)
        
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
            f"✅ Кошелёк добавлен: {name}\n"
            f"{format_address(address)}\n\n"
            f"Мониторинг начнётся через 30 секунд"
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

async def check_balances():
    """Мониторинг балансов: проверяем каждые 30 секунд"""
    logger.info("⏰ Мониторинг балансов запущен (проверка каждые 30 сек)")
    
    while True:
        try:
            if not db.wallets:
                await asyncio.sleep(30)
                continue
            
            await get_token_prices()
            
            for wallet in db.wallets:
                address = wallet["address"]
                name = wallet["name"]
                
                logger.info(f"🔍 Проверяю балансы для {name}")
                
                for token_symbol in TOKENS.keys():
                    current_balance = await get_balance(address, token_symbol)
                    old_balance = db.get_balance(address, token_symbol)
                    
                    if old_balance is None:
                        # Первая проверка - просто сохраняем
                        db.set_balance(address, token_symbol, current_balance)
                        logger.info(f"📝 Начальный баланс {token_symbol}: {current_balance}")
                        continue
                    
                    # Проверяем изменение
                    diff = current_balance - old_balance
                    
                    if abs(diff) > 0.0001:  # Изменение больше 0.0001
                        logger.info(f"💰 ИЗМЕНЕНИЕ! {name} {token_symbol} diff={diff}")
                        
                        direction = "IN" if diff > 0 else "OUT"
                        amount = abs(diff)
                        
                        # Пытаемся найти детали транзакции
                        tx_details = await find_matching_transaction(address, token_symbol, amount, direction)
                        
                        emoji = "🟢" if direction == "IN" else "🔴"
                        usd_str = format_usd(amount, token_symbol)
                        usd_balance = format_usd(current_balance, token_symbol)
                        
                        msg = f"{emoji} {direction} | {format_balance(amount)} {token_symbol}{usd_str}\n"
                        msg += f"Кошелёк: {name}\n"
                        
                        if tx_details:
                            # Нашли транзакцию - показываем детали
                            if direction == "IN":
                                msg += f"From: {format_address(tx_details['from'])}\n"
                            else:
                                msg += f"To: {format_address(tx_details['to'])}\n"
                            
                            msg += f"Новый баланс: {format_balance(current_balance)} {token_symbol}{usd_balance}\n"
                            msg += f"<a href='https://bscscan.com/tx/{tx_details['hash']}'>Tx</a>"
                            
                            parse_mode = "HTML"
                            disable_preview = True
                            logger.info(f"✅ Найдена транзакция: {tx_details['hash'][:10]}...")
                        else:
                            # Не нашли - простой алерт
                            msg += f"Новый баланс: {format_balance(current_balance)} {token_symbol}{usd_balance}\n"
                            now_utc = datetime.now(timezone.utc).strftime("%H:%M UTC")
                            msg += f"\n🕐 {now_utc}"
                            
                            parse_mode = None
                            disable_preview = False
                        
                        try:
                            await bot.send_message(
                                chat_id=TELEGRAM_USER_ID,
                                text=msg,
                                parse_mode=parse_mode,
                                disable_web_page_preview=disable_preview
                            )
                            logger.info(f"✅ Алерт отправлен!")
                        except Exception as e:
                            logger.error(f"❌ Ошибка отправки алерта: {e}")
                        
                        # Обновляем баланс
                        db.set_balance(address, token_symbol, current_balance)
            
            await asyncio.sleep(30)  # Проверка каждые 30 секунд
            
        except Exception as e:
            logger.error(f"❌ Ошибка мониторинга: {e}")
            await asyncio.sleep(30)

async def main():
    logger.info("🚀 Бот запускается")
    
    is_connected = w3.is_connected()
    if is_connected:
        block_num = w3.eth.block_number
        logger.info(f"✅ BSC подключен (блок: {block_num})")
    else:
        logger.error("❌ Ошибка подключения к BSC")
        return
    
    await get_token_prices()
    
    logger.info(f"📊 Загружено кошельков: {len(db.wallets)}")
    
    # Запускаем мониторинг и бота параллельно
    asyncio.create_task(check_balances())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
