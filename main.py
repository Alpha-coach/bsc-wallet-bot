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
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
TELEGRAM_USER_ID = int(os.getenv("TELEGRAM_USER_ID"))
BNB_RPC = os.getenv("BNB_RPC", "https://bsc-dataseed.binance.org/")

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

ERC20_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function"
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "from", "type": "address"},
            {"indexed": True, "name": "to", "type": "address"},
            {"indexed": False, "name": "value", "type": "uint256"}
        ],
        "name": "Transfer",
        "type": "event"
    }
]

# Кэш для цен (обновляется каждые 5 минут)
price_cache = {}
price_cache_time = 0

async def get_token_prices():
    """Получить цены токенов в USD из CoinGecko"""
    global price_cache, price_cache_time
    
    current_time = asyncio.get_event_loop().time()
    
    # Если кэш свежий (меньше 5 минут) - возвращаем его
    if current_time - price_cache_time < 300 and price_cache:
        return price_cache
    
    try:
        # Собираем все coingecko_id для запроса
        coin_ids = []
        for token_info in TOKENS.values():
            if token_info.get("coingecko_id"):
                coin_ids.append(token_info["coingecko_id"])
        
        if not coin_ids:
            return {}
        
        # Запрос к CoinGecko API
        ids_string = ",".join(coin_ids)
        url = f"https://api.coingecko.com/api/v3/simple/price?ids={ids_string}&vs_currencies=usd"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    # Преобразуем в удобный формат: token_symbol -> price
                    new_cache = {}
                    for token_symbol, token_info in TOKENS.items():
                        coingecko_id = token_info.get("coingecko_id")
                        if coingecko_id and coingecko_id in data:
                            new_cache[token_symbol] = data[coingecko_id]["usd"]
                    
                    price_cache = new_cache
                    price_cache_time = current_time
                    logger.info(f"Цены обновлены: {price_cache}")
                    return price_cache
    
    except Exception as e:
        logger.error(f"Ошибка получения цен: {e}")
    
    return price_cache

def format_usd(amount, token_symbol):
    """Форматировать сумму в USD"""
    if token_symbol in price_cache:
        usd_value = amount * price_cache[token_symbol]
        return f" (${usd_value:,.2f})"
    return ""

class SimpleDB:
    def __init__(self):
        self.wallets = []
        self.processed_txs = set()
        self.load()
    
    def load(self):
        try:
            if os.path.exists("data.json"):
                with open("data.json", "r") as f:
                    data = json.load(f)
                    self.wallets = data.get("wallets", [])
                    self.processed_txs = set(data.get("processed_txs", []))
        except Exception as e:
            logger.error(f"Ошибка загрузки БД: {e}")
    
    def save(self):
        try:
            with open("data.json", "w") as f:
                json.dump({
                    "wallets": self.wallets,
                    "processed_txs": list(self.processed_txs)
                }, f, indent=2)
        except Exception as e:
            logger.error(f"Ошибка сохранения БД: {e}")
    
    def add_wallet(self, address, name="Main"):
        current_block = w3.eth.block_number
        wallet = {
            "address": address,
            "name": name,
            "last_block": current_block
        }
        
        for existing_wallet in self.wallets:
            if existing_wallet["address"].lower() == address.lower():
                return False
        
        self.wallets.append(wallet)
        self.save()
        logger.info(f"Кошелёк добавлен с блока {current_block}")
        return True
    
    def remove_wallet(self, index):
        """Удалить кошелёк по индексу"""
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
    
    def mark_processed(self, tx_hash):
        self.processed_txs.add(tx_hash)
        if len(self.processed_txs) > 10000:
            self.processed_txs = set(list(self.processed_txs)[-5000:])
        self.save()
    
    def is_processed(self, tx_hash):
        return tx_hash in self.processed_txs

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
    if amount >= 1:
        return f"{amount:,.2f}"
    else:
        return f"{amount:.4f}"

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
    
    # Обновляем цены перед показом балансов
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
    
    # Если номер не указан - показываем список
    if len(args) < 2:
        msg = "Выбери номер кошелька для удаления:\n\n"
        for i, wallet in enumerate(db.wallets, 1):
            msg += f"{i}. {wallet['name']}\n"
            msg += f"   {format_address(wallet['address'])}\n\n"
        msg += "Используй: /remove_wallet <номер>"
        await message.answer(msg)
        return
    
    # Пытаемся удалить по номеру
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
        # Обновляем цены перед отправкой уведомления
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
        
        logger.info(f"Уведомление: {direction} {amount} {token_symbol}{usd_amount}")
        
    except Exception as e:
        logger.error(f"Ошибка отправки: {e}")

async def process_transaction(tx_hash, wallet_address, wallet_name):
    try:
        if db.is_processed(tx_hash):
            logger.debug(f"Транзакция уже обработана: {tx_hash}")
            return
        
        logger.info(f"Обработка транзакции {tx_hash} для кошелька {wallet_name}")
        
        tx = w3.eth.get_transaction(tx_hash)
        tx_receipt = w3.eth.get_transaction_receipt(tx_hash)
        
        if tx_receipt.status == 0:
            logger.info(f"Транзакция {tx_hash} неуспешна (status=0)")
            db.mark_processed(tx_hash)
            return
        
        wallet_address_lower = wallet_address.lower()
        
        # Обработка нативных BNB транзакций
        if tx.value > 0:
            from_addr = tx['from'].lower()
            to_addr = tx['to'].lower() if tx['to'] else ""
            
            if to_addr == wallet_address_lower:
                amount = w3.from_wei(tx.value, 'ether')
                logger.info(f"Найдена входящая BNB транзакция: {amount} BNB")
                
                await send_transaction_alert(
                    wallet_name=wallet_name,
                    wallet_address=wallet_address,
                    token_symbol="BNB",
                    amount=float(amount),
                    direction="IN",
                    from_addr=tx['from'],
                    to_addr=wallet_address,
                    tx_hash=tx_hash
                )
                
                db.mark_processed(tx_hash)
                return
            
            elif from_addr == wallet_address_lower:
                amount = w3.from_wei(tx.value, 'ether')
                logger.info(f"Найдена исходящая BNB транзакция: {amount} BNB")
                
                await send_transaction_alert(
                    wallet_name=wallet_name,
                    wallet_address=wallet_address,
                    token_symbol="BNB",
                    amount=float(amount),
                    direction="OUT",
                    from_addr=wallet_address,
                    to_addr=tx['to'],
                    tx_hash=tx_hash
                )
                
                db.mark_processed(tx_hash)
                return
        
        # Обработка ERC20 токенов
        # КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: проверяем каждый токен отдельно
        for token_symbol, token_info in TOKENS.items():
            if token_symbol == "BNB":
                continue
            
            token_address = Web3.to_checksum_address(token_info["address"])
            
            # ПРОВЕРЯЕМ: связана ли транзакция с этим конкретным токеном
            if tx['to'] and tx['to'].lower() != token_address.lower():
                continue  # Эта транзакция не к этому токену
            
            contract = w3.eth.contract(address=token_address, abi=ERC20_ABI)
            
            try:
                transfer_events = contract.events.Transfer().process_receipt(tx_receipt)
            except Exception as e:
                logger.debug(f"Нет Transfer событий для {token_symbol}: {e}")
                continue
            
            for event in transfer_events:
                from_addr = event['args']['from'].lower()
                to_addr = event['args']['to'].lower()
                value = event['args']['value']
                
                if to_addr == wallet_address_lower:
                    amount = value / (10 ** token_info["decimals"])
                    logger.info(f"Найдена входящая {token_symbol} транзакция: {amount} {token_symbol}")
                    
                    await send_transaction_alert(
                        wallet_name=wallet_name,
                        wallet_address=wallet_address,
                        token_symbol=token_symbol,
                        amount=amount,
                        direction="IN",
                        from_addr=event['args']['from'],
                        to_addr=wallet_address,
                        tx_hash=tx_hash
                    )
                    
                    db.mark_processed(tx_hash)
                    return  # ВАЖНО: выходим после первого найденного события
                
                elif from_addr == wallet_address_lower:
                    amount = value / (10 ** token_info["decimals"])
                    logger.info(f"Найдена исходящая {token_symbol} транзакция: {amount} {token_symbol}")
                    
                    await send_transaction_alert(
                        wallet_name=wallet_name,
                        wallet_address=wallet_address,
                        token_symbol=token_symbol,
                        amount=amount,
                        direction="OUT",
                        from_addr=wallet_address,
                        to_addr=event['args']['to'],
                        tx_hash=tx_hash
                    )
                    
                    db.mark_processed(tx_hash)
                    return  # ВАЖНО: выходим после первого найденного события
        
        # Если дошли сюда - транзакция не связана с нашими токенами
        logger.debug(f"Транзакция {tx_hash} не содержит релевантных Transfer событий")
        db.mark_processed(tx_hash)
                    
    except Exception as e:
        logger.error(f"Ошибка обработки tx {tx_hash}: {e}", exc_info=True)

async def monitor_new_blocks():
    logger.info("Мониторинг блоков запущен")
    
    last_block = w3.eth.block_number
    logger.info(f"Начальный блок: {last_block}")
    
    while True:
        try:
            current_block = w3.eth.block_number
            
            if current_block > last_block:
                blocks_to_process = current_block - last_block
                logger.info(f"Новых блоков: {blocks_to_process} (с {last_block + 1} по {current_block})")
                
                for block_num in range(last_block + 1, current_block + 1):
                    block = w3.eth.get_block(block_num, full_transactions=True)
                    logger.debug(f"Обработка блока {block_num}, транзакций: {len(block.transactions)}")
                    
                    for tx in block.transactions:
                        tx_hash = tx.hash.hex()
                        
                        for wallet in db.wallets:
                            wallet_address = wallet["address"].lower()
                            
                            # Проверяем участвует ли наш кошелёк в транзакции
                            if tx['from'].lower() == wallet_address or (tx['to'] and tx['to'].lower() == wallet_address):
                                logger.info(f"Найдена транзакция для кошелька {wallet['name']}: {tx_hash}")
                                await process_transaction(
                                    tx_hash=tx_hash,
                                    wallet_address=wallet["address"],
                                    wallet_name=wallet["name"]
                                )
                                break
                
                last_block = current_block
            
            await asyncio.sleep(45)
            
        except Exception as e:
            logger.error(f"Ошибка мониторинга: {e}", exc_info=True)
            await asyncio.sleep(30)

async def main():
    logger.info("Бот запускается")
    
    if w3.is_connected():
        logger.info(f"BSC подключен (блок: {w3.eth.block_number})")
    else:
        logger.error("Ошибка подключения к BSC")
        return
    
    # Загружаем цены при старте
    await get_token_prices()
    
    asyncio.create_task(monitor_new_blocks())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
