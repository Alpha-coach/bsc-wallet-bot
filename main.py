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
    """Форматировать баланс с учётом размера суммы"""
    if amount == 0:
        return "0.0000"
    elif amount >= 1:
        return f"{amount:,.2f}"
    elif amount >= 0.0001:
        return f"{amount:.4f}"
    else:
        # Для очень мелких сумм показываем больше знаков
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
        
        logger.info(f"✅ Уведомление отправлено: {direction} {amount} {token_symbol}{usd_amount} для {wallet_name}")
        
    except Exception as e:
        logger.error(f"Ошибка отправки уведомления: {e}")

async def process_transaction(tx_hash, wallet_address, wallet_name):
    """
    АГРЕССИВНАЯ обработка транзакции - проверяет ВСЕ возможные сценарии
    """
    try:
        if db.is_processed(tx_hash):
            return
        
        logger.info(f"🔍 Обработка tx {tx_hash[:10]}... для {wallet_name}")
        
        tx = w3.eth.get_transaction(tx_hash)
        tx_receipt = w3.eth.get_transaction_receipt(tx_hash)
        
        if tx_receipt.status == 0:
            logger.info(f"❌ Транзакция неуспешна: {tx_hash[:10]}...")
            db.mark_processed(tx_hash)
            return
        
        wallet_address_lower = wallet_address.lower()
        found_relevant_transfer = False
        
        # ============ ОБРАБОТКА BNB ============
        if tx.value > 0:
            from_addr = tx['from'].lower()
            to_addr = tx['to'].lower() if tx['to'] else ""
            
            if to_addr == wallet_address_lower:
                amount = w3.from_wei(tx.value, 'ether')
                logger.info(f"💰 Входящий BNB: {amount}")
                
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
                
                found_relevant_transfer = True
            
            elif from_addr == wallet_address_lower:
                amount = w3.from_wei(tx.value, 'ether')
                logger.info(f"💸 Исходящий BNB: {amount}")
                
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
                
                found_relevant_transfer = True
        
        # ============ ОБРАБОТКА ERC20 ТОКЕНОВ ============
        # АГРЕССИВНЫЙ РЕЖИМ: проверяем ВСЕ токены независимо от tx['to']
        
        for token_symbol, token_info in TOKENS.items():
            if token_symbol == "BNB":
                continue
            
            try:
                token_address = Web3.to_checksum_address(token_info["address"])
                contract = w3.eth.contract(address=token_address, abi=ERC20_ABI)
                
                # Пытаемся получить Transfer события
                transfer_events = contract.events.Transfer().process_receipt(tx_receipt)
                
                if not transfer_events:
                    continue
                
                logger.debug(f"🔎 Найдено {len(transfer_events)} Transfer событий для {token_symbol}")
                
                # Проверяем каждое событие
                for event in transfer_events:
                    from_addr = event['args']['from'].lower()
                    to_addr = event['args']['to'].lower()
                    value = event['args']['value']
                    
                    # ВХОДЯЩИЙ токен
                    if to_addr == wallet_address_lower:
                        amount = value / (10 ** token_info["decimals"])
                        logger.info(f"💰 Входящий {token_symbol}: {amount}")
                        
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
                        
                        found_relevant_transfer = True
                    
                    # ИСХОДЯЩИЙ токен
                    elif from_addr == wallet_address_lower:
                        amount = value / (10 ** token_info["decimals"])
                        logger.info(f"💸 Исходящий {token_symbol}: {amount}")
                        
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
                        
                        found_relevant_transfer = True
                        
            except Exception as e:
                # Нормально если токен не участвует в транзакции
                logger.debug(f"Токен {token_symbol} не участвует: {e}")
                continue
        
        # Отмечаем как обработанную
        db.mark_processed(tx_hash)
        
        if found_relevant_transfer:
            logger.info(f"✅ Транзакция {tx_hash[:10]}... успешно обработана")
        else:
            logger.debug(f"⚪ Транзакция {tx_hash[:10]}... не релевантна для кошелька")
                    
    except Exception as e:
        logger.error(f"❌ Ошибка обработки {tx_hash[:10]}...: {e}", exc_info=True)
        # Отмечаем чтобы не зациклиться
        db.mark_processed(tx_hash)

async def monitor_new_blocks():
    """
    АГРЕССИВНЫЙ мониторинг - проверяет ВСЕ транзакции в блоке на наличие Transfer событий
    """
    logger.info("🚀 Мониторинг блоков запущен (АГРЕССИВНЫЙ РЕЖИМ)")
    
    last_block = w3.eth.block_number
    logger.info(f"📍 Начальный блок: {last_block}")
    
    # Создаём контракты для всех токенов заранее
    token_contracts = {}
    for token_symbol, token_info in TOKENS.items():
        if token_symbol != "BNB":
            token_address = Web3.to_checksum_address(token_info["address"])
            token_contracts[token_symbol] = w3.eth.contract(address=token_address, abi=ERC20_ABI)
    
    while True:
        try:
            current_block = w3.eth.block_number
            
            if current_block > last_block:
                blocks_to_process = current_block - last_block
                logger.info(f"🆕 Новых блоков: {blocks_to_process} (с {last_block + 1} по {current_block})")
                
                for block_num in range(last_block + 1, current_block + 1):
                    block = w3.eth.get_block(block_num, full_transactions=True)
                    tx_count = len(block.transactions)
                    
                    if tx_count > 0:
                        logger.info(f"📦 Блок {block_num}: {tx_count} транзакций")
                    
                    # Собираем список кошельков для быстрой проверки
                    wallet_addresses = {w["address"].lower(): w for w in db.wallets}
                    
                    for tx in block.transactions:
                        tx_hash = tx.hash.hex()
                        
                        # Пропускаем уже обработанные
                        if db.is_processed(tx_hash):
                            continue
                        
                        # Быстрая проверка: участвует ли хоть один наш кошелёк?
                        tx_from = tx['from'].lower()
                        tx_to = tx['to'].lower() if tx['to'] else ""
                        
                        involved_wallet = None
                        
                        # Прямое участие (для BNB)
                        if tx_from in wallet_addresses:
                            involved_wallet = wallet_addresses[tx_from]
                        elif tx_to in wallet_addresses:
                            involved_wallet = wallet_addresses[tx_to]
                        
                        # Если не нашли прямого участия, проверяем Transfer события
                        if not involved_wallet:
                            try:
                                tx_receipt = w3.eth.get_transaction_receipt(tx_hash)
                                
                                if tx_receipt.status == 0:
                                    db.mark_processed(tx_hash)
                                    continue
                                
                                # Быстрая проверка Transfer событий
                                for token_symbol, contract in token_contracts.items():
                                    try:
                                        events = contract.events.Transfer().process_receipt(tx_receipt)
                                        
                                        for event in events:
                                            event_from = event['args']['from'].lower()
                                            event_to = event['args']['to'].lower()
                                            
                                            if event_from in wallet_addresses:
                                                involved_wallet = wallet_addresses[event_from]
                                                break
                                            elif event_to in wallet_addresses:
                                                involved_wallet = wallet_addresses[event_to]
                                                break
                                        
                                        if involved_wallet:
                                            break
                                    except:
                                        continue
                            except:
                                pass
                        
                        # Если нашли причастный кошелёк - обрабатываем
                        if involved_wallet:
                            logger.info(f"🎯 Найдена транзакция для {involved_wallet['name']}: {tx_hash[:10]}...")
                            await process_transaction(
                                tx_hash=tx_hash,
                                wallet_address=involved_wallet["address"],
                                wallet_name=involved_wallet["name"]
                            )
                
                last_block = current_block
            
            await asyncio.sleep(45)
            
        except Exception as e:
            logger.error(f"❌ Ошибка мониторинга: {e}", exc_info=True)
            await asyncio.sleep(30)

async def main():
    logger.info("🤖 Бот запускается")
    
    if w3.is_connected():
        logger.info(f"✅ BSC подключен (блок: {w3.eth.block_number})")
    else:
        logger.error("❌ Ошибка подключения к BSC")
        return
    
    # Загружаем цены при старте
    await get_token_prices()
    
    logger.info(f"👛 Загружено кошельков: {len(db.wallets)}")
    for wallet in db.wallets:
        logger.info(f"  - {wallet['name']}: {format_address(wallet['address'])}")
    
    asyncio.create_task(monitor_new_blocks())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
