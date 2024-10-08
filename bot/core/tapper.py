import asyncio
from datetime import datetime
from random import randint, choices
from time import time
from urllib.parse import unquote, quote

import aiohttp
from aiohttp_proxy import ProxyConnector
from better_proxy import Proxy
from pyrogram import Client
from pyrogram.errors import Unauthorized, UserDeactivated, AuthKeyUnregistered, FloodWait
from pyrogram.raw.functions.messages import RequestAppWebView
from pyrogram.raw.types import InputBotAppShortName

from typing import Callable
import functools
from tzlocal import get_localzone
from bot.config import settings
from bot.exceptions import InvalidSession
from bot.utils import logger
from .agents import generate_random_user_agent
from .headers import headers
from .profiles import profiles

def error_handler(func: Callable):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            await asyncio.sleep(1)
    return wrapper

def convert_to_local_and_unix(iso_time):
    dt = datetime.fromisoformat(iso_time.replace('Z', '+00:00'))
    local_dt = dt.astimezone(get_localzone())
    unix_time = int(local_dt.timestamp())
    return unix_time

class Tapper:
    def __init__(self, tg_client: Client, proxy: str | None):
        self.session_name = tg_client.name
        self.tg_client = tg_client
        self.proxy = proxy

    async def get_tg_web_data(self) -> str:
        import json
        if self.proxy:
            proxy = Proxy.from_str(self.proxy)
            proxy_dict = dict(
                scheme=proxy.protocol,
                hostname=proxy.host,
                port=proxy.port,
                username=proxy.login,
                password=proxy.password
            )
        else:
            proxy_dict = None

        self.tg_client.proxy = proxy_dict

        try:
            if not self.tg_client.is_connected:
                try:
                    await self.tg_client.connect()

                except (Unauthorized, UserDeactivated, AuthKeyUnregistered):
                    raise InvalidSession(self.session_name)
            
            while True:
                try:
                    peer = await self.tg_client.resolve_peer('BlumCryptoBot')
                    break
                except FloodWait as fl:
                    fls = fl.value

                    logger.warning(f"{self.session_name} | FloodWait {fl}")
                    logger.info(f"{self.session_name} | Sleep {fls}s")
                    await asyncio.sleep(fls + 3)
            
            ref_id = choices([settings.REF_ID, "ref_lLkNB0I26E"], weights=[85, 15], k=1)[0]
            web_view = await self.tg_client.invoke(RequestAppWebView(
                peer=peer,
                app=InputBotAppShortName(bot_id=peer, short_name="app"),
                platform='android',
                write_allowed=True,
                start_param=ref_id
            ))

            auth_url = web_view.url
            tg_web_data = unquote(
                string=unquote(string=auth_url.split('tgWebAppData=')[1].split('&tgWebAppVersion')[0]))
            tg_web_data_parts = tg_web_data.split('&')
            user = json.loads(tg_web_data_parts[0].split('=')[1])
            init_data = (f"user={user["id"]}")
            
            if self.tg_client.is_connected:
                await self.tg_client.disconnect()

            return ref_id, init_data

        except Exception as error:
            logger.error(f"{self.session_name} | Unknown error: {error}")
            await asyncio.sleep(delay=3)
            return None, None

    @error_handler
    async def make_request(self, http_client, method, endpoint=None, domain_name=None, url=None, **kwargs):
        full_url = url or f"https://{domain_name}-domain.blum.codes/api/v1{endpoint or ''}"
        response = await http_client.request(method, full_url, **kwargs)
        return await response.json()
        
    @error_handler
    async def login(self, http_client, user_data: str, ref_id: str):
        response = await http_client.request("POST", "https://user-domain.blum.codes/api/v1/auth/provider/PROVIDER_TELEGRAM_MINI_APP", json={"query": profiles[self.session_name]["query"]})
        response_data = await response.json()
        if 'token' in response_data:
            return response_data['token']['refresh']
        return None

    @error_handler
    async def check_proxy(self, http_client: aiohttp.ClientSession) -> None:
        response = await self.make_request(http_client, 'GET', url='https://httpbin.org/ip', timeout=aiohttp.ClientTimeout(5))
        ip = response.get('origin')
        logger.info(f"{self.session_name} | Proxy IP: {ip}")

    @error_handler
    async def get_balance(self, http_client):
        return await self.make_request(http_client, "GET", "/user/balance", "game")
    
    @error_handler
    async def check_daily_reward(self, http_client):
        return await self.make_request(http_client, "GET", "/daily-reward?offset=-420", "game")

    @error_handler
    async def claim_farming(self, http_client):
        return await self.make_request(http_client, "POST", "/farming/claim", "game")

    @error_handler
    async def start_farming(self, http_client):
        return await self.make_request(http_client, "POST", "/farming/start", "game")

    @error_handler
    async def check_balance_friend(self, http_client):
        return await self.make_request(http_client, "GET", "/friends/balance", "user")

    @error_handler
    async def claim_balance_friend(self, http_client):
        return await self.make_request(http_client, "POST", "/friends/claim", "user")

    @error_handler
    async def play_game(self, http_client):
        return await self.make_request(http_client, "POST", "/game/play", "game")
    
    @error_handler
    async def claim_game(self, http_client, game_id: str, points: int):
        return await self.make_request(http_client, "POST", "/game/claim", "game", json={"gameId": game_id, "points": points})

    @error_handler
    async def claim_daily(self, http_client):
        return await self.make_request(http_client, "GET", "/daily-reward?offset=-420", "game")

    @error_handler
    async def get_new_token(self, http_client, old_refresh_token: str):
        return await self.make_request(http_client, "POST", "/auth/refresh", "user", json={'refresh': old_refresh_token})

    # checking
    @error_handler
    async def get_tasks(self, http_client):
        return await self.make_request(http_client, "POST", "/tasks/list", json={'language_code': 'en'})

    # checking
    @error_handler
    async def start_task(self, http_client, data):
        return await self.make_request(http_client, "POST", "/tasks/start", json=data)

    # checking
    @error_handler
    async def check_task(self, http_client, data):
        return await self.make_request(http_client, "POST", "/tasks/check", json=data)

    # checking
    @error_handler
    async def claim_task(self, http_client, data):
        return await self.make_request(http_client, "POST", "/tasks/claim", json=data)

    async def run(self) -> None:        
        if settings.USE_RANDOM_DELAY_IN_RUN:
            random_delay = randint(settings.RANDOM_DELAY_IN_RUN[0], settings.RANDOM_DELAY_IN_RUN[1])
            logger.info(f"{self.tg_client.name} | Bot will start in <light-red>{random_delay}s</light-red>")
            await asyncio.sleep(delay=random_delay)
        
        proxy_conn = ProxyConnector().from_url(self.proxy) if self.proxy else None
        http_client = aiohttp.ClientSession(headers=headers, connector=proxy_conn)
        if self.proxy:
            await self.check_proxy(http_client=http_client)
        
        if settings.FAKE_USERAGENT:            
            http_client.headers['User-Agent'] = generate_random_user_agent(device_type='android', browser_type='chrome')

        # ``
        # Blum Farming Bot
        # ``
        end_farming_dt = 0
        token_expiration = 0
        tickets = 0
        next_stars_check = 0
        next_combo_check = 0
        
        while True:
            try:
                # set up proxy and client session
                logger.info(f"{self.session_name} | Set up proxy and Client Session!")
                if http_client.closed:
                    if proxy_conn:
                        if not proxy_conn.closed:
                            proxy_conn.close()

                    proxy_conn = ProxyConnector().from_url(self.proxy) if self.proxy else None
                    http_client = aiohttp.ClientSession(headers=headers, connector=proxy_conn)
                    if settings.FAKE_USERAGENT:            
                        http_client.headers['User-Agent'] = generate_random_user_agent(device_type='android', browser_type='chrome')

                # get token and refresh after expired
                logger.info(f"{self.session_name} | Get token and refresh after expired!")
                timenow = time()
                if timenow >= token_expiration:
                    if (token_expiration != 0):
                        logger.warning(f"{self.session_name} | Token expired, refreshing...")
                    ref_id = profiles[self.session_name]["ref_id"]
                    user_data = profiles[self.session_name]["query"]
                    access_token = await self.login(http_client=http_client, user_data=user_data, ref_id=ref_id)

                    if not access_token:
                        logger.error(f"{self.session_name} | Failed login")
                        logger.info(f"{self.session_name} | Sleep <light-red>300s</light-red>")
                        await asyncio.sleep(delay=300)
                        continue
                    else:
                        logger.success(f"{self.session_name} | <light-red>🍅 Login successful</light-red>")
                        http_client.headers["Authorization"] = f"Bearer {access_token}"
                        token_expiration = timenow + 3600

                # Start farming
                logger.info(f"{self.session_name} | Get go!")
                await asyncio.sleep(delay=1)

                ## get user balance
                balance = await self.get_balance(http_client=http_client)
                available_balance = balance['availableBalance']
                farming_info = balance.get('farming')
                logger.info(f"{self.session_name} | Current balance: <light-red>{available_balance}</light-red>")

                if 'farming' in balance:
                    end_time_ms = farming_info['endTime']
                    end_time_s = end_time_ms / 1000.0
                    end_utc_date_time = datetime.utcfromtimestamp(end_time_s)
                    current_utc_time = datetime.utcnow()
                    time_difference = end_utc_date_time - current_utc_time
                    hours_remaining = int(time_difference.total_seconds() // 3600)
                    minutes_remaining = int((time_difference.total_seconds() % 3600) // 60)
                    logger.info(f"{self.session_name} | Thời gian nhận faming: <light-red>{hours_remaining} giờ {minutes_remaining} phút</light-red> - Số token: <light-red>{farming_info['balance']}</light-red>")
                else:
                    logger.warning(f"{self.session_name} | Thông tin về farming không có sẵn")
                    hours_remaining = 0
                    minutes_remaining = 0
                
                ## daily reward
                logger.info(f"{self.session_name} | Đang kiểm tra phần thưởng hàng ngày...")
                daily_reward_response = await self.check_daily_reward(http_client=http_client)
                if daily_reward_response is None:
                    logger.error(f"{self.session_name} | Không thể kiểm tra phần thưởng hàng ngày, thử lại ...")
                else:
                    if 'message' in daily_reward_response and daily_reward_response['message'] == 'same day':
                        logger.warning(f"{self.session_name} | Phần thưởng hàng ngày đã được nhận hôm nay")
                    elif 'message' in daily_reward_response and daily_reward_response['message'] == 'OK':
                        logger.success(f"{self.session_name} | Phần thưởng hàng ngày đã được nhận thành công!")
                    else:
                        logger.info(f"{self.session_name} | Không có phần thưởng nào để nhận!")
                
                ## claim farming balance
                if hours_remaining <= 0:
                    logger.info(f"{self.session_name} | Đang nhận số dư...")
                    claim_response = await self.claim_farming(http_client=http_client)
                    if claim_response:
                        logger.success(f"{self.session_name} | Đã nhận: <light-red>{claim_response['availableBalance']}</light-red>")
                        logger.info(f"{self.session_name} | Bắt đầu farming ...")
                        start_response = await self.start_farming(http_client=http_client)
                        if start_response:
                            logger.success(f"{self.session_name} | Farming đã bắt đầu.")
                        else:
                            logger.error(f"{self.session_name} | Không thể bắt đầu farming!")
                    else:
                        logger.error(f"{self.session_name} | Không thể nhận số dư")
                
                ## check and claim friend balance
                logger.info(f"{self.session_name} | Đang kiểm tra số dư bạn bè...")
                friend_balance = await self.check_balance_friend(http_client=http_client)
                if friend_balance:
                    if friend_balance['canClaim']:
                        logger.info(f"{self.session_name} | Số dư bạn bè: <light-red>{friend_balance['amountForClaim']}</light-red>")
                        logger.info(f"{self.session_name} | Đang nhận số dư bạn bè ...")
                        claim_friend_balance = await self.claim_balance_friend(http_client=http_client)
                        if 'claimBalance' in claim_friend_balance:
                            claimed_amount = claim_friend_balance['claimBalance']
                            logger.success(f"{self.session_name} | Nhận thành công: <light-red>{claimed_amount}</light-red>")
                        else:
                            logger.warning(f"{self.session_name} | Không thể nhận số dư bạn bè")
                    else:
                        can_claim_at = friend_balance.get('canClaimAt')
                        if can_claim_at:
                            claim_time = datetime.utcfromtimestamp(int(can_claim_at) / 1000)
                            current_time = datetime.utcnow()
                            time_diff = claim_time - current_time
                            hours, remainder = divmod(int(time_diff.total_seconds()), 3600)
                            minutes, seconds = divmod(remainder, 60)
                            logger.info(f"{self.session_name} | Số dư bạn bè: Có thể nhận sau <light-red>{hours} giờ {minutes} phút</light-red>")
                        else:
                            logger.warning(f"{self.session_name} | Tài khoản không có bạn bè")
                else:
                    logger.error(f"{self.session_name} | Không thể kiểm tra số dư bạn bè")
                
                ## play game and claim
                while balance['playPasses'] > 0:
                    logger.info(f"{self.session_name} | Khởi tạo game ...")
                    game_response = await self.play_game(http_client=http_client)
                    logger.info(f"{self.session_name} | Đang kiểm tra game ...")
                    await asyncio.sleep(delay=1)
                    if 'gameId' in game_response:
                        claim_response = await self.claim_game(http_client=http_client, game_id=game_response['gameId'], points=2000)

                        if claim_response is None:
                            logger.warning(f"{self.session_name} | Không thể nhận phần thưởng game, thử lại ...")

                        while True:
                            await asyncio.sleep(delay=5)
                            claim_response = await self.claim_game(http_client=http_client,game_id=game_response['gameId'], points=2000)
                            if claim_response is None:
                                logger.warning(f"{self.session_name} | Không thể nhận phần thưởng game, thử lại...")
                            elif 'message' in claim_response and claim_response['message'] == 'game session not finished':
                                logger.info(f"{self.session_name} | Game chưa kết thúc.. chơi tiếp")
                            elif 'message' in claim_response and claim_response['message'] == 'game session not found':
                                logger.info(f"{self.session_name} | Game đã kết thúc")
                                break
                            elif 'message' in claim_response and claim_response['message'] == 'Token is invalid':
                                logger.warning(f"{self.session_name} | Token không hợp lệ, lấy token mới...")
                                new_token = await self.get_new_token(http_client=http_client, old_refresh_token=access_token)
                                timenow = time()

                                if not new_token:
                                    logger.error(f"{self.session_name} | Failed refresh token")
                                    logger.info(f"{self.session_name} | Sleep <light-red>300s</light-red>")
                                else:
                                    logger.success(f"{self.session_name} | <light-red>🍅 Login successful</light-red>")
                                    http_client.headers["Authorization"] = f"Bearer {new_token}"
                                    token_expiration = timenow + 3600
                                continue
                            else:
                                logger.info(f"{self.session_name} | Game kết thúc")
                                break
                        
                        balance_new = await self.get_balance(http_client=http_client)
                        if balance_new['playPasses'] > 0:
                            logger.info(f"{self.session_name} | Vé vẫn còn, chơi game tiếp...")
                            continue 
                        else:
                            logger.info(f"{self.session_name} | Không còn vé.")
                            break
                    else:
                        logger.info(f"{self.session_name} | Game không thể khởi tạo! Bắt đầu khởi tạo game mới!")
                        continue

                balance_newest = await self.get_balance(http_client=http_client)
                remain_farm_time = 10
                if 'farming' in balance_newest:
                    remain_farm_time = round((balance_newest["farming"]["endTime"] - balance_newest["timestamp"])/1000.0)

                logger.success(f'{self.session_name} | Sleep <light-red>{round(remain_farm_time/60)}m.</light-red>')
                await asyncio.sleep(remain_farm_time)
                await http_client.close()
                if proxy_conn:
                    if not proxy_conn.closed:
                        proxy_conn.close()
            except InvalidSession as error:
                raise error

            except Exception as error:
                logger.error(f"{self.session_name} | Unknown error: {error}")
                await asyncio.sleep(delay=3)
                logger.info(f'{self.session_name} | Sleep <light-red>3m.</light-red>')
                await asyncio.sleep(180)

async def run_tapper(tg_client: Client):
    proxy = None
    if settings.USE_PROXY_FROM_FILE:
        proxy_data = profiles[tg_client.name]['proxy'].strip()
        if proxy_data:
            proxy = Proxy.from_str(proxy=proxy_data).as_url
            logger.info(f"{tg_client.name} | Run bot with this proxy: {proxy}")
        else:
            logger.warning(f"{tg_client.name} | The proxy is empty!")
    else:
        proxy = None

    try:
        await Tapper(tg_client=tg_client, proxy=proxy).run()
    except InvalidSession:
        logger.error(f"{tg_client.name} | Invalid Session")
