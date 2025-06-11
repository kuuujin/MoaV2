import discord
from discord.ui import View, Button
from discord.ext import commands
from google.cloud import storage
import json
from io import BytesIO
import asyncio
import datetime
import pytz
from Levenshtein import distance
import re

# 봇 토큰을 여기에 입력하세요
TOKEN = ''

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix='/', intents=intents)

scanning_users = {}
SCAN_INTERVAL = 20 * 60
BUCKET_NAME = 'moastorage'
BLOB_NAME = 'data/hotdeal.json'

LEVENSHTEIN_THRESHOLD = 4 
JACCARD_THRESHOLD = 0.4   
SIMILAR_DEAL_LOOKBACK_MONTHS = 6
MAX_SIMILAR_DEALS = 3

KST = pytz.timezone('Asia/Seoul')

# --- 가격 추출 함수 (다시 추가) ---
def extract_numeric_price(text: str) -> float | None:
    """
    텍스트에서 최종적인 단일 숫자 가격을 추출합니다.
    괄호 안의 계산식은 무시하고, 괄호 밖의 최종 가격을 우선적으로 찾습니다.
    """
    if not text:
        return None

    # 1. '숫자원' 또는 '숫자 ₩' 형식 (괄호 밖에 있는 명확한 가격)
    price_match_won = re.search(r'([\d,]+)\s*(?:원|₩)(?![^()]*\))', text)
    if price_match_won:
        try:
            return float(price_match_won.group(1).replace(',', ''))
        except ValueError:
            pass

    # 2. 괄호 밖에 있는 숫자로만 끝나는 경우 (단위 '원'이 생략된 경우)
    price_match_end = re.search(r'(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*$(?![^()]*\))', text)
    if price_match_end:
        try:
            return float(price_match_end.group(1).replace(',', ''))
        except ValueError:
            pass

    # 3. 괄호 안에 있지만 계산식이 아닌 단일 숫자 (예: (8900))
    price_in_paren_single = re.search(r'\(([\d,]+)\)$', text)
    if price_in_paren_single:
        try:
            return float(price_in_paren_single.group(1).replace(',', ''))
        except ValueError:
            pass
            
    # 4. 다른 모든 시도가 실패했을 때, 텍스트에서 첫 번째 유효한 숫자 패턴
    price_match_fallback = re.search(r'(\d{1,3}(?:,\d{3})*(?:\.\d+)?)', text)
    if price_match_fallback:
        try:
            return float(price_match_fallback.group(1).replace(',', ''))
        except ValueError:
            pass

    return None
# --- 가격 추출 함수 끝 ---


class PaginatorView(View):
    def __init__(self, interaction: discord.Interaction, pages: list[discord.Embed], timeout: float = 180):
        super().__init__(timeout=timeout)
        self.interaction = interaction
        self.pages = pages
        self.current_page = 0
        self.total_pages = len(pages)

        self.prev_button = Button(label="이전", style=discord.ButtonStyle.primary, disabled=True)
        self.prev_button.callback = self.prev_page
        self.add_item(self.prev_button)

        self.page_number = discord.ui.Button(label=f"{self.current_page + 1}/{self.total_pages}", style=discord.ButtonStyle.secondary, disabled=True)
        self.add_item(self.page_number)

        self.next_button = Button(label="다음", style=discord.ButtonStyle.primary, disabled=self.total_pages <= 1)
        self.next_button.callback = self.next_page
        self.add_item(self.next_button)

        self.update_buttons()

    async def prev_page(self, interaction: discord.Interaction):
        self.current_page -= 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.pages[self.current_page], view=self)

    async def next_page(self, interaction: discord.Interaction):
        self.current_page += 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.pages[self.current_page], view=self)

    def update_buttons(self):
        self.prev_button.disabled = self.current_page == 0
        self.next_button.disabled = self.current_page >= self.total_pages - 1
        if hasattr(self, 'page_number'):
            self.page_number.label = f"{self.current_page + 1}/{self.total_pages}"

    async def on_timeout(self) -> None:
        await self.interaction.edit_original_response(view=None)

@bot.event
async def on_ready():
    print(f'{bot.user}으로 로그인했습니다!')
    try:
        synced = await bot.tree.sync()
        print(f'{len(synced)}개의 커맨드를 동기화했습니다.')
    except Exception as e:
        print(f"커맨드 동기화 오류: {e}")
    bot.loop.create_task(periodic_scan())

@bot.tree.command(name="검색", description="키워드와 일치하는 정보를 보냅니다.")
async def search_keyword(interaction: discord.Interaction, 키워드: str):
    await interaction.response.defer(ephemeral=True)
    try:
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(BLOB_NAME)
        json_bytes = await asyncio.to_thread(blob.download_as_bytes) # 비동기 처리
        data = await asyncio.to_thread(json.load, BytesIO(json_bytes)) # 비동기 처리

        data.sort(key=lambda x: int(x.get('no', 0)) if x.get('no') is not None else 0, reverse=True)
        
        matched_items = [item for item in data if 키워드.lower() in item.get("title","").lower()]

        if not matched_items:
            await interaction.followup.send("해당 키워드에 대한 결과를 찾을 수 없습니다.", ephemeral=True)
            return

        items_per_page = 4
        pages = [matched_items[i:i + items_per_page] for i in range(0, len(matched_items), items_per_page)]
        embed_pages = []

        for page_num, page_items in enumerate(pages):
            embed = discord.Embed(title=f"🔍 키워드 '{키워드}' 검색 결과 (페이지 {page_num + 1}/{len(pages)})", color=discord.Color.blue())
            for index, item in enumerate(page_items):
                title = item.get('title', '정보 없음')
                price = item.get('price', '정보 없음') # 가격 정보 다시 추가
                link = item.get('link', '정보 없음')
                timestamp = item.get('timestamp', '정보 없음')

                embed.add_field(name=f"🎁 상품 {index + 1 + (page_num * items_per_page)}", value="", inline=False)
                embed.add_field(name="제목", value=title, inline=False)
                embed.add_field(name="가격", value=price, inline=True) # 가격 필드 다시 추가
                embed.add_field(name="링크", value=f"[바로가기]({link})" if link != '정보 없음' else '정보 없음', inline=False)
                embed.add_field(name="등록 시간", value=timestamp, inline=True)
                if index < len(page_items) - 1:
                    embed.add_field(name="", value="-" * 30, inline=False)

            embed_pages.append(embed)

        if not embed_pages:
            await interaction.followup.send("검색 결과가 없습니다.", ephemeral=True)
            return

        paginator = PaginatorView(interaction, embed_pages)
        await interaction.followup.send(embed=embed_pages[0], view=paginator, ephemeral=True)

    except Exception as e:
        print(f"검색 중 오류 발생: {e}")
        await interaction.followup.send("검색 중 오류가 발생했습니다. 다시 시도해주세요.", ephemeral=True)

async def fetch_recent_results(키워드: str, since: datetime.datetime, seen_titles: set):
    try:
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(BLOB_NAME)
        json_bytes = await asyncio.to_thread(blob.download_as_bytes) # 비동기 처리
        data = await asyncio.to_thread(json.load, BytesIO(json_bytes)) # 비동기 처리

        matched_items = []
        for item in data:
            title = item.get("title", "")
            timestamp_str = item.get("timestamp", "")
            price = item.get("price", "가격 정보 없음") # 가격 정보 다시 추가

            if 키워드.lower() in title.lower() and timestamp_str and title not in seen_titles:
                try:
                    item_time = datetime.datetime.strptime(timestamp_str, "%Y/%m/%d-%H:%M").astimezone(KST)
                    if item_time > since:
                        # 가격 정보 포함하여 메시지 구성
                        matched_items.append(f"[{title}]({item.get('link', '링크 없음')}) - **가격: {price}**") 
                        seen_titles.add(title)
                except ValueError:
                    continue
        return matched_items
    except Exception as e:
        print(f"최근 결과 검색 중 오류 발생: {e}")
        return []

@bot.tree.command(name="스캔시작", description="새로운 키워드 알림 스캔을 시작합니다.")
async def start_scan(interaction: discord.Interaction, 키워드: str):
    await interaction.response.defer(ephemeral=True) 

    user_id = interaction.user.id

    if user_id not in scanning_users:
        scanning_users[user_id] = {}

    user_keywords = scanning_users[user_id]

    if 키워드 in user_keywords:
        await interaction.followup.send(f"'{키워드}' 키워드는 이미 스캔 중입니다.", ephemeral=True)
        return

    now = datetime.datetime.now(KST)
    one_hour_ago = now - datetime.timedelta(hours=1)

    user_keywords[키워드] = {
        "last_seen_titles": set(),
        "start_time": now
    }

    seen_titles = user_keywords[키워드]["last_seen_titles"]
    recent_results = await fetch_recent_results(키워드, since=one_hour_ago, seen_titles=seen_titles)

    if recent_results:
        try:
            dm_channel = await interaction.user.create_dm()
            await dm_channel.send(f"{interaction.user.mention}님이 입력한 키워드 **'{키워드}'**와 관련한 최근 1시간 이내 정보가 있어요!\n\n" +
                                  "\n".join(recent_results))
        except discord.errors.Forbidden:
            await interaction.followup.send(
                f"'{키워드}' 스캔을 시작했지만, DM 전송이 차단되어 있어 알림을 보내지 못했습니다. "
                "DM 설정을 확인해주세요.", ephemeral=True
            )
            return
        except Exception as e:
            print(f"DM 전송 실패: {e}")
            await interaction.followup.send(f"'{키워드}' 스캔을 시작했지만, DM 전송 중 오류가 발생했습니다. 다시 시도해주세요.", ephemeral=True)
            return
    
    await interaction.followup.send(f"**'{키워드}'**에 대한 스캔을 시작합니다. 새로운 결과가 있으면 DM으로 알려드릴게요. (최대 1시간 내의 최근 정보는 이미 DM으로 발송되었습니다.)", ephemeral=True)


@bot.tree.command(name="스캔확인", description="현재 스캔 중인 키워드를 확인합니다.")
async def check_scan(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)

    user_id = interaction.user.id
    scan_info = scanning_users.get(user_id)

    if not scan_info:
        await interaction.followup.send("현재 스캔 중인 키워드가 없습니다.", ephemeral=True)
        return

    embed = discord.Embed(
        title="🔍 현재 스캔 상태",
        description=f"{interaction.user.name}님이 스캔 중인 키워드 목록입니다.",
        color=discord.Color.orange()
    )

    for keyword, info in scan_info.items():
        start_time = info.get("start_time")
        if isinstance(start_time, datetime.datetime):
            start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S KST")
        else:
            start_time_str = "알 수 없음"
        embed.add_field(name=f"**{keyword}**", value=f"시작 시간: {start_time_str}", inline=False)

    try:
        await interaction.user.send(embed=embed)
        await interaction.followup.send("현재 스캔 상태를 DM으로 보냈습니다.", ephemeral=True)
    except discord.errors.Forbidden:
        await interaction.followup.send("DM을 보낼 수 없습니다. DM 설정을 확인해주세요.", ephemeral=True)
    except Exception as e:
        print(f"스캔 확인 DM 전송 오류: {e}")
        await interaction.followup.send("스캔 상태를 확인하는 중 오류가 발생했습니다.", ephemeral=True)


@bot.tree.command(name="스캔중지", description="키워드 알림 스캔을 중지합니다.(all=전체 키워드 종료)")
async def stop_scan(interaction: discord.Interaction, 키워드: str):
    await interaction.response.defer(ephemeral=True)

    user_id = interaction.user.id
    
    if user_id not in scanning_users:
        await interaction.followup.send("현재 활성화된 키워드 스캔이 없습니다.", ephemeral=True)
        return

    if 키워드.lower() == "all":
        if user_id in scanning_users:
            del scanning_users[user_id]
            await interaction.followup.send("모든 키워드에 대한 스캔을 중지했습니다.", ephemeral=True)
        else:
            await interaction.followup.send("현재 활성화된 키워드 스캔이 없습니다.", ephemeral=True)
        return

    if 키워드 not in scanning_users.get(user_id, {}):
        await interaction.followup.send(f"'{키워드}'에 대한 스캔이 활성화되어 있지 않습니다.", ephemeral=True)
        return

    del scanning_users[user_id][키워드]
    if not scanning_users[user_id]:
        del scanning_users[user_id]

    await interaction.followup.send(f"**'{키워드}'**에 대한 스캔을 중지합니다.", ephemeral=True)

def jaccard_similarity(s1: str, s2: str) -> float:
    """두 문자열의 Jaccard 유사도를 계산합니다."""
    def normalize_text(text):
        text = re.sub(r'[^가-힣a-zA-Z\s]', '', text)
        return set(text.lower().split())

    set1 = normalize_text(s1)
    set2 = normalize_text(s2)

    if not set1 and not set2:
        return 1.0
    if not set1 or not set2:
        return 0.0

    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))
    return intersection / union if union != 0 else 0.0


async def find_similar_deals(
    target_keyword: str, 
    new_match_title: str, 
    all_data: list, 
    seen_titles: set, 
    lookback_months: int, 
    current_time: datetime.datetime
) -> list:
    """
    주어진 키워드와 새로 발견된 핫딜 제목을 기준으로 유사한 과거 핫딜을 찾습니다.
    새로 발견된 핫딜 제목과 비교하여 Levenshtein 거리와 Jaccard 유사도를 사용합니다.
    """
    similar_deals = []
    
    lookback_date = current_time - datetime.timedelta(days=30 * lookback_months)

    for item in all_data:
        await asyncio.sleep(0) 
        item_title = item.get("title", "")
        item_timestamp_str = item.get("timestamp", "")
        
        item_timestamp = item.get('parsed_timestamp') 
        if not item_timestamp and item_timestamp_str:
            try:
                naive_dt = datetime.datetime.strptime(item_timestamp_str, "%Y/%m/%d-%H:%M")
                item_timestamp = KST.localize(naive_dt)
            except ValueError:
                continue

        if not item_title or not item_timestamp or item_title in seen_titles:
            continue

        if item_timestamp < lookback_date:
            continue

        lev_dist = distance(new_match_title.lower(), item_title.lower())
        jac_sim = jaccard_similarity(new_match_title, item_title)

        # 1. 유사 핫딜의 제목에 target_keyword가 포함되어야 함
        # 2. Levenshtein 거리 또는 Jaccard 유사도 조건을 만족해야 함
        if target_keyword.lower() in item_title.lower() and \
           (lev_dist <= LEVENSHTEIN_THRESHOLD or jac_sim >= JACCARD_THRESHOLD):
            similar_deals.append(item)
            seen_titles.add(item_title)
            
    similar_deals.sort(key=lambda x: x.get('parsed_timestamp') or datetime.datetime.min.replace(tzinfo=KST), reverse=True)
            
    return similar_deals[:MAX_SIMILAR_DEALS]


async def process_user_scan_for_keyword(user_id: int, keyword: str, scan_info: dict, all_data: list, now: datetime.datetime):
    """단일 사용자의 단일 키워드에 대한 스캔을 처리합니다."""
    
    last_seen_titles = scan_info["last_seen_titles"]
    start_time = scan_info.get("start_time")
    
    if not start_time:
        start_time = now
        scan_info["start_time"] = now

    # 테스트를 위해 start_time에 15분 여유를 줍니다. (필요 없으면 주석 처리 또는 제거)
    test_start_time = start_time - datetime.timedelta(minutes=15) 

    new_matches = []
    
    current_titles_in_data = {item.get("title") for item in all_data if item.get("title")}

    for item in all_data:
        await asyncio.sleep(0) 
        try:
            item_no = item.get("no")
            item_title = item.get("title", "")
            item_timestamp_str = item.get("timestamp")

            if not item_no or not item_title or not item_timestamp_str:
                continue

            item_timestamp = None
            try:
                naive_dt = datetime.datetime.strptime(item_timestamp_str, "%Y/%m/%d-%H:%M")
                item_timestamp = KST.localize(naive_dt)
            except ValueError as e:
                print(f"시간 변환 오류: '{item_timestamp_str}' - {e}")
                continue

            if keyword.lower() in item_title.lower() and \
               item_title not in last_seen_titles and \
               item_timestamp and item_timestamp >= test_start_time: # test_start_time 사용
                
                new_matches.append(item)
        except Exception as item_error:
            print(f"항목 처리 중 예외 발생: {item_error}. 항목 내용: {item}")
            continue

    if new_matches:
        user = await bot.fetch_user(user_id)
        if user:
            for new_deal in new_matches:
                await asyncio.sleep(0) 
                # --------------------- 새로운 핫딜 알림 임베드 ---------------------
                embed = discord.Embed(title=f"🔔 새로운 키워드 알림: **'{keyword}'**", color=discord.Color.green())
                
                new_deal_title = new_deal.get('title', '정보 없음')
                new_deal_price_str = new_deal.get('price', '정보 없음') # 가격 정보 다시 추가
                new_deal_link = new_deal.get('link', '')
                new_deal_timestamp = new_deal.get('timestamp', '정보 없음')

                embed.add_field(name="제목", value=new_deal_title, inline=False)
                embed.add_field(name="가격", value=new_deal_price_str, inline=True) # 가격 필드 다시 추가
                embed.add_field(name="링크", value=f"[바로가기]({new_deal_link})" if new_deal_link else '정보 없음', inline=False)
                embed.add_field(name="등록 시간", value=new_deal_timestamp, inline=True)
                embed.add_field(name="", value="-" * 30, inline=False) # 구분선 유지

                embed.timestamp = now
                try:
                    await user.send(embed=embed)
                except discord.errors.Forbidden:
                    print(f"경고: {user_id}님의 DM이 막혀 있어 알림을 보내지 못했습니다. (discord.errors.Forbidden)")
                except Exception as e:
                    print(f"오류: DM 전송 중 다른 오류 발생 ({user_id}): {e}")

                # --------------------- 유사 핫딜 정보 임베드 (개별 비교 포함) ---------------------
                # find_similar_deals 함수는 seen_titles를 업데이트하므로, 여기서는 새로 찾은 딜의 제목을 추가하기 위해 임시 set을 넘겨주는 것이 안전할 수 있음.
                # 하지만 이미 process_user_scan_for_keyword의 상위 scope에서 last_seen_titles가 전달되고 있으므로 굳이 여기서 다시 초기화할 필요는 없습니다.
                # 오히려 find_similar_deals의 seen_titles 인자가 의미가 없어질 수 있으므로,
                # 여기서는 그냥 빈 set 또는 new_deal_title만 포함하는 set을 넘겨주는 것이 맞습니다.
                # find_similar_deals에서 seen_titles를 업데이트하게 하거나,
                # 아니면 find_similar_deals가 seen_titles를 반환하도록 변경해야 합니다.
                # 현재 코드를 유지하려면, find_similar_deals 내에서 seen_titles를 복사해서 사용해야 합니다.
                all_deal_titles_for_similar_search = {new_deal_title} # 유사 핫딜 검색을 위한 임시 seen_titles
                similar_deals = await find_similar_deals(
                    keyword, 
                    new_deal_title, 
                    all_data, 
                    all_deal_titles_for_similar_search, # 임시 seen_titles 사용
                    SIMILAR_DEAL_LOOKBACK_MONTHS,
                    now
                )
                
                if similar_deals:
                    similar_embed = discord.Embed(
                        title=f"📦 유사 핫딜 정보: '{new_deal_title}'", 
                        description=f"**{new_deal_title}**에 비해 과거 **유사 핫딜** 가격을 비교합니다. (최대 {MAX_SIMILAR_DEALS}개)", 
                        color=discord.Color.orange()
                    )
                    
                    for s_item in similar_deals:
                        await asyncio.sleep(0) 
                        s_item_title = s_item.get('title', '정보 없음')
                        s_item_price_str = s_item.get('price', '정보 없음') # 가격 정보 다시 추가
                        s_item_link = s_item.get('link', '')
                        s_item_timestamp = s_item.get('timestamp', '정보 없음')

                        similar_embed.add_field(name=f"제목", value=s_item_title, inline=False)
                        similar_embed.add_field(name="가격", value=s_item_price_str, inline=True) # 가격 필드 다시 추가
                        similar_embed.add_field(name="링크", value=f"[바로가기]({s_item_link})" if s_item_link else '정보 없음', inline=False)
                        similar_embed.add_field(name="등록 시간", value=s_item_timestamp, inline=True)
                        similar_embed.add_field(name="", value="-" * 30, inline=False)
                    
                    if similar_embed.fields and similar_embed.fields[-1].value == "-" * 30:
                        similar_embed.remove_field(-1)
                    
                    similar_embed.timestamp = now
                    try:
                        await user.send(embed=similar_embed)
                    except discord.errors.Forbidden:
                        print(f"경고: {user_id}님의 DM이 막혀 유사 핫딜 알림을 보내지 못했습니다.")
                    except Exception as e:
                        print(f"오류: 유사 핫딜 DM 전송 중 다른 오류 발생 ({user_id}): {e}")

    last_seen_titles.update(current_titles_in_data)


async def periodic_scan():
    await bot.wait_until_ready()
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    while not bot.is_closed():
        now = datetime.datetime.now(KST)
        
        try:
            blob = bucket.blob(BLOB_NAME)
            json_bytes = await asyncio.to_thread(blob.download_as_bytes) # 비동기 처리
            raw_data = await asyncio.to_thread(json.load, BytesIO(json_bytes))
            
            if not isinstance(raw_data, list):
                print(f"오류: Google Cloud Storage에서 로드된 데이터가 리스트 형식이 아닙니다. 타입: {type(raw_data)}")
                await asyncio.sleep(SCAN_INTERVAL)
                continue

            # 데이터를 전처리하여 메모리에 효율적으로 저장 (예: timestamp 파싱)
            processed_data = []
            for item in raw_data:
                await asyncio.sleep(0) # 데이터 전처리 중 블로킹 방지
                try:
                    item_timestamp_str = item.get("timestamp")
                    if item_timestamp_str:
                        naive_dt = datetime.datetime.strptime(item_timestamp_str, "%Y/%m/%d-%H:%M")
                        item['parsed_timestamp'] = KST.localize(naive_dt)
                    else:
                        item['parsed_timestamp'] = None
                    processed_data.append(item)
                except ValueError as e:
                    print(f"데이터 전처리 중 시간 변환 오류: '{item_timestamp_str}' - {e}")
                    item['parsed_timestamp'] = None # 파싱 실패 시 None으로 설정
                    processed_data.append(item)
                except Exception as e:
                    print(f"데이터 전처리 중 기타 오류: {item.get('title', '제목 없음')} - {e}")
                    item['parsed_timestamp'] = None
                    processed_data.append(item)
            
            tasks = []
            for user_id, keywords_info in scanning_users.copy().items():
                for keyword, scan_info in keywords_info.copy().items():
                    tasks.append(process_user_scan_for_keyword(user_id, keyword, scan_info, processed_data, now))
            
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            
        except Exception as e:
            print(f"주기적 스캔 중 치명적인 오류 발생: {e}")

        await asyncio.sleep(SCAN_INTERVAL)

bot.run(TOKEN)