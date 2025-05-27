import discord
from discord.ui import View, Button
from discord.ext import commands
from google.cloud import storage
import json
from io import BytesIO
import asyncio
import datetime
import pytz

# 봇 토큰을 여기에 입력하세요
TOKEN = ''

intents = discord.Intents.default()
intents.message_content = True # 메시지 내용을 읽기 위한 권한 활성화

bot = commands.Bot(command_prefix='/', intents=intents)

# 스캔 기능을 활성화한 사용자와 키워드, 마지막 스캔 시의 데이터 ID, 스캔 시작 시간을 저장할 딕셔너리
scanning_users = {}
SCAN_INTERVAL = 5 * 60 # 스캔 간격 (20분 * 60초)
BUCKET_NAME = 'moastorage'
BLOB_NAME = 'data/hotdeal.json'

# KST (Korean Standard Time) 타임존 객체 생성
KST = pytz.timezone('Asia/Seoul')

class PaginatorView(View):
    def __init__(self, interaction: discord.Interaction, pages: list[discord.Embed], timeout: float = 180):
        super().__init__(timeout=timeout)
        self.interaction = interaction
        self.pages = pages
        self.current_page = 0
        self.total_pages = len(pages)

        # 이전 버튼
        self.prev_button = Button(label="이전", style=discord.ButtonStyle.primary, disabled=True)
        self.prev_button.callback = self.prev_page
        self.add_item(self.prev_button)

        # 페이지 번호 표시 (선택 사항)
        self.page_number = discord.ui.Button(label=f"{self.current_page + 1}/{self.total_pages}", style=discord.ButtonStyle.secondary, disabled=True)
        self.add_item(self.page_number)

        # 다음 버튼
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
        synced = await bot.tree.sync() # 봇이 속한 모든 서버에 동기화
        print(f'{len(synced)}개의 커맨드를 동기화했습니다.')
    except Exception as e:
        print(f"커맨드 동기화 오류: {e}")
    bot.loop.create_task(periodic_scan()) # 봇 시작 시 스캔 작업 시작

@bot.tree.command(name="검색", description="키워드와 일치하는 정보를 보냅니다.")
async def search_keyword(interaction: discord.Interaction, 키워드: str):
    try:
        client = storage.Client()
        bucket_name = BUCKET_NAME
        blob_name = BLOB_NAME
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        json_bytes = blob.download_as_bytes()
        data = json.load(BytesIO(json_bytes))
        matched_items = [item for item in data if 키워드 in item.get("title","")]

        if not matched_items:
            await interaction.response.send_message("해당 키워드에 대한 결과를 찾을 수 없습니다.", ephemeral=True)
            return

        # 결과를 페이지로 나누기
        items_per_page = 4
        pages = [matched_items[i:i + items_per_page] for i in range(0, len(matched_items), items_per_page)]
        embed_pages = []

        for page_num, page_items in enumerate(pages):
            embed = discord.Embed(title=f"🔍 키워드 '{키워드}' 검색 결과 (페이지 {page_num + 1}/{len(pages)})", color=discord.Color.blue())
            for index, item in enumerate(page_items):
                title = item.get('title', '정보 없음')
                price = item.get('price', '정보 없음')
                link = item.get('link', '정보 없음')
                timestamp = item.get('timestamp', '정보 없음')

                embed.add_field(name=f"🎁 상품 {index + 1 + (page_num * items_per_page)}", value="", inline=False)
                embed.add_field(name="제목", value=title, inline=False)
                embed.add_field(name="가격", value=price, inline=True)
                embed.add_field(name="링크", value=link, inline=False)
                embed.add_field(name="등록 시간", value=timestamp, inline=True)
                if index < len(page_items) - 1: # 마지막 상품이 아니면 구분선 추가
                    embed.add_field(name="", value="-" * 30, inline=False) # 일반적인 하이픈(-) 구분선 사용

            embed_pages.append(embed)

        if not embed_pages:
            await interaction.response.send_message("검색 결과가 없습니다.", ephemeral=True)
            return

        # PaginatorView 생성 및 메시지 전송
        paginator = PaginatorView(interaction, embed_pages)
        await interaction.response.send_message(embed=embed_pages[0], view=paginator, ephemeral=True)

    except Exception as e:
        print(f"에러 발생: {e}")
        await interaction.response.send_message("검색 중 오류가 발생했습니다.", ephemeral=True)


async def fetch_recent_results(키워드: str, since: datetime.datetime, seen_titles: set):
    try:
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(BLOB_NAME)
        json_bytes = blob.download_as_bytes()
        data = json.load(BytesIO(json_bytes))

        matched_items = []
        for item in data:
            title = item.get("title", "")
            timestamp_str = item.get("timestamp", "")

            if 키워드 in title and timestamp_str and title not in seen_titles:
                try:
                    item_time = datetime.datetime.strptime(timestamp_str, "%Y/%m/%d-%H:%M").astimezone(KST)
                    if item_time > since:
                        matched_items.append(f"[{title}]({item.get('link', '링크 없음')}) - {item.get('price', '가격 정보 없음')}")
                        seen_titles.add(title)  # 제목 기준으로 중복 체크
                except ValueError:
                    print(f"시간 파싱 실패: {timestamp_str}")
                    continue

        return matched_items

    except Exception as e:
        print(f"최근 결과 검색 중 오류 발생: {e}")
        return []

@bot.tree.command(name="스캔시작", description="새로운 키워드 알림 스캔을 시작합니다.")
async def start_scan(interaction: discord.Interaction, 키워드: str):
    user_id = interaction.user.id

    if user_id not in scanning_users:
        scanning_users[user_id] = {}

    user_keywords = scanning_users[user_id]

    if 키워드 in user_keywords:
        await interaction.response.send_message(f"'{키워드}' 키워드는 이미 스캔 중입니다.", ephemeral=True)
        return

    now = datetime.datetime.now(KST)
    one_hour_ago = now - datetime.timedelta(hours=1)

    # 스캔 상태 저장 및 중복 추적용 세트 초기화
    user_keywords[키워드] = {
        "last_seen_titles": set(),
        "start_time": now
    }

    print(f"DEBUG: User {user_id} started scan for '{키워드}' at {now}")

    seen_titles = user_keywords[키워드]["last_seen_titles"]
    recent_results = await fetch_recent_results(키워드, since=one_hour_ago, seen_titles=seen_titles)

    if recent_results:
        try:
            dm_channel = await interaction.user.create_dm()
            await dm_channel.send(f"{interaction.user.mention}님이 입력한 키워드 '{키워드}'와 관련한 최신 정보가 있어요!\n\n" +
                                  "\n".join(recent_results))
        except Exception as e:
            print(f"DM 전송 실패: {e}")
    else:
        print("최근 정보 없음, 현행 유지")

    await interaction.response.send_message(f"'{키워드}'에 대한 스캔을 시작합니다. 새로운 결과가 있으면 DM으로 알려드릴게요.", ephemeral=True)

@bot.tree.command(name="스캔확인", description="현재 스캔 중인 키워드를 확인합니다.")
async def check_scan(interaction: discord.Interaction):
    user_id = interaction.user.id
    scan_info = scanning_users.get(user_id)

    if not scan_info:
        await interaction.response.send_message("현재 스캔 중인 키워드가 없습니다.", ephemeral=True)
        return

    embed = discord.Embed(
        title="🔍 현재 스캔 상태",
        description=f"{interaction.user.name}님이 스캔 중인 키워드 목록입니다.",
        color=discord.Color.orange()
    )

    for keyword, info in scan_info.items():
        start_time = info.get("start_time")
        if isinstance(start_time, datetime.datetime):
            start_time = start_time.strftime("%Y-%m-%d %H:%M:%S")
        embed.add_field(name=keyword, value=f"시작 시간: {start_time}", inline=False)

    try:
        await interaction.user.send(embed=embed)
        await interaction.response.send_message("현재 스캔 상태를 DM으로 보냈습니다.", ephemeral=True)
    except discord.errors.Forbidden:
        await interaction.response.send_message("DM을 보낼 수 없습니다. DM 설정을 확인해주세요.", ephemeral=True)


@bot.tree.command(name="스캔중지", description="키워드 알림 스캔을 중지합니다.(all=전체 키워드 종료)")
async def stop_scan(interaction: discord.Interaction, 키워드: str):
    user_id = interaction.user.id

    
    if user_id not in scanning_users:
        await interaction.response.send_message("현재 활성화된 키워드 스캔이 없습니다.", ephemeral=True)
        return

    
    if 키워드.lower() == "all":
        del scanning_users[user_id]
        print(f"DEBUG: User {user_id} stopped ALL scans")
        await interaction.response.send_message("모든 키워드에 대한 스캔을 중지했습니다.", ephemeral=True)
        return

    
    if 키워드 not in scanning_users[user_id]:
        await interaction.response.send_message(f"'{키워드}'에 대한 스캔이 활성화되어 있지 않습니다.", ephemeral=True)
        return

    del scanning_users[user_id][키워드]
    if not scanning_users[user_id]:
        del scanning_users[user_id]

    print(f"DEBUG: User {user_id} stopped scan for '{키워드}'")
    await interaction.response.send_message(f"'{키워드}'에 대한 스캔을 중지합니다.", ephemeral=True)


async def process_user_scan(user_id, keyword, current_data, now):
    keyword = keyword["keyword"]
    last_seen_ids = keyword["last_seen_ids"]
    start_time = keyword.get("start_time")
    if not start_time:
        start_time = datetime.datetime.now(KST)
        print(f"DEBUG: User {user_id} had no start_time, defaulting to current KST: {start_time}")

    new_matches = []

    # print(f"\n--- 디버깅: 사용자 {user_id} 스캔 (키워드: '{keyword}') ---")
    # print(f"  스캔 시작 시간 (KST): {start_time}")
    # print(f"  이전에 본 ID 개수: {len(last_seen_ids)}개")
    # print(f"  현재 데이터 항목 총 개수: {len(current_data)}개")

    for i, item in enumerate(current_data):
        try: 
            item_no = item.get("no")
            item_title = item.get("title", "")
            item_timestamp_str = item.get("timestamp")

            # print(f"\n  처리 중인 항목 #{i+1}: ID={item_no}, 제목='{item_title}', 원본 타임스탬프='{item_timestamp_str}'")

            item_timestamp = None
            if item_timestamp_str:
                try:
                    # JSON 데이터의 timestamp 형식이 'yyyy/mm/dd-hh:ss'
                    naive_dt = datetime.datetime.strptime(item_timestamp_str, "%Y/%m/%d-%H:%M")
                    item_timestamp = KST.localize(naive_dt) # KST로 지역화
                    # print(f"    파싱된 항목 타임스탬프 (KST): {item_timestamp}")
                except ValueError:
                    print(f"    오류: '{item_timestamp_str}'의 타임스탬프 형식이 잘못되었습니다. 해당 항목을 건너뜀.")
                    continue

            # 조건 평가
            is_new_id = item_no and item_no not in last_seen_ids
            has_keyword = keyword in item_title
            is_after_scan_start = item_timestamp and item_timestamp >= start_time

            # print(f"    조건 확인:")
            # print(f"      - 새로운 ID인가? ({item_no}가 last_seen_ids에 없는가?): {is_new_id}")
            # print(f"      - 키워드('{keyword}')가 제목('{item_title}')에 포함되어 있는가?: {has_keyword}")
            # print(f"      - 스캔 시작 시간 이후인가? ({item_timestamp} >= {start_time}): {is_after_scan_start}")

            if (is_new_id and has_keyword and is_after_scan_start):
                new_matches.append(item)
                # print(f"    --> 모든 조건 만족: '{item_title}'")
            else:
                print(f"    --> 모든 조건을 만족하지 못함.")
        except Exception as item_error: # 개별 item 처리 중 발생한 에러를 잡습니다.
            print(f"오류: 항목 #{i+1} 처리 중 예외 발생: {item_error}. 항목 내용: {item}")
            # 이 오류가 발생한 경우, 해당 항목을 건너뛰고 다음 항목으로 진행
            continue

    if new_matches:
        user = await bot.fetch_user(user_id)
        if user:
            print(f"DEBUG: 사용자 {user_id}에게 {len(new_matches)}개의 새 알림을 DM으로 전송 중.")
            embed = discord.Embed(title=f"🔔 새로운 키워드 알림: '{keyword}'", color=discord.Color.green())
            for item in new_matches:
                embed.add_field(name="제목", value=item.get('title', '정보 없음'), inline=False)
                embed.add_field(name="가격", value=item.get('price', '정보 없음'), inline=True)
                embed.add_field(name="링크", value=item.get('link', '정보 없음'), inline=False)
                embed.add_field(name="등록 시간", value=item.get('timestamp', '정보 없음'), inline=True)
                embed.timestamp = now
                try:
                    await user.send(embed=embed)
                except discord.errors.Forbidden:
                    print(f"경고: {user_id}님의 DM이 막혀 있어 알림을 보내지 못했습니다. (discord.errors.Forbidden)")
                except Exception as e:
                    print(f"오류: DM 전송 중 다른 오류 발생 ({user_id}): {e}")

    # **여기서 last_seen_ids 업데이트 방식을 변경해봅시다.**
    # 현재 데이터의 모든 ID를 last_seen_ids에 추가하여 다음 스캔에서 중복 알림 방지
    # 리스트 컴프리헨션을 먼저 실행하여 잠재적 문제를 분리합니다.
    ids_to_add = []
    for item in current_data:
        item_no = item.get("no")
        if item_no:
            ids_to_add.append(item_no)

    scanning_users[user_id]["last_seen_ids"].update(ids_to_add)
    print(f"DEBUG: 사용자 {user_id}의 last_seen_ids가 {len(scanning_users[user_id]['last_seen_ids'])}개로 업데이트됨.")


async def periodic_scan():
    await bot.wait_until_ready()
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    while not bot.is_closed():
        now = datetime.datetime.now(KST) # 주기적인 스캔 시간도 KST로
        print(f"\n--- 주기적 스캔 시작: {now.strftime('%Y-%m-%d %H:%M:%S KST')} ---")
        print(f"현재 스캔 활성화 사용자: {list(scanning_users.keys())}")
        try:
            blob = bucket.blob(BLOB_NAME)
            json_bytes = blob.download_as_bytes()
            current_data = json.load(BytesIO(json_bytes))
            # print(f"DEBUG: Google Cloud Storage에서 데이터 로드 성공. 로드된 데이터 타입: {type(current_data)}")
            # if isinstance(current_data, list):
            #     print(f"DEBUG: 로드된 데이터 항목 총 개수: {len(current_data)}")
            #     if len(current_data) > 0:
            #         print(f"DEBUG: 첫 번째 항목의 타입: {type(current_data[0])}")
            #         print(f"DEBUG: 첫 번째 항목 내용: {current_data[0]}")
            #         if isinstance(current_data[0], dict):
            #             print(f"DEBUG: 첫 번째 항목에 'no' 키가 있는가?: {'no' in current_data[0]}")
            #             print(f"DEBUG: 첫 번째 항목에 'title' 키가 있는가?: {'title' in current_data[0]}")
            #             print(f"DEBUG: 첫 번째 항목에 'timestamp' 키가 있는가?: {'timestamp' in current_data[0]}")
            #     else:
            #         print("DEBUG: 로드된 데이터 리스트가 비어 있습니다.")
            # elif isinstance(current_data, dict):
            #     print("DEBUG: 로드된 데이터가 딕셔너리입니다. 예상되는 리스트 구조와 다를 수 있습니다.")
            #     print(f"DEBUG: 딕셔너리 키: {current_data.keys()}")

            # 각 사용자의 스캔 작업을 비동기적으로 실행
            # 각 스캔 작업에서 발생할 수 있는 오류를 개별적으로 처리하기 위해 gather에 return_exceptions=True 추가
            results = await asyncio.gather(*[
                process_user_scan(user_id, keyword, scan_info, current_data, now)
                for user_id, keywords in scanning_users.copy().items()
                for keyword, scan_info in keywords.items()
            ], return_exceptions=True)


            for user_id, result in zip(scanning_users.copy().keys(), results):
                if isinstance(result, Exception):
                    print(f"오류: 사용자 {user_id}의 스캔 작업 중 예외 발생: {result}")


        except Exception as e:
            print(f"**치명적인 오류 발생: 스캔 중 오류 발생: {e}**")

        await asyncio.sleep(SCAN_INTERVAL) # 20분마다 스캔 실행

bot.run(TOKEN)