import os
import json
import asyncio
from datetime import datetime, timezone

import aiohttp
import discord
from discord.ext import commands, tasks
from discord import app_commands
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")

GROUP_ID = 5008654
STAFF_MIN_RANK = 50
CONNECTIONS_CHANNEL = "connections"
AUTO_SCAN_MINUTES = 5
CONFIRM_SCANS = 2
TEAM_CONCURRENCY = 4
MAX_MULTI_USERS = 25
MAX_TEAM_USERS = 250

STAFF_CACHE_FILE = "witness_staff_cache.json"
PENDING_FILE = "witness_pending.json"
STATE_FILE = "witness_state.json"

GROUPS_API = "https://groups.roblox.com"
FRIENDS_API = "https://friends.roblox.com"
USERS_API = "https://users.roblox.com"

intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)
scan_lock = asyncio.Lock()


def load_json(path, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def save_json(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)


def utc_now():
    return datetime.now(timezone.utc)


def find_channel(guild, name):
    return discord.utils.get(guild.text_channels, name=name)


def profile_url(user_id):
    return f"https://www.roblox.com/users/{user_id}/profile"


async def request_json(session, method, url, payload=None, attempts=4):
    last_error = None
    for attempt in range(attempts):
        try:
            async with session.request(method, url, json=payload) as response:
                if response.status == 200:
                    return await response.json()
                if response.status == 429:
                    retry_after = response.headers.get("Retry-After", "2")
                    try:
                        wait = float(retry_after)
                    except ValueError:
                        wait = 2.0
                    await asyncio.sleep(min(max(wait, 1), 10))
                    continue
                text = await response.text()
                last_error = RuntimeError(
                    f"Roblox API {response.status}: {text[:250]}"
                )
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            last_error = exc
        await asyncio.sleep(1.5 * (attempt + 1))
    raise last_error or RuntimeError("Roblox request failed")


async def get_group_info(session):
    return await request_json(
        session, "GET", f"{GROUPS_API}/v1/groups/{GROUP_ID}"
    )


async def get_group_roles(session):
    data = await request_json(
        session, "GET", f"{GROUPS_API}/v1/groups/{GROUP_ID}/roles"
    )
    return data.get("roles", [])


async def get_users_in_role(session, role_id):
    users = {}
    cursor = None
    while True:
        url = (
            f"{GROUPS_API}/v1/groups/{GROUP_ID}/roles/{role_id}/users"
            f"?limit=100&sortOrder=Asc"
        )
        if cursor:
            url += f"&cursor={cursor}"
        data = await request_json(session, "GET", url)
        for entry in data.get("data", []):
            user = entry.get("user", entry)
            user_id = user.get("userId") or user.get("id")
            if user_id is None:
                continue
            users[str(user_id)] = {
                "username": user.get("username") or user.get("name") or "Unknown",
                "display_name": user.get("displayName") or "",
            }
        cursor = data.get("nextPageCursor")
        if not cursor:
            break
    return users


async def resolve_username(session, username):
    data = await request_json(
        session,
        "POST",
        f"{USERS_API}/v1/usernames/users",
        payload={"usernames": [username], "excludeBannedUsers": False},
    )
    matches = data.get("data", [])
    if not matches:
        return None
    user = matches[0]
    return {
        "user_id": str(user.get("id")),
        "username": user.get("name", username),
        "display_name": user.get("displayName", ""),
    }


async def get_friends(session, user_id):
    data = await request_json(
        session, "GET", f"{FRIENDS_API}/v1/users/{user_id}/friends"
    )
    friends = {}
    for friend in data.get("data", []):
        friend_id = friend.get("id") or friend.get("userId")
        if friend_id is None:
            continue
        friends[str(friend_id)] = {
            "username": friend.get("name") or friend.get("username") or "Unknown",
            "display_name": friend.get("displayName") or "",
        }
    return friends


async def build_snapshot():
    staff = {}
    roles_out = {}
    leaders = {}
    headers = {"User-Agent": "The-Witness-Divine-Sister-Court/1.0"}
    timeout = aiohttp.ClientTimeout(total=120)

    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
        group_info = await get_group_info(session)
        roles = await get_group_roles(session)
        staff_roles = sorted(
            [r for r in roles if int(r.get("rank", 0)) >= STAFF_MIN_RANK],
            key=lambda r: int(r.get("rank", 0)),
        )

        for role in staff_roles:
            role_id = role.get("id")
            role_name = role.get("name", "Unknown Role")
            role_rank = int(role.get("rank", 0))
            users = await get_users_in_role(session, role_id)
            roles_out[str(role_id)] = {
                "name": role_name,
                "rank": role_rank,
                "count": len(users),
            }

            for user_id, info in users.items():
                staff[user_id] = {
                    "username": info["username"],
                    "display_name": info["display_name"],
                    "role_id": role_id,
                    "role_name": role_name,
                    "role_rank": role_rank,
                }

                low = role_name.lower()
                if "matrona" in low or "reverend" in low:
                    leaders[user_id] = {
                        "username": info["username"],
                        "display_name": info["display_name"],
                        "role_name": role_name,
                        "category": "Matrona" if "matrona" in low else "Reverend",
                    }

        owner = group_info.get("owner")
        if owner:
            owner_id = owner.get("userId") or owner.get("id")
            if owner_id is not None:
                leaders[str(owner_id)] = {
                    "username": owner.get("username") or owner.get("name") or "Divine Sister",
                    "display_name": owner.get("displayName", ""),
                    "role_name": "Divine Sister / Group Owner",
                    "category": "Owner",
                }

    snapshot = {
        "staff": staff,
        "roles": roles_out,
        "leaders": leaders,
        "updated_at": utc_now().isoformat(),
    }
    save_json(STATE_FILE, snapshot)
    return snapshot


async def ensure_snapshot():
    snapshot = load_json(STATE_FILE, {})
    if not snapshot.get("staff") or not snapshot.get("leaders"):
        return await build_snapshot()
    return snapshot


def filter_leaders(leaders, scope):
    scope = scope.lower()
    if scope == "matronas":
        return {k: v for k, v in leaders.items() if v.get("category") == "Matrona"}
    if scope == "reverends":
        return {k: v for k, v in leaders.items() if v.get("category") == "Reverend"}
    if scope == "owner":
        return {k: v for k, v in leaders.items() if v.get("category") == "Owner"}
    return leaders


async def check_user_connections(subject, leaders):
    headers = {"User-Agent": "The-Witness-Divine-Sister-Court/1.0"}
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
        friends = await get_friends(session, subject["user_id"])

    found = []
    for leader_id, leader in leaders.items():
        if str(leader_id) in friends:
            found.append({"user_id": str(leader_id), **leader})
    found.sort(key=lambda x: (x.get("category", ""), x.get("username", "").lower()))
    return found


def subject_from_cached_user(user_id, info):
    return {
        "user_id": str(user_id),
        "username": info.get("username", "Unknown"),
        "display_name": info.get("display_name", ""),
        "role_name": info.get("role_name", "Unknown Role"),
    }


def build_single_embed(subject, connections, scope):
    embed = discord.Embed(
        title="🔗 PUBLIC CONNECTION CHECK",
        colour=discord.Colour.gold() if connections else discord.Colour.dark_grey(),
        timestamp=utc_now(),
    )
    embed.description = (
        f"### {subject['username']}\n"
        f"**Current role:** {subject.get('role_name', 'Unknown')}\n\n"
        + (
            f"Found **{len(connections)}** public leadership friend connection(s)."
            if connections
            else "No public leadership friend connections were found in this scope."
        )
    )
    embed.add_field(name="Scope", value=scope.title(), inline=True)
    embed.add_field(
        name="Profile",
        value=f"[Open Roblox profile]({profile_url(subject['user_id'])})",
        inline=True,
    )
    for connection in connections[:20]:
        embed.add_field(
            name=connection.get("username", "Unknown"),
            value=(
                f"{connection.get('role_name', 'Unknown')}\n"
                f"`{connection.get('category', 'Leader')}`"
            ),
            inline=True,
        )
    embed.set_footer(
        text="The Witness • Public Roblox friendships only • Connection ≠ proof of favoritism"
    )
    return embed


def build_bulk_embeds(title, results, scope, checked_count):
    connected = [r for r in results if r["connections"]]
    if not connected:
        embed = discord.Embed(
            title=title,
            description=(
                f"Checked **{checked_count}** account(s).\n"
                f"No public leadership friend connections were found for scope **{scope.title()}**."
            ),
            colour=discord.Colour.dark_grey(),
            timestamp=utc_now(),
        )
        embed.set_footer(text="The Witness • Connection ≠ proof of favoritism")
        return [embed]

    pages = []
    for start in range(0, min(len(connected), 200), 20):
        page = connected[start:start + 20]
        embed = discord.Embed(
            title=title,
            description=(
                f"Checked **{checked_count}** account(s).\n"
                f"**{len(connected)}** had at least one public leadership connection.\n"
                f"Scope: **{scope.title()}**"
            ),
            colour=discord.Colour.gold(),
            timestamp=utc_now(),
        )
        for result in page:
            subject = result["subject"]
            lines = [
                f"• **{c['username']}** — {c['role_name']}"
                for c in result["connections"][:8]
            ]
            if len(result["connections"]) > 8:
                lines.append(f"• +{len(result['connections']) - 8} more")
            embed.add_field(
                name=f"🔗 {subject['username']} — {subject.get('role_name', 'Unknown')}",
                value="\n".join(lines),
                inline=False,
            )
        embed.set_footer(
            text="The Witness • Public Roblox friendships only • Connection ≠ proof of favoritism"
        )
        pages.append(embed)
    return pages[:10]


async def post_single_result(guild, subject, connections, scope):
    channel = find_channel(guild, CONNECTIONS_CHANNEL)
    if not channel:
        raise RuntimeError(f"#{CONNECTIONS_CHANNEL} does not exist")
    return await channel.send(embed=build_single_embed(subject, connections, scope))


async def post_bulk_results(guild, title, results, scope, checked_count):
    channel = find_channel(guild, CONNECTIONS_CHANNEL)
    if not channel:
        raise RuntimeError(f"#{CONNECTIONS_CHANNEL} does not exist")
    embeds = build_bulk_embeds(title, results, scope, checked_count)
    await channel.send(embeds=embeds[:10])


async def check_subjects_safely(subjects, leaders):
    semaphore = asyncio.Semaphore(TEAM_CONCURRENCY)

    async def worker(subject):
        async with semaphore:
            try:
                connections = await check_user_connections(subject, leaders)
                return {"subject": subject, "connections": connections, "error": None}
            except Exception as exc:
                return {"subject": subject, "connections": [], "error": repr(exc)}
            finally:
                await asyncio.sleep(0.15)

    return await asyncio.gather(*(worker(subject) for subject in subjects))


async def username_choices(current):
    snapshot = await ensure_snapshot()
    current_low = current.lower()
    choices = []
    for _, info in snapshot.get("staff", {}).items():
        username = info.get("username", "Unknown")
        display_name = info.get("display_name", "")
        if current_low in (username + " " + display_name).lower():
            label = username + (f" ({display_name})" if display_name else "")
            choices.append(app_commands.Choice(name=label[:100], value=username))
        if len(choices) >= 25:
            break
    return choices


async def role_choices(current):
    snapshot = await ensure_snapshot()
    current_low = current.lower()
    choices = []
    roles = sorted(
        snapshot.get("roles", {}).values(),
        key=lambda r: int(r.get("rank", 0)),
        reverse=True,
    )
    for role in roles:
        name = role.get("name", "Unknown Role")
        if current_low in name.lower():
            choices.append(
                app_commands.Choice(
                    name=f"{name} ({role.get('count', 0)})"[:100],
                    value=name,
                )
            )
        if len(choices) >= 25:
            break
    return choices


SCOPE_CHOICES = [
    app_commands.Choice(name="All leadership", value="all"),
    app_commands.Choice(name="Matronas only", value="matronas"),
    app_commands.Choice(name="Reverends only", value="reverends"),
    app_commands.Choice(name="Divine Sister / group owner only", value="owner"),
]


@bot.tree.command(name="connections", description="Check one Roblox user against DPI leadership friendships")
@app_commands.describe(username="Roblox username", scope="Leadership set to compare against")
@app_commands.choices(scope=SCOPE_CHOICES)
async def connections(interaction: discord.Interaction, username: str, scope: app_commands.Choice[str]):
    await interaction.response.defer(ephemeral=True, thinking=True)
    snapshot = await ensure_snapshot()
    leaders = filter_leaders(snapshot.get("leaders", {}), scope.value)
    if not leaders:
        await interaction.followup.send("No leadership targets were found for that scope.", ephemeral=True)
        return

    subject = None
    for user_id, info in snapshot.get("staff", {}).items():
        if info.get("username", "").lower() == username.lower():
            subject = subject_from_cached_user(user_id, info)
            break

    if subject is None:
        headers = {"User-Agent": "The-Witness-Divine-Sister-Court/1.0"}
        async with aiohttp.ClientSession(headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as session:
            resolved = await resolve_username(session, username)
        if resolved is None:
            await interaction.followup.send("I couldn't find that Roblox username.", ephemeral=True)
            return
        subject = {**resolved, "role_name": "Outside cached staff roster"}

    found = await check_user_connections(subject, leaders)
    message = await post_single_result(interaction.guild, subject, found, scope.value)
    await interaction.followup.send(
        f"🔗 Done — **{len(found)}** connection(s). Posted in {message.channel.mention}.",
        ephemeral=True,
    )


@connections.autocomplete("username")
async def connections_username_autocomplete(interaction: discord.Interaction, current: str):
    return await username_choices(current)


@bot.tree.command(name="connectionsmany", description="Check several Roblox usernames against DPI leadership")
@app_commands.describe(usernames=f"Comma-separated Roblox usernames (max {MAX_MULTI_USERS})", scope="Leadership set")
@app_commands.choices(scope=SCOPE_CHOICES)
async def connectionsmany(interaction: discord.Interaction, usernames: str, scope: app_commands.Choice[str]):
    await interaction.response.defer(ephemeral=True, thinking=True)
    names = list(dict.fromkeys(
        p.strip() for p in usernames.replace("\n", ",").split(",") if p.strip()
    ))[:MAX_MULTI_USERS]
    if not names:
        await interaction.followup.send("Give me at least one username.", ephemeral=True)
        return

    snapshot = await ensure_snapshot()
    leaders = filter_leaders(snapshot.get("leaders", {}), scope.value)
    cached = {
        info.get("username", "").lower(): subject_from_cached_user(uid, info)
        for uid, info in snapshot.get("staff", {}).items()
    }

    subjects = []
    unresolved = []
    for name in names:
        if name.lower() in cached:
            subjects.append(cached[name.lower()])
        else:
            unresolved.append(name)

    if unresolved:
        headers = {"User-Agent": "The-Witness-Divine-Sister-Court/1.0"}
        async with aiohttp.ClientSession(headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as session:
            for name in unresolved:
                resolved = await resolve_username(session, name)
                if resolved:
                    subjects.append({**resolved, "role_name": "Outside cached staff roster"})
                await asyncio.sleep(0.1)

    results = await check_subjects_safely(subjects, leaders)
    await post_bulk_results(interaction.guild, "🔗 MULTI-USER CONNECTION CHECK", results, scope.value, len(subjects))
    connected_count = sum(bool(r["connections"]) for r in results)
    channel = find_channel(interaction.guild, CONNECTIONS_CHANNEL)
    await interaction.followup.send(
        f"🔗 Checked **{len(subjects)}** account(s); **{connected_count}** had connections. Results: {channel.mention}",
        ephemeral=True,
    )


@bot.tree.command(name="teamconnections", description="Check a whole DPI staff rank against leadership friendships")
@app_commands.describe(rank="Staff rank/team to scan", scope="Leadership set")
@app_commands.choices(scope=SCOPE_CHOICES)
async def teamconnections(interaction: discord.Interaction, rank: str, scope: app_commands.Choice[str]):
    await interaction.response.defer(ephemeral=True, thinking=True)
    snapshot = await ensure_snapshot()
    staff = snapshot.get("staff", {})
    role_names = {info.get("role_name", "") for info in staff.values()}

    chosen = next((r for r in role_names if r.lower() == rank.lower()), None)
    if chosen is None:
        matches = [r for r in role_names if rank.lower() in r.lower()]
        if len(matches) == 1:
            chosen = matches[0]
    if chosen is None:
        await interaction.followup.send("I couldn't identify that rank. Use autocomplete.", ephemeral=True)
        return

    subjects = [
        subject_from_cached_user(uid, info)
        for uid, info in staff.items()
        if info.get("role_name") == chosen
    ][:MAX_TEAM_USERS]

    leaders = filter_leaders(snapshot.get("leaders", {}), scope.value)
    results = await check_subjects_safely(subjects, leaders)
    await post_bulk_results(
        interaction.guild,
        f"🔗 TEAM CONNECTION CHECK — {chosen}",
        results,
        scope.value,
        len(subjects),
    )
    connected_count = sum(bool(r["connections"]) for r in results)
    channel = find_channel(interaction.guild, CONNECTIONS_CHANNEL)
    await interaction.followup.send(
        f"🔗 Scanned **{len(subjects)}** member(s) of **{chosen}**; **{connected_count}** had connections. Results: {channel.mention}",
        ephemeral=True,
    )


@teamconnections.autocomplete("rank")
async def teamconnections_rank_autocomplete(interaction: discord.Interaction, current: str):
    return await role_choices(current)


@bot.tree.command(name="witnessrefresh", description="Refresh staff and leadership caches from Roblox")
async def witnessrefresh(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True, thinking=True)
    try:
        snapshot = await build_snapshot()
        await interaction.followup.send(
            f"👁️ Refreshed **{len(snapshot['staff'])}** staff and **{len(snapshot['leaders'])}** leadership targets.",
            ephemeral=True,
        )
    except Exception as exc:
        print("Witness refresh error:", repr(exc))
        await interaction.followup.send(f"❌ Refresh failed: `{type(exc).__name__}`", ephemeral=True)


@bot.tree.command(name="witnessstatus", description="Show The Witness status")
async def witnessstatus(interaction: discord.Interaction):
    snapshot = load_json(STATE_FILE, {})
    leaders = snapshot.get("leaders", {})
    embed = discord.Embed(title="👁️ The Witness — Status", colour=discord.Colour.gold(), timestamp=utc_now())
    embed.add_field(name="Cached Staff", value=str(len(snapshot.get("staff", {}))), inline=True)
    embed.add_field(name="Matronas", value=str(sum(v.get("category") == "Matrona" for v in leaders.values())), inline=True)
    embed.add_field(name="Reverends", value=str(sum(v.get("category") == "Reverend" for v in leaders.values())), inline=True)
    embed.add_field(name="Group Owner", value=str(sum(v.get("category") == "Owner" for v in leaders.values())), inline=True)
    embed.add_field(name="Promotion Watch", value=f"Every {AUTO_SCAN_MINUTES} min", inline=True)
    embed.add_field(name="Output", value=f"#{CONNECTIONS_CHANNEL}", inline=True)
    embed.set_footer(text="Public Roblox friendship data only")
    await interaction.response.send_message(embed=embed, ephemeral=True)


def rank_change_key(user_id, old_role_id, new_role_id):
    return f"{user_id}:{old_role_id}:{new_role_id}"


def detect_promotion_candidates(old_staff, new_staff):
    events = []
    for user_id, new in new_staff.items():
        old = old_staff.get(user_id)
        if old is None:
            events.append({
                "type": "new_staff",
                "user_id": user_id,
                "old_role_id": None,
                "new_role_id": new.get("role_id"),
                "subject": subject_from_cached_user(user_id, new),
            })
            continue
        if old.get("role_id") == new.get("role_id"):
            continue
        if int(new.get("role_rank", 0)) > int(old.get("role_rank", 0)):
            events.append({
                "type": "promotion",
                "user_id": user_id,
                "old_role_id": old.get("role_id"),
                "new_role_id": new.get("role_id"),
                "subject": subject_from_cached_user(user_id, new),
            })
    return events


def confirm_promotion_events(candidates):
    pending = load_json(PENDING_FILE, {})
    next_pending = {}
    confirmed = []
    for event in candidates:
        key = rank_change_key(event["user_id"], event.get("old_role_id"), event.get("new_role_id"))
        count = int(pending.get(key, {}).get("count", 0)) + 1
        if count >= CONFIRM_SCANS:
            confirmed.append(event)
        else:
            next_pending[key] = {"count": count, "event": event}
    save_json(PENDING_FILE, next_pending)
    return confirmed


@tasks.loop(minutes=AUTO_SCAN_MINUTES)
async def promotion_connection_watch():
    async with scan_lock:
        try:
            previous = load_json(STAFF_CACHE_FILE, {})
            snapshot = await build_snapshot()
            current = snapshot["staff"]

            if not previous:
                save_json(STAFF_CACHE_FILE, current)
                print(f"Witness baseline: {len(current)} staff")
                return

            candidates = detect_promotion_candidates(previous, current)
            confirmed = confirm_promotion_events(candidates)

            baseline_to_save = dict(current)
            confirmed_ids = {e["user_id"] for e in confirmed}
            for event in candidates:
                uid = event["user_id"]
                if uid in confirmed_ids:
                    continue
                if uid in previous:
                    baseline_to_save[uid] = previous[uid]
                else:
                    baseline_to_save.pop(uid, None)
            save_json(STAFF_CACHE_FILE, baseline_to_save)

            if not confirmed:
                return

            leaders = filter_leaders(snapshot["leaders"], "all")
            subjects = [e["subject"] for e in confirmed]
            results = await check_subjects_safely(subjects, leaders)
            connected_results = [r for r in results if r["connections"]]

            if connected_results:
                for guild in bot.guilds:
                    await post_bulk_results(
                        guild,
                        "👁️ PROMOTION CONNECTION WATCH",
                        connected_results,
                        "all",
                        len(subjects),
                    )

            print(
                f"Witness promotion scan: {len(confirmed)} confirmed; "
                f"{len(connected_results)} with leadership connections"
            )
        except Exception as exc:
            print("Promotion connection watcher error:", repr(exc))


@promotion_connection_watch.before_loop
async def before_promotion_connection_watch():
    await bot.wait_until_ready()


synced_once = False


@bot.event
async def on_ready():
    global synced_once
    print(f"The Witness is online as {bot.user}")

    if not synced_once:
        for guild in bot.guilds:
            try:
                bot.tree.clear_commands(guild=guild)
                await bot.tree.sync(guild=guild)
            except Exception as exc:
                print(f"Guild command cleanup error in {guild.name}:", repr(exc))
        try:
            synced = await bot.tree.sync()
            print(f"Synced {len(synced)} global slash commands")
        except Exception as exc:
            print("Global command sync error:", repr(exc))
        synced_once = True

    try:
        await ensure_snapshot()
    except Exception as exc:
        print("Initial Witness snapshot error:", repr(exc))

    if not promotion_connection_watch.is_running():
        promotion_connection_watch.start()


if not TOKEN:
    raise RuntimeError("DISCORD_TOKEN was not found. Check your .env / Railway variable.")

bot.run(TOKEN)
