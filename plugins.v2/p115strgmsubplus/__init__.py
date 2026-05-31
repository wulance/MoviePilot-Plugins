"""
115网盘订阅追更增强版插件
结合MoviePilot订阅功能，自动搜索115网盘资源并转存缺失剧集
"""
import hashlib
import datetime
import random
from pathlib import Path
from threading import Lock
from typing import Optional, Any, List, Dict, Tuple
from urllib.parse import parse_qs, quote, unquote, urlparse

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import text

from app.core.config import settings, global_vars
from app.core.context import TorrentInfo
from app.core.event import Event, eventmanager
from app.db import SessionFactory
from app.db.subscribe_oper import SubscribeOper
from app.db.systemconfig_oper import SystemConfigOper
from app.db.models.site import Site
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import EventType, MediaType, NotificationType, SystemConfigKey

from .clients import PanSouClient, P115ClientManager, NullbrClient
from .handlers import SearchHandler, SyncHandler, SubscribeHandler, ApiHandler
from .ui import UIConfig
from .utils import (
    download_so_file,
    get_hdhive_token_info,
    check_hdhive_cookie_valid,
    refresh_hdhive_cookie_with_playwright,
    hdhive_checkin_api,
    hdhive_checkin_playwright,
)

lock = Lock()


class P115SearchResults:
    """非 list 返回值，用于让 MoviePilot 插件模块短路系统索引器。"""

    def __init__(self, items: Optional[List[TorrentInfo]] = None):
        self._items = items or []

    def __bool__(self):
        return bool(self._items)

    def __iter__(self):
        return iter(self._items)

    def __len__(self):
        return len(self._items)


class P115StrgmSubPlus(_PluginBase):
    """115网盘订阅追更增强版插件"""

    # 插件名称
    plugin_name = "115网盘订阅追更增强版"
    # 插件描述
    plugin_desc = "结合MoviePilot订阅功能，自动搜索115网盘资源并转存缺失的电影和剧集。"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/jxxghp/MoviePilot-Plugins/main/icons/cloud.png"
    # 插件版本
    plugin_version = "1.4.4"
    # 插件作者
    plugin_author = "mrtian2016"
    # 作者主页
    author_url = "https://github.com/mrtian2016"
    # 插件配置项ID前缀
    plugin_config_prefix = "p115strgmsubplus_"
    plugin_order = 20
    auth_level = 1

    # 私有变量
    _scheduler: Optional[BackgroundScheduler] = None
    _toggle_scheduler: Optional[BackgroundScheduler] = None  # 用于延迟切换/窗口切换

    # 配置属性
    _enabled: bool = False
    _onlyonce: bool = False
    _cron: str = "30 2,10,18 * * *"
    _notify: bool = False

    _cookies: str = ""
    _pansou_enabled: bool = True
    _pansou_url: str = "https://so.252035.xyz"
    _pansou_username: str = ""
    _pansou_password: str = ""
    _pansou_auth_enabled: bool = False
    _pansou_channels: str = "QukanMovie"

    _save_path: str = "/我的接收/MoviePilot/TV"
    _movie_save_path: str = "/我的接收/MoviePilot/Movie"
    _only_115: bool = True
    _mp_search_enabled: bool = True
    _exclude_subscribes: List[int] = []

    _nullbr_enabled: bool = False
    _nullbr_appid: str = ""
    _nullbr_api_key: str = ""

    _hdhive_enabled: bool = False
    _hdhive_username: str = ""
    _hdhive_password: str = ""
    _hdhive_cookie: str = ""
    _hdhive_auto_refresh: bool = False
    _hdhive_refresh_before: int = 86400
    _hdhive_query_mode: str = "api"
    _hdhive_api_key: str = ""
    _hdhive_auto_unlock: bool = False
    _hdhive_max_unlock_points: int = 50
    _hdhive_max_points_per_sub: int = 20
    
    # HDHive 签到配置
    _hdhive_checkin_enabled: bool = False
    _hdhive_checkin_mode: str = "api"       # "api" 或 "playwright"
    _hdhive_checkin_type: str = "normal"    # "normal" 或 "gamble"
    _hdhive_checkin_cron: str = "0 8 * * *"

    # 是否屏蔽系统订阅（True=已屏蔽系统订阅，False=已恢复系统订阅）
    _block_system_subscribe: bool = False

    _max_transfer_per_sync: int = 50
    _batch_size: int = 20
    _skip_other_season_dirs: bool = True

    # 窗口配置：站点/延迟/窗口期
    _unblock_site_ids: List[int] = []
    _unblock_site_names: List[str] = []
    _unblock_delay_minutes: int = 5          # -1 禁用触发条件1（并视为禁用窗口）
    _system_subscribe_window_hours: float = 1.0  # 0 禁用窗口

    # 运行时对象
    _pansou_client: Optional[PanSouClient] = None
    _p115_manager: Optional[P115ClientManager] = None
    _nullbr_client: Optional[NullbrClient] = None
    _hdhive_client: Optional[Any] = None

    # 处理器
    _search_handler: Optional[SearchHandler] = None
    _subscribe_handler: Optional[SubscribeHandler] = None
    _sync_handler: Optional[SyncHandler] = None
    _api_handler: Optional[ApiHandler] = None

    _MIN_INTERVAL_HOURS: int = 8
    _MP_SEARCH_SITE_ID: int = -115
    _MP_SEARCH_SITE_NAME: str = "115网盘"
    _MP_SEARCH_DOMAIN: str = "p115strgmsubplus.local"
    _MP_MAGNET_PREFIX: str = "magnet:?xt=urn:btih:"

    # ------------------ 调度器 ------------------

    def _ensure_toggle_scheduler(self):
        if not self._toggle_scheduler:
            self._toggle_scheduler = BackgroundScheduler(timezone=settings.TZ)
            self._toggle_scheduler.start()

    def _cancel_toggle_jobs(self):
        if not self._toggle_scheduler:
            return
        for job_id in ["p115_unblock_job", "p115_reblock_job"]:
            try:
                self._toggle_scheduler.remove_job(job_id)
            except Exception:
                pass

    # ------------------ cron间隔校验 ------------------

    @staticmethod
    def _cron_interval_ge_min_hours(cron_expr: str, min_hours: int) -> bool:
        cron_expr = (cron_expr or "").strip()
        if not cron_expr:
            return False
        try:
            tz = pytz.timezone(settings.TZ)
            trigger = CronTrigger.from_crontab(cron_expr, timezone=tz)
        except Exception:
            return False

        now = datetime.datetime.now(tz=pytz.timezone(settings.TZ))
        fire_times: List[datetime.datetime] = []
        prev = None
        current = now
        for _ in range(12):
            nxt = trigger.get_next_fire_time(prev, current)
            if not nxt:
                break
            fire_times.append(nxt)
            prev = nxt
            current = nxt + datetime.timedelta(seconds=1)

        if len(fire_times) < 2:
            return True

        min_delta = min(fire_times[i + 1] - fire_times[i] for i in range(len(fire_times) - 1))
        return min_delta >= datetime.timedelta(hours=min_hours)

    # ------------------ 站点解析 ------------------

    def _load_site_records(self) -> List[Dict[str, Any]]:
        with SessionFactory() as db:
            rows = db.execute(text("SELECT id, name, is_active FROM site")).fetchall()
        out = []
        for r in rows:
            out.append({"id": int(r[0]), "name": str(r[1]), "is_active": bool(r[2])})
        return out

    def _resolve_site_ids(self, ids: Optional[List[int]] = None, names: Optional[List[str]] = None) -> List[int]:
        ids = ids or []
        names = names or []

        site_records = self._load_site_records()
        by_name = {s["name"]: s for s in site_records}
        by_id = {s["id"]: s for s in site_records}

        final_ids: List[int] = []
        for sid in ids:
            if sid in by_id:
                final_ids.append(sid)
            else:
                logger.warning(f"站点ID不存在：id={sid}（将跳过）")

        for nm in names:
            rec = by_name.get(nm)
            if not rec:
                logger.warning(f"站点名称不存在：name={nm}（将跳过）")
                continue
            final_ids.append(int(rec["id"]))

        seen = set()
        uniq = []
        for x in final_ids:
            if x not in seen:
                seen.add(x)
                uniq.append(x)

        mapped = []
        for x in uniq:
            rec = by_id.get(x, {})
            mapped.append(f"{rec.get('name','?')}({x})")
        logger.info(f"订阅站点解析结果：ids={uniq} | 映射={mapped}")
        return uniq

    def _ensure_115_site_id(self, db=None) -> int:
        """
        确保 115网盘 站点存在并返回 ID
        :param db: 可选的数据库会话，若未传入则创建新会话
        """
        def _do_ensure(session):
            row = session.execute(text("SELECT id FROM site WHERE name=:n LIMIT 1"), {"n": "115网盘"}).fetchone()
            if row and row[0] is not None:
                return int(row[0])

            # existing = Site.get(session, -1)
            row_ex = session.execute(text("SELECT id FROM site WHERE id=:i"), {"i": -1}).fetchone()
            if not row_ex:
                session.execute(
                    text(
                        "INSERT INTO site (id, name, url, is_active, limit_interval, limit_count, limit_seconds, timeout) "
                        "VALUES (:id, :name, :url, :is_active, :limit_interval ,:limit_count, :limit_seconds, :timeout)"
                    ),
                    {
                        "id": -1,
                        "name": "115网盘",
                        "url": "https://115.com",
                        "is_active": True,
                        "limit_interval": 10000000,
                        "limit_count": 1,
                        "limit_seconds": 10000000,
                        "timeout": 1
                    }
                )
                session.commit()
                logger.info("已插入站点记录：115网盘(id=-1)")
            return -1

        if db is not None:
            return _do_ensure(db)
        else:
            with SessionFactory() as new_db:
                return _do_ensure(new_db)

    def _apply_sites_to_all_subscribes(self, site_ids: List[int], reason: str):
        """ 应用站点ID到所有订阅 """
        exclude_ids = set(self._exclude_subscribes or [])
        with SessionFactory() as db:
            # 复用 SubscribeOper 实例，避免循环中重复创建
            subscribe_oper = SubscribeOper(db=db)
            subs = subscribe_oper.list() or []
            updated = 0
            excluded = 0
            for s in subs:
                if s.id in exclude_ids:
                    excluded += 1
                    continue
                subscribe_oper.update(s.id, {"sites": site_ids})
                updated += 1
        logger.info(f"{reason}：已更新 {updated} 个订阅（跳过 {excluded} 个排除订阅）")

    # ------------------ 禁用窗口判断 ------------------

    def _window_disabled(self) -> bool:
        # 站点空 / 窗口=0 / delay=-1 => 始终保持屏蔽，不安排任何进入已恢复状态任务
        if not self._unblock_site_names:
            return True
        if float(self._system_subscribe_window_hours or 0) <= 0:
            return True
        if int(self._unblock_delay_minutes) < 0:
            return True
        return False

    def _window_enabled(self) -> bool:
        return not self._window_disabled()

    # ------------------ 系统默认订阅站点：只在已恢复系统订阅时尝试 ------------------

    def _try_set_default_sites_for_unblocked(self, site_ids: List[int]):
        """
        只在“已恢复系统订阅”时尝试设置系统默认订阅站点为窗口站点。
        若系统不存在对应key，会静默失败，不影响订阅 sites 已更新。
        """
        try:
            from app.db.systemconfig_oper import SystemConfigOper
        except Exception:
            return

        def _build_oper(db):
            try:
                return SystemConfigOper(db)
            except Exception:
                try:
                    return SystemConfigOper(db=db)
                except Exception:
                    return None

        candidate_keys = [
            "subscribe_sites",
            "subscribe_site_ids",
            "system_subscribe_sites",
            "system_subscribe_site_ids",
            "subscribe_sites_selected",
        ]

        with SessionFactory() as db:
            oper = _build_oper(db)
            if not oper:
                return
            get_fn = getattr(oper, "get", None) or getattr(oper, "get_by_key", None)
            set_fn = getattr(oper, "set", None) or getattr(oper, "set_by_key", None)
            if not get_fn or not set_fn:
                return

            for k in candidate_keys:
                try:
                    cur = get_fn(k)
                except Exception:
                    cur = None
                if cur is None:
                    continue
                try:
                    set_fn(k, site_ids)
                    logger.info(f"已恢复系统订阅：已尝试同步默认订阅站点 key={k} value={site_ids}")
                    break
                except Exception:
                    continue

    # ------------------ 两态切换（日志统一） ------------------

    def _enter_blocked(self, reason: str):
        """
        已屏蔽系统订阅：
        - 全量订阅 sites=仅115
        - 不再尝试设置屏蔽态默认站点=115（依赖 SubscribeAdded 兜底）
        - 取消所有窗口任务
        """
        self._ensure_toggle_scheduler()
        self._cancel_toggle_jobs()
        self._init_subscribe_handler()

        self._subscribe_handler.set_blocked_sites_only_115()
        self._block_system_subscribe = True
        self.__update_config()
        logger.info(f"已屏蔽系统订阅（仅115网盘）：{reason}")

    def _enter_unblocked(self, reason: str):
        """
        已恢复系统订阅：
        - 全量订阅 sites=UI站点
        - 尽力设置系统默认订阅站点=UI站点（若存在key）
        - 从进入时刻计窗口，到期切回屏蔽
        """
        if not self._window_enabled():
            self._block_system_subscribe = True
            self.__update_config()
            self._enter_blocked(reason=f"{reason}（窗口禁用）")
            return

        self._ensure_toggle_scheduler()
        self._cancel_toggle_jobs()
        self._init_subscribe_handler()

        site_ids = self._resolve_site_ids(ids=self._unblock_site_ids, names=self._unblock_site_names)
        if not site_ids:
            self._block_system_subscribe = True
            self.__update_config()
            self._enter_blocked(reason=f"{reason}（站点解析失败）")
            return

        self._apply_sites_to_all_subscribes(site_ids, reason="已恢复系统订阅：全量同步站点")
        self._try_set_default_sites_for_unblocked(site_ids)

        self._block_system_subscribe = False
        self.__update_config()
        logger.info(f"已恢复系统订阅：站点={self._unblock_site_names} 窗口期={self._system_subscribe_window_hours}h（{reason}）")

        self._schedule_reblock_after_window()

    def _schedule_reblock_after_window(self):
        hours = float(self._system_subscribe_window_hours or 0)
        if hours <= 0:
            return

        tz = pytz.timezone(settings.TZ)
        now = datetime.datetime.now(tz=tz)
        run_date = now + datetime.timedelta(hours=hours)

        self._toggle_scheduler.add_job(
            func=lambda: self._enter_blocked(reason="窗口到期"),
            trigger="date",
            run_date=run_date,
            id="p115_reblock_job",
            replace_existing=True
        )
        logger.info(f"已安排：{run_date} 切换为已屏蔽系统订阅（仅115网盘）")

    def _schedule_unblock_after_delay(self, base_time: datetime.datetime):
        delay = int(self._unblock_delay_minutes)
        if delay < 0:
            return
        if not self._window_enabled():
            return

        self._ensure_toggle_scheduler()
        self._cancel_toggle_jobs()

        tz = pytz.timezone(settings.TZ)
        base_time = base_time.astimezone(tz)
        run_date = base_time + datetime.timedelta(minutes=delay)

        self._toggle_scheduler.add_job(
            func=lambda: self._enter_unblocked(reason="触发条件1：最后一次任务"),
            trigger="date",
            run_date=run_date,
            id="p115_unblock_job",
            replace_existing=True
        )
        logger.info(f"已安排：{run_date} 切换为已恢复系统订阅（延迟={delay}min）")

    # ------------------ 触发条件1：最后一次任务判断 ------------------

    def _is_last_run_today(self, run_start: datetime.datetime) -> bool:
        """判断当前运行是否是今天的最后一次任务"""
        try:
            tz = pytz.timezone(settings.TZ)
            run_start = run_start.astimezone(tz)
            trigger = CronTrigger.from_crontab(self._cron, timezone=tz)
            nxt = trigger.get_next_fire_time(None, run_start + datetime.timedelta(seconds=1))
            if not nxt:
                logger.debug(f"判断最后一次任务：无下次触发时间，返回 False")
                return False
            is_last = nxt.date() != run_start.date()
            logger.debug(f"判断最后一次任务：当前={run_start.strftime('%Y-%m-%d %H:%M')}, 下次={nxt.strftime('%Y-%m-%d %H:%M')}, 是否最后一次={is_last}")
            return is_last
        except Exception as e:
            logger.warning(f"判断是否当天最后一次触发失败：{e}，按 23:00 兜底")
            return run_start.hour == 23 and run_start.minute == 00

    # ------------------ 事件兜底：SubscribeAdded 保留，SubscribeModified 禁用写入 ------------------

    def _get_subscribe_id_from_event(self, event: Event) -> Optional[int]:
        if not event or not event.event_data:
            return None
        data = event.event_data or {}
        subscribe_id = data.get("subscribe_id") or data.get("id")
        if not subscribe_id and isinstance(data.get("subscribe"), dict):
            subscribe_id = data["subscribe"].get("id")
        try:
            return int(subscribe_id) if subscribe_id is not None else None
        except Exception:
            return None

    @eventmanager.register(EventType.SubscribeAdded)
    def on_subscribe_added(self, event: Event):
        """
        保留：新订阅兜底
        - 已屏蔽系统订阅时：新订阅必拉回仅115
        - 已恢复系统订阅时：新订阅同步窗口站点（保持一致）
        """
        sid = self._get_subscribe_id_from_event(event)
        if not sid:
            return
        try:
            self._init_subscribe_handler()

            if self._block_system_subscribe:
                if hasattr(self._subscribe_handler, "set_sites_for_subscribe_only_115"):
                    self._subscribe_handler.set_sites_for_subscribe_only_115(sid)
                else:
                    # 兜底：使用统一的 db session
                    with SessionFactory() as db:
                        site_id_115 = self._ensure_115_site_id(db)
                        SubscribeOper(db=db).update(sid, {"sites": [site_id_115]})
                logger.info(f"已屏蔽系统订阅：新增订阅已拉回仅115（subscribe_id={sid}）")
            else:
                if self._window_enabled() and hasattr(self._subscribe_handler, "set_sites_for_subscribe_by_names"):
                    self._subscribe_handler.set_sites_for_subscribe_by_names(sid, self._unblock_site_names)
                    logger.info(f"已恢复系统订阅：新增订阅已同步窗口站点（subscribe_id={sid})")

        except Exception as e:
            logger.error(f"SubscribeAdded 兜底失败：{e}")

    @eventmanager.register(EventType.SubscribeModified)
    def on_subscribe_modified(self, event: Event):
        """
        禁用：不再对 subscribe.modified 做拉回写入
        目的：用户手动修改订阅站点时，不再被自动拉回仅115
        """
        sid = self._get_subscribe_id_from_event(event)
        if not sid:
            return
        if self._block_system_subscribe:
            logger.info(f"已屏蔽系统订阅：检测到订阅改动，按规则不自动拉回（subscribe_id={sid}）")
        return

    # ------------------ HDHive cookie（保留） ------------------

    def _check_and_refresh_hdhive_cookie(self) -> Optional[str]:
        if not self._hdhive_auto_refresh:
            return self._hdhive_cookie if self._hdhive_cookie else None

        if not self._hdhive_username or not self._hdhive_password:
            logger.warning("HDHive: 已启用自动刷新但未配置用户名/密码，无法刷新 Cookie")
            return self._hdhive_cookie if self._hdhive_cookie else None

        if self._hdhive_cookie:
            is_valid, reason = check_hdhive_cookie_valid(self._hdhive_cookie, self._hdhive_refresh_before)
            if is_valid:
                logger.info(f"HDHive: Cookie 检查通过 - {reason}")
                return self._hdhive_cookie
            else:
                logger.info(f"HDHive: Cookie 需要刷新 - {reason}")
        else:
            logger.info("HDHive: 未配置 Cookie，尝试登录获取")

        logger.info("HDHive: 开始刷新 Cookie...")
        new_cookie = refresh_hdhive_cookie_with_playwright(self._hdhive_username, self._hdhive_password)

        if new_cookie:
            token_info = get_hdhive_token_info(new_cookie)
            if token_info:
                logger.info(
                    f"HDHive: 新 Cookie 信息 - 用户ID: {token_info['user_id']}, "
                    f"过期时间: {token_info['exp_time'].strftime('%Y-%m-%d %H:%M:%S')}"
                )
            self._hdhive_cookie = new_cookie
            self.__update_config()
            logger.info("HDHive: Cookie 刷新成功并已保存到配置")
            return new_cookie

        logger.error("HDHive: Cookie 刷新失败")
        return self._hdhive_cookie if self._hdhive_cookie else None

    # ------------------ HDHive 签到 ------------------

    # ------------------ HDHive 签到 ------------------

    def _do_hdhive_checkin(self):
        """执行 HDHive 签到"""
        if not self._hdhive_checkin_enabled:
            return

        mode = self._hdhive_checkin_mode
        checkin_type = self._hdhive_checkin_type
        type_label = "赌狗签到" if checkin_type == "gamble" else "每日签到"
        mode_label = "Playwright" if mode == "playwright" else "API"

        logger.info(f"HDHive 签到: 开始执行 [{mode_label}] [{type_label}]")

        try:
            if mode == "playwright":
                if not self._hdhive_username or not self._hdhive_password:
                    logger.warning("HDHive 签到: Playwright 模式未配置用户名/密码")
                    return
                result = hdhive_checkin_playwright(
                    username=self._hdhive_username,
                    password=self._hdhive_password,
                    cookie=self._hdhive_cookie,
                    checkin_type=checkin_type,
                )
            else:
                # API 模式：优先使用 API Key，否则使用 Cookie
                if self._hdhive_api_key:
                    result = hdhive_checkin_api(
                        api_key=self._hdhive_api_key,
                        checkin_type=checkin_type,
                    )
                else:
                    cookie = self._check_and_refresh_hdhive_cookie()
                    if not cookie:
                        logger.warning("HDHive 签到: API 模式无可用 API Key 或 Cookie")
                        return
                    result = hdhive_checkin_api(
                        cookie=cookie,
                        checkin_type=checkin_type,
                    )

            # 日志
            if result["success"]:
                points_info = f"，积分: +{result['points']}" if result.get("points") else ""
                multiplier_info = f"（{result['multiplier']}x翻倍）" if result.get("multiplier") and result["multiplier"] > 1 else ""
                logger.info(f"HDHive 签到成功: {result['message']}{points_info}{multiplier_info}")
            else:
                logger.warning(f"HDHive 签到失败: {result['message']}")

            # 签到通知始终发送（不受 self._notify 控制）
            status = "✅ 成功" if result.get("success") else "❌ 失败"
            text_parts = [f"模式: {mode_label}", f"结果: {result.get('message', '未知')}"]
            if result.get("points"):
                text_parts.append(f"积分: +{result['points']}")
            if result.get("multiplier") and result["multiplier"] > 1:
                text_parts.append(f"倍率: {result['multiplier']}x（Premium）")
            self.post_message(
                mtype=NotificationType.Plugin,
                title=f"【HDHive {type_label}】{status}",
                text="\n".join(text_parts),
            )

        except Exception as e:
            logger.error(f"HDHive 签到异常: {e}", exc_info=True)
            self.post_message(
                mtype=NotificationType.Plugin,
                title=f"【HDHive {type_label}】❌ 异常",
                text=f"签到执行异常: {e}",
            )

    # ------------------ init_plugin ------------------

    def init_plugin(self, config: dict = None):
        self.stop_service()
        self._ensure_toggle_scheduler()
        download_so_file(Path(__file__).parent / "lib")

        if config:
            self._enabled = config.get("enabled", False)

            self._cron = (config.get("cron", self._cron) or "").strip()
            if self._cron:
                ok = self._cron_interval_ge_min_hours(self._cron, self._MIN_INTERVAL_HOURS)
                if not ok:
                    logger.warning(
                        f"Cron 过于频繁（要求间隔>= {self._MIN_INTERVAL_HOURS}h）：{self._cron}，已回退默认 30 */8 * * *"
                    )
                    self._cron = "30 */8 * * *"

            self._notify = config.get("notify", False)
            self._onlyonce = config.get("onlyonce", False)
            self._cookies = config.get("cookies", "")

            self._pansou_enabled = config.get("pansou_enabled", True)
            self._pansou_url = config.get("pansou_url", "https://so.252035.xyz/")
            self._pansou_username = config.get("pansou_username", "")
            self._pansou_password = config.get("pansou_password", "")
            self._pansou_auth_enabled = config.get("pansou_auth_enabled", False)
            self._pansou_channels = config.get("pansou_channels", "QukanMovie")

            self._save_path = config.get("save_path", "/我的接收/MoviePilot/TV")
            self._movie_save_path = config.get("movie_save_path", "/我的接收/MoviePilot/Movie")
            self._only_115 = config.get("only_115", True)
            self._mp_search_enabled = config.get("mp_search_enabled", True)
            self._exclude_subscribes = config.get("exclude_subscribes", []) or []

            self._nullbr_enabled = config.get("nullbr_enabled", False)
            self._nullbr_appid = config.get("nullbr_appid", "")
            self._nullbr_api_key = config.get("nullbr_api_key", "")

            self._hdhive_enabled = config.get("hdhive_enabled", False)
            self._hdhive_query_mode = config.get("hdhive_query_mode", "api")
            self._hdhive_api_key = config.get("hdhive_api_key", "")
            self._hdhive_auto_unlock = config.get("hdhive_auto_unlock", False)
            self._hdhive_max_unlock_points = int(config.get("hdhive_max_unlock_points", 50) or 50)
            self._hdhive_max_points_per_sub = int(config.get("hdhive_max_points_per_sub", 20) or 20)
            self._hdhive_username = config.get("hdhive_username", "")
            self._hdhive_password = config.get("hdhive_password", "")
            self._hdhive_cookie = config.get("hdhive_cookie", "")
            self._hdhive_auto_refresh = config.get("hdhive_auto_refresh", False)
            self._hdhive_refresh_before = int(config.get("hdhive_refresh_before", 86400) or 86400)
            self._hdhive_checkin_enabled = config.get("hdhive_checkin_enabled", False)
            self._hdhive_checkin_mode = config.get("hdhive_checkin_mode", "api")
            # UI 使用 hdhive_checkin_gambler (bool)，转换为内部 hdhive_checkin_type (string)
            self._hdhive_checkin_type = "gamble" if config.get("hdhive_checkin_gambler", False) else "normal"
            # 签到时间每次初始化随机生成（6~9点随机分钟），避免固定时间触发风控
            self._hdhive_checkin_cron = f"{random.randint(0, 59)} {random.randint(6, 9)} * * *"

            self._max_transfer_per_sync = int(config.get("max_transfer_per_sync", 50) or 50)
            self._batch_size = int(config.get("batch_size", 20) or 20)
            self._skip_other_season_dirs = config.get("skip_other_season_dirs", True)

            # UI新增配置
            self._unblock_site_ids = config.get("unblock_site_ids", []) or []
            raw_sites = config.get("unblock_site_names", self._unblock_site_names)
            if isinstance(raw_sites, str):
                self._unblock_site_names = [x.strip() for x in raw_sites.split(",") if x.strip()]
            else:
                self._unblock_site_names = raw_sites or []

            self._unblock_delay_minutes = int(config.get("unblock_delay_minutes", self._unblock_delay_minutes))
            self._system_subscribe_window_hours = float(
                config.get("unblock_window_hours", config.get("system_subscribe_window_hours", self._system_subscribe_window_hours))
            )

            self._block_system_subscribe = bool(config.get("block_system_subscribe", False))

        # 初始化客户端/handlers
        self._init_clients()
        self._init_handlers()
        self._sync_mp_search_site()

        # 配置立即生效
        if self._block_system_subscribe:
            self._enter_blocked(reason="配置应用")
        else:
            # 用户手动关闭屏蔽：应用站点并取消窗口任务（不自动回弹）
            self._cancel_toggle_jobs()
            if self._unblock_site_names:
                site_ids = self._resolve_site_ids(ids=self._unblock_site_ids, names=self._unblock_site_names)
                if site_ids:
                    self._apply_sites_to_all_subscribes(site_ids, reason="用户关闭屏蔽：全量同步站点")
                    self._try_set_default_sites_for_unblocked(site_ids)
            self.__update_config()
            logger.info("用户已关闭屏蔽系统订阅（配置应用）")

        # 立即运行一次
        if self._enabled or self._onlyonce:
            if self._onlyonce:
                self._scheduler = BackgroundScheduler(timezone=settings.TZ)
                self._scheduler.add_job(
                    func=self.sync_subscribes,
                    trigger='date',
                    run_date=datetime.datetime.now(tz=pytz.timezone(settings.TZ)) + datetime.timedelta(seconds=3)
                )
                if self._scheduler.get_jobs():
                    self._scheduler.start()

            if self._onlyonce:
                self._onlyonce = False
                self.__update_config()

    # ------------------ init clients/handlers ------------------

    def _init_clients(self):
        """初始化客户端"""
        proxy = settings.PROXY
        if proxy:
            logger.info(f"使用 MoviePilot PROXY: {proxy}")

        if self._pansou_enabled and self._pansou_url:
            self._pansou_client = PanSouClient(
                base_url=self._pansou_url,
                username=self._pansou_username,
                password=self._pansou_password,
                auth_enabled=self._pansou_auth_enabled,
                proxy=proxy
            )

        if self._nullbr_enabled:
            if not self._nullbr_appid or not self._nullbr_api_key:
                missing = []
                if not self._nullbr_appid:
                    missing.append("APP ID")
                if not self._nullbr_api_key:
                    missing.append("API Key")
                logger.warning(f"Nullbr 已启用但缺少必要配置：{', '.join(missing)}，将无法使用 Nullbr 查询功能")
                self._nullbr_client = None
            else:
                self._nullbr_client = NullbrClient(app_id=self._nullbr_appid, api_key=self._nullbr_api_key, proxy=proxy)
                logger.info("Nullbr 客户端初始化成功")

        # HDHive 客户端初始化（仅 Playwright 模式搜索时动态创建客户端，API 模式直接在 search_hdhive 中请求）
        if self._hdhive_enabled:
            # Playwright 测试用户名密码，API 测试 api_key
            if self._hdhive_query_mode == "playwright" and (not self._hdhive_username or not self._hdhive_password):
                logger.warning("HDHive (Playwright 模式) 已启用但未配置用户名和密码，将无法使用 HDHive 查询功能")
                self._hdhive_client = None
            elif self._hdhive_query_mode == "api" and not self._hdhive_api_key:
                logger.warning("HDHive (API 模式) 已启用但未配置 API Key，将无法使用 HDHive 查询功能")
                self._hdhive_client = None
            else:
                logger.info(f"HDHive 配置已加载（模式：{self._hdhive_query_mode}）")
                self._hdhive_client = None

        if self._cookies:
            self._p115_manager = P115ClientManager(cookies=self._cookies)

    def _init_subscribe_handler(self):
        self._subscribe_handler = SubscribeHandler(
            exclude_subscribes=self._exclude_subscribes,
            notify=self._notify,
            post_message_func=self.post_message
        )

    def _init_handlers(self):
        self._init_subscribe_handler()

        self._search_handler = SearchHandler(
            pansou_client=self._pansou_client,
            nullbr_client=self._nullbr_client,
            hdhive_client=self._hdhive_client,
            pansou_enabled=self._pansou_enabled,
            nullbr_enabled=self._nullbr_enabled,
            hdhive_enabled=self._hdhive_enabled,
            hdhive_query_mode=self._hdhive_query_mode,
            hdhive_api_key=self._hdhive_api_key,
            hdhive_auto_unlock=self._hdhive_auto_unlock,
            hdhive_max_unlock_points=self._hdhive_max_unlock_points,
            hdhive_max_points_per_sub=self._hdhive_max_points_per_sub,
            hdhive_username=self._hdhive_username,
            hdhive_password=self._hdhive_password,
            hdhive_cookie=self._hdhive_cookie,
            only_115=self._only_115,
            pansou_channels=self._pansou_channels
        )
        # 设置持久化函数，用于保存订阅的历史积分花费
        self._search_handler.set_data_funcs(self.get_data, self.save_data)

        self._sync_handler = SyncHandler(
            p115_manager=self._p115_manager,
            search_handler=self._search_handler,
            subscribe_handler=self._subscribe_handler,
            chain=self.chain,
            save_path=self._save_path,
            movie_save_path=self._movie_save_path,
            max_transfer_per_sync=self._max_transfer_per_sync,
            batch_size=self._batch_size,
            skip_other_season_dirs=self._skip_other_season_dirs,
            notify=self._notify,
            post_message_func=self.post_message,
            get_data_func=self.get_data,
            save_data_func=self.save_data
        )

        self._api_handler = ApiHandler(
            pansou_client=self._pansou_client,
            p115_manager=self._p115_manager,
            only_115=self._only_115,
            save_path=self._save_path,
            get_data_func=self.get_data,
            save_data_func=self.save_data
        )

    # ------------------ MoviePilot 搜索接入 ------------------

    def _sync_mp_search_site(self):
        """把 115 网盘注册为 MoviePilot 搜索页可选的虚拟索引站点。"""
        if not self._enabled or not self._mp_search_enabled:
            self._remove_mp_search_site_from_selected()
            return

        try:
            from app.helper.sites import SitesHelper  # noqa

            indexer = {
                "id": self._MP_SEARCH_SITE_ID,
                "name": self._MP_SEARCH_SITE_NAME,
                "domain": f"https://{self._MP_SEARCH_DOMAIN}/",
                "url": f"https://{self._MP_SEARCH_DOMAIN}/",
                "parser": "P115StrgmSubPlus",
                "public": True,
                "pri": 0,
                "proxy": False,
                "language": "zh",
                "result_num": 20,
                "timeout": 120,
            }
            sites_helper = SitesHelper()
            if hasattr(sites_helper, "add_indexer"):
                sites_helper.add_indexer(self._MP_SEARCH_DOMAIN, indexer)
            elif hasattr(sites_helper, "_indexers"):
                sites_helper._indexers[self._MP_SEARCH_DOMAIN] = indexer
            self._add_mp_search_site_to_selected()
            logger.info("已接入 MoviePilot 搜索：115网盘")
        except Exception as e:
            logger.warning(f"接入 MoviePilot 搜索失败：{e}")

    def _add_mp_search_site_to_selected(self):
        """如果用户配置了搜索站点白名单，自动追加 115 虚拟站点。"""
        try:
            selected_sites = SystemConfigOper().get(SystemConfigKey.IndexerSites) or []
            if selected_sites and self._MP_SEARCH_SITE_ID not in selected_sites:
                selected_sites.append(self._MP_SEARCH_SITE_ID)
                SystemConfigOper().set(SystemConfigKey.IndexerSites, selected_sites)
                logger.info("已将 115网盘 加入 MoviePilot 搜索站点")
        except Exception as e:
            logger.warning(f"更新 MoviePilot 搜索站点失败：{e}")

    def _remove_mp_search_site_from_selected(self):
        """关闭 MP 搜索接入时，从已选搜索站点中移除虚拟站点。"""
        try:
            selected_sites = SystemConfigOper().get(SystemConfigKey.IndexerSites) or []
            if self._MP_SEARCH_SITE_ID in selected_sites:
                selected_sites.remove(self._MP_SEARCH_SITE_ID)
                SystemConfigOper().set(SystemConfigKey.IndexerSites, selected_sites)
        except Exception as e:
            logger.warning(f"移除 MoviePilot 115 搜索站点失败：{e}")

    def _is_mp_search_site(self, site: Optional[Dict[str, Any]]) -> bool:
        if not site:
            return False
        return site.get("id") == self._MP_SEARCH_SITE_ID or site.get("parser") == "P115StrgmSubPlus"

    def _build_p115_magnet(self, share_url: str, title: str, mtype: Optional[MediaType] = None) -> str:
        digest = hashlib.sha1(share_url.encode("utf-8")).hexdigest()
        save_path = self._movie_save_path if mtype == MediaType.MOVIE else self._save_path
        return (
            f"{self._MP_MAGNET_PREFIX}{digest}"
            f"&dn={quote(title or self._MP_SEARCH_SITE_NAME)}"
            f"&x.p115={quote(share_url, safe='')}"
            f"&x.save={quote(save_path or '')}"
        )

    def _parse_p115_magnet(self, content: Any) -> Tuple[Optional[str], Optional[str]]:
        if not isinstance(content, str) or not content.startswith(self._MP_MAGNET_PREFIX):
            return None, None
        query = parse_qs(urlparse(content).query)
        share_url = unquote((query.get("x.p115") or [""])[0])
        save_path = unquote((query.get("x.save") or [""])[0])
        return share_url, save_path

    def _search_p115_for_mp(self, keyword: str, mtype: Optional[MediaType] = None) -> List[TorrentInfo]:
        if not self._mp_search_enabled:
            return []
        if not self._pansou_client:
            logger.warning("PanSou 客户端未初始化，无法接入 MoviePilot 搜索")
            return []
        if not keyword or not keyword.strip():
            return []

        channels = None
        if self._pansou_channels and self._pansou_channels.strip():
            channels = [ch.strip() for ch in self._pansou_channels.split(",") if ch.strip()]
        cloud_types = ["115"] if self._only_115 else None
        search_result = self._pansou_client.search(
            keyword=keyword.strip(),
            cloud_types=cloud_types,
            channels=channels,
            limit=20,
        )
        if not search_result or search_result.get("error"):
            logger.warning(f"MoviePilot 搜索 115 资源失败：{(search_result or {}).get('error')}")
            return []

        resources = (search_result.get("results") or {}).get("115网盘", [])
        torrents: List[TorrentInfo] = []
        for resource in resources:
            share_url = resource.get("url")
            title = resource.get("title") or keyword
            if not share_url:
                continue
            torrent = TorrentInfo(
                site=self._MP_SEARCH_SITE_ID,
                site_name=self._MP_SEARCH_SITE_NAME,
                site_order=0,
                title=title,
                description=f"115网盘分享链接：{share_url}",
                enclosure=self._build_p115_magnet(share_url, title, mtype),
                page_url=share_url,
                pubdate=resource.get("update_time") or "",
                seeders=999,
                peers=0,
                grabs=0,
                size=0,
                uploadvolumefactor=1.0,
                downloadvolumefactor=0.0,
                category=mtype.value if isinstance(mtype, MediaType) else None,
                labels=["115网盘", "插件转存"],
            )
            torrents.append(torrent)

        logger.info(f"MoviePilot 搜索 115 资源完成：{keyword}，返回 {len(torrents)} 条")
        return torrents

    def mp_search_page_size(self, site: dict, keyword: Optional[str] = None):
        if self._is_mp_search_site(site):
            return 20
        return None

    def mp_search_torrents(
            self,
            site: dict,
            keyword: str = None,
            mtype: Optional[MediaType] = None,
            page: Optional[int] = 0,
            **kwargs
    ):
        if not self._is_mp_search_site(site):
            return None
        if page and int(page or 0) > 0:
            return P115SearchResults()
        return P115SearchResults(self._search_p115_for_mp(keyword=keyword, mtype=mtype))

    async def async_mp_search_torrents(self, site: dict, keyword: str = None,
                                       mtype: Optional[MediaType] = None,
                                       page: Optional[int] = 0, **kwargs):
        return self.mp_search_torrents(site=site, keyword=keyword, mtype=mtype, page=page, **kwargs)

    def mp_download(self, content: Any, **kwargs):
        """把 115 虚拟磁力链接转换为 115 转存，并向 MoviePilot 返回一个伪下载 ID。"""
        share_url, save_path = self._parse_p115_magnet(content)
        if not share_url:
            return None
        if not self._p115_manager:
            return "P115StrgmSubPlus", None, None, "115 客户端未初始化"

        target_path = save_path or self._save_path
        success = self._p115_manager.transfer_share(share_url, target_path)
        if not success:
            return "P115StrgmSubPlus", None, None, "115 转存失败"

        fake_hash = f"p115-{hashlib.sha1(f'{share_url}|{target_path}'.encode('utf-8')).hexdigest()[:32]}"
        logger.info(f"MoviePilot 手动下载已转为 115 转存：{share_url} => {target_path}")
        return "P115StrgmSubPlus", fake_hash, "NoSubfolder", None

    def mp_download_added(self, context: Any, **kwargs):
        torrent = getattr(context, "torrent_info", None)
        if torrent and torrent.site == self._MP_SEARCH_SITE_ID:
            return "P115StrgmSubPlus"
        return None

    # ------------------ 配置写回 ------------------

    def __update_config(self):
        self.update_config({
            "enabled": self._enabled,
            "cron": self._cron,
            "notify": self._notify,
            "onlyonce": self._onlyonce,
            "only_115": self._only_115,
            "mp_search_enabled": self._mp_search_enabled,
            "save_path": self._save_path,
            "movie_save_path": self._movie_save_path,
            "cookies": self._cookies,
            "pansou_enabled": self._pansou_enabled,
            "pansou_url": self._pansou_url,
            "pansou_username": self._pansou_username,
            "pansou_password": self._pansou_password,
            "pansou_auth_enabled": self._pansou_auth_enabled,
            "pansou_channels": self._pansou_channels,
            "nullbr_enabled": self._nullbr_enabled,
            "nullbr_appid": self._nullbr_appid,
            "nullbr_api_key": self._nullbr_api_key,
            # HDHive 配置
            "hdhive_enabled": self._hdhive_enabled,
            "hdhive_query_mode": self._hdhive_query_mode,
            "hdhive_api_key": self._hdhive_api_key,
            "hdhive_auto_unlock": self._hdhive_auto_unlock,
            "hdhive_max_unlock_points": self._hdhive_max_unlock_points,
            "hdhive_max_points_per_sub": self._hdhive_max_points_per_sub,
            "hdhive_username": self._hdhive_username,
            "hdhive_password": self._hdhive_password,
            "hdhive_cookie": self._hdhive_cookie,
            "hdhive_auto_refresh": self._hdhive_auto_refresh,
            "hdhive_refresh_before": self._hdhive_refresh_before,
            # HDHive 签到
            "hdhive_checkin_enabled": self._hdhive_checkin_enabled,
            "hdhive_checkin_mode": self._hdhive_checkin_mode,
            "hdhive_checkin_type": self._hdhive_checkin_type,
            "hdhive_checkin_gambler": self._hdhive_checkin_type == "gamble",
            "hdhive_checkin_cron": self._hdhive_checkin_cron,
            # 其他配置
            "exclude_subscribes": self._exclude_subscribes,
            "block_system_subscribe": self._block_system_subscribe,
            "max_transfer_per_sync": self._max_transfer_per_sync,
            "batch_size": self._batch_size,
            "skip_other_season_dirs": self._skip_other_season_dirs,
            "unblock_site_ids": self._unblock_site_ids,
            "unblock_site_names": self._unblock_site_names,
            "unblock_delay_minutes": self._unblock_delay_minutes,
            "system_subscribe_window_hours": self._system_subscribe_window_hours,
            "unblock_window_hours": self._system_subscribe_window_hours,
        })

    # ------------------ stop ------------------

    def stop_service(self):
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception:
            pass

        try:
            if self._toggle_scheduler:
                self._toggle_scheduler.remove_all_jobs()
                if self._toggle_scheduler.running:
                    self._toggle_scheduler.shutdown()
                self._toggle_scheduler = None
        except Exception:
            pass

    # ======================================================================
    # 必备：get_state / get_form / get_page / get_api / get_service
    # ======================================================================

    def get_state(self) -> bool:
        return self._enabled

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return UIConfig.get_form()

    def get_page(self) -> Optional[List[dict]]:
        history = self.get_data('history') or []
        return UIConfig.get_page(history)

    def get_module(self) -> Dict[str, Any]:
        if not self._enabled or not self._mp_search_enabled:
            return {}
        return {
            "get_search_page_size": self.mp_search_page_size,
            "search_torrents": self.mp_search_torrents,
            "async_search_torrents": self.async_mp_search_torrents,
            "download": self.mp_download,
            "download_added": self.mp_download_added,
        }

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/sync_subscribes",
                "endpoint": self.sync_subscribes,
                "methods": ["GET"],
                "summary": "执行同步订阅追更"
            },
            {
                "path": "/clear_history",
                "endpoint": self.api_clear_history,
                "methods": ["POST"],
                "summary": "清空历史记录"
            }
        ]
    
    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """定义远程控制命令"""
        return [{
            "cmd": "/p115_sub_action",
            "event": EventType.PluginAction,
            "desc": "115网盘订阅追更增强版",
            "category": "订阅",
            "data": {
                "action": "p115_sub_action"
            }
        }]


    def get_service(self) -> List[Dict[str, Any]]:
        if not self._enabled:
            return []

        services = []

        if self._cron and self._cron_interval_ge_min_hours(self._cron, self._MIN_INTERVAL_HOURS):
            try:
                services.append({
                    "id": "P115StrgmSubPlus",
                    "name": "115网盘订阅追更增强版服务",
                    "trigger": CronTrigger.from_crontab(self._cron),
                    "func": self.sync_subscribes,
                    "kwargs": {}
                })
            except Exception as e:
                logger.warning(f"Cron 表达式无效：{self._cron}，将回退 interval=8h。错误：{e}")
                services.append({
                    "id": "P115StrgmSubPlus",
                    "name": "115网盘订阅追更增强版服务",
                    "trigger": "interval",
                    "func": self.sync_subscribes,
                    "kwargs": {"hours": 8}
                })
        else:
            services.append({
                "id": "P115StrgmSubPlus",
                "name": "115网盘订阅追更增强版服务",
                "trigger": "interval",
                "func": self.sync_subscribes,
                "kwargs": {"hours": 8}
            })

        # HDHive 签到服务
        if self._hdhive_checkin_enabled and self._hdhive_checkin_cron:
            try:
                services.append({
                    "id": "P115StrgmSubPlus_HDHiveCheckin",
                    "name": "HDHive 签到服务",
                    "trigger": CronTrigger.from_crontab(self._hdhive_checkin_cron),
                    "func": self._do_hdhive_checkin,
                    "kwargs": {}
                })
                logger.info(f"HDHive 签到服务已注册，Cron: {self._hdhive_checkin_cron}")
            except Exception as e:
                logger.warning(f"HDHive 签到 Cron 表达式无效：{self._hdhive_checkin_cron}，错误：{e}")

        return services

    # ======================================================================
    # 必备：_do_sync（返回 bool）
    # ======================================================================

    def _do_sync(self) -> bool:
        # 至少启用一个搜索源
        if not self._pansou_enabled and not self._nullbr_enabled and not self._hdhive_enabled:
            logger.error("搜索源均未启用（PanSou/Nullbr/HDHive），无法执行")
            if self._notify:
                self.post_message(
                    mtype=NotificationType.Plugin,
                    title="【115网盘订阅追更增强版】配置错误",
                    text="PanSou、Nullbr、HDHive 均未启用，请至少启用一个搜索源。"
                )
            return False

        # 115 客户端检查
        if not self._p115_manager:
            logger.error("115 客户端未初始化，请检查 Cookie 配置")
            return False

        if not self._p115_manager.check_login():
            logger.error("115 登录失败，Cookie 可能已过期")
            if self._notify:
                self.post_message(
                    mtype=NotificationType.Manual,
                    title="【115网盘订阅追更增强版】登录失败",
                    text="115 Cookie 可能已过期，请更新后重试。"
                )
            return False

        logger.info("开始执行 115 网盘订阅同步...")
        if self._notify:
            self.post_message(
                mtype=NotificationType.Plugin,
                title="【115网盘订阅追更增强版】开始执行",
                text="正在扫描订阅列表并同步缺失内容..."
            )

        # reset api counters
        try:
            self._p115_manager.reset_api_call_count()
        except Exception:
            pass
        try:
            if self._pansou_client:
                self._pansou_client.reset_api_call_count()
        except Exception:
            pass
        try:
            if self._nullbr_client:
                self._nullbr_client.reset_api_call_count()
        except Exception:
            pass
        try:
            if self._search_handler:
                self._search_handler.reset_task_spent_points()
        except Exception:
            pass

        # 获取订阅
        with SessionFactory() as db:
            subscribes = SubscribeOper(db=db).list("N,R")

        if not subscribes:
            logger.info("无订阅数据")
            if self._notify:
                self.post_message(
                    mtype=NotificationType.Plugin,
                    title="【115网盘订阅追更增强版】执行完成",
                    text="当前无订阅数据。"
                )
            return True

        tv_subscribes = [s for s in subscribes if s.type == MediaType.TV.value]
        movie_subscribes = [s for s in subscribes if s.type == MediaType.MOVIE.value]

        if not tv_subscribes and not movie_subscribes:
            logger.info("无电影/剧集订阅")
            return True

        history: List[dict] = self.get_data('history') or []
        transfer_details: List[Dict[str, Any]] = []
        transferred_count = 0

        exclude_ids = set(self._exclude_subscribes or [])

        # 处理电影
        for subscribe in movie_subscribes:
            if global_vars.is_system_stopped:
                break
            if subscribe.id in exclude_ids:
                continue
            transferred_count = self._sync_handler.process_movie_subscribe(
                subscribe=subscribe,
                history=history,
                transfer_details=transfer_details,
                transferred_count=transferred_count
            )

        # 处理剧集
        for subscribe in tv_subscribes:
            if global_vars.is_system_stopped:
                break
            if subscribe.id in exclude_ids:
                continue
            transferred_count = self._sync_handler.process_tv_subscribe(
                subscribe=subscribe,
                history=history,
                transfer_details=transfer_details,
                transferred_count=transferred_count,
                exclude_ids=exclude_ids
            )

        self.save_data('history', history)

        logger.info(f"115 网盘订阅同步完成，共转存 {transferred_count} 个文件")

        if self._notify:
            if transferred_count > 0:
                self._sync_handler.send_transfer_notification(transfer_details, transferred_count)
            else:
                self.post_message(
                    mtype=NotificationType.Plugin,
                    title="【115网盘订阅追更增强版】执行完成",
                    text="本次同步未发现需要转存的新资源。"
                )

        return True

    # ------------------ API包装（用于 get_api） ------------------

    def api_clear_history(self, apikey: str) -> dict:
        return self._api_handler.clear_history(apikey)

    # ------------------ 同步入口（触发条件1） ------------------

    def sync_subscribes(self):
        with lock:
            tz = pytz.timezone(settings.TZ)
            run_start = datetime.datetime.now(tz=tz)

            success = False
            try:
                success = self._do_sync()
            except Exception as e:
                logger.error(f"同步任务异常：{e}")
                success = False
            finally:
                # 仅在用户开启了���蔽系统订阅时，才执行自动窗口切换逻辑
                if success and self._block_system_subscribe and self._is_last_run_today(run_start):
                    if int(self._unblock_delay_minutes) < 0 or (not self._window_enabled()):
                        self._enter_blocked(reason="触发条件1")
                    else:
                        self._schedule_unblock_after_delay(datetime.datetime.now(tz=pytz.timezone(settings.TZ)))

    # ------------------ 业务 API（保留） ------------------

    def api_search(self, keyword: str, apikey: str) -> dict:
        return self._api_handler.search(keyword, apikey)

    def api_transfer(self, share_url: str, save_path: str, apikey: str) -> dict:
        return self._api_handler.transfer(share_url, save_path, apikey)

    def api_list_directories(self, path: str = "/", apikey: str = "") -> dict:
        return self._api_handler.list_directories(path, apikey)

    @eventmanager.register(EventType.PluginAction)
    def remote_sync(self, event: Event):
        if not event:
            return
        event_data = event.event_data
        if not event_data or event_data.get("action") != "p115_sub_action":
            return

        logger.info("收到命令，开始执行追更任务")
        self.post_message(
            mtype=NotificationType.Plugin,
            channel=event_data.get("channel"),
            title="【115网盘订阅追更增强版】开始执行",
            text="已收到远程命令，正在执行追更任务...",
            userid=event_data.get("user")
        )

        self.sync_subscribes()

        self.post_message(
            mtype=NotificationType.Plugin,
            channel=event_data.get("channel"),
            title="【115网盘订阅追更增强版】执行完成",
            text="远程触发的追更任务已完成。",
            userid=event_data.get("user")
        )
