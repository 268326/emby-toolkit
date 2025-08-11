# core_processor_sa.py

import os
import json
import sqlite3
import concurrent.futures
from typing import Dict, List, Optional, Any, Tuple
import shutil
import threading
import time
import requests
import copy
import random
# 确保所有依赖都已正确导入
import emby_handler
import tmdb_handler
import utils
import constants
import logging
import actor_utils
from cachetools import TTLCache
from db_handler import ActorDBManager
from db_handler import get_db_connection as get_central_db_connection
from ai_translator import AITranslator
from utils import LogDBManager, get_override_path_for_item, translate_country_list
from watchlist_processor import WatchlistProcessor
from douban import DoubanApi

logger = logging.getLogger(__name__)
try:
    from douban import DoubanApi
    DOUBAN_API_AVAILABLE = True
except ImportError:
    DOUBAN_API_AVAILABLE = False
    class DoubanApi:
        def __init__(self, *args, **kwargs): pass
        def get_acting(self, *args, **kwargs): return {}
        def close(self): pass

def _read_local_json(file_path: str) -> Optional[Dict[str, Any]]:
    if not os.path.exists(file_path):
        logger.warning(f"本地元数据文件不存在: {file_path}")
        return None
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"读取本地JSON文件失败: {file_path}, 错误: {e}")
        return None
def _save_metadata_to_cache(
    cursor: sqlite3.Cursor,
    tmdb_id: str,
    item_type: str,
    item_details: Dict[str, Any],
    processed_cast: List[Dict[str, Any]],
    raw_tmdb_json: Dict[str, Any]
):
    """
    【V5.3 - 终极版】
    确保所有 TMDB 元数据均从 raw_tmdb_json 提取，并兼容所有已知键值差异。
    """
    try:
        # --- 统一从 raw_tmdb_json (权威数据源) 中提取所有信息 ---
        
        # 导演
        actor_data_container = raw_tmdb_json.get("casts") or raw_tmdb_json.get("credits", {})
        crew = actor_data_container.get("crew", [])
        directors = [{"id": m.get("id"), "name": m.get("name")} for m in crew if m.get("job") == "Director"]

        # 国家 (兼容 'production_countries' 和 'origin_country')
        countries_to_translate = []
        if 'production_countries' in raw_tmdb_json and raw_tmdb_json['production_countries']:
            countries_to_translate = [c.get('name') for c in raw_tmdb_json['production_countries'] if c.get('name')]
        elif 'origin_country' in raw_tmdb_json:
            countries_to_translate = raw_tmdb_json.get('origin_country', [])
        translated_countries = translate_country_list(countries_to_translate)

        # 【【【最终手术：工作室/电视网络】】】
        # 兼容 'production_companies' (电影/在线电视剧) 和 'networks' (本地电视剧缓存)
        studios_list = raw_tmdb_json.get("production_companies")
        if not studios_list:
            studios_list = raw_tmdb_json.get("networks", [])
        
        studios = [s.get("name") for s in studios_list if s.get("name")]
        logger.trace(f"从权威数据源提取到工作室/电视网络: {studios}")
        # 【【【手术结束】】】

        # 类型
        genres = [g.get("name") for g in raw_tmdb_json.get("genres", []) if g.get("name")]

        # 标题、年份、评分等 (兼容电影和电视剧)
        title = raw_tmdb_json.get('title') or raw_tmdb_json.get('name')
        original_title = raw_tmdb_json.get('original_title') or raw_tmdb_json.get('original_name')
        release_date_str = raw_tmdb_json.get('release_date') or raw_tmdb_json.get('first_air_date')
        release_year = int(release_date_str.split('-')[0]) if release_date_str and '-' in release_date_str else None
        rating = raw_tmdb_json.get('vote_average')
        
        # --- 准备要存入数据库的数据 ---
        metadata = {
            "tmdb_id": tmdb_id,
            "item_type": item_type,
            "title": title,
            "original_title": original_title,
            "release_year": release_year,
            "rating": rating,
            "genres_json": json.dumps(genres, ensure_ascii=False),
            "actors_json": json.dumps([
                {"id": p.get("id"), "name": p.get("name"), "original_name": p.get("original_name") or p.get("name")}
                for p in processed_cast
            ], ensure_ascii=False),
            "directors_json": json.dumps(directors, ensure_ascii=False),
            "studios_json": json.dumps(studios, ensure_ascii=False), # <--- 现在来源绝对可靠
            "countries_json": json.dumps(translated_countries, ensure_ascii=False),
            "date_added": item_details.get("DateCreated", "").split("T")[0] if item_details.get("DateCreated") else None,
            "release_date": release_date_str,
        }
        
        # --- 数据库写入 ---
        columns = ', '.join(metadata.keys())
        placeholders = ', '.join('?' for _ in metadata)
        sql = f"INSERT OR REPLACE INTO media_metadata ({columns}) VALUES ({placeholders})"
        cursor.execute(sql, tuple(metadata.values()))
        logger.debug(f"成功将《{metadata.get('title', '未知标题')}》的元数据缓存到数据库。")

    except Exception as e:
        logger.error(f"保存元数据到缓存表时失败: {e}", exc_info=True)
        
# ==========================================================================================
# +++ 新增：JSON 构建器函数 (JSON Builders) +++
# 这些函数严格按照您提供的最小化模板来构建新的JSON对象，确保结构纯净。
# ==========================================================================================

def _build_movie_json(source_data: Dict[str, Any], processed_cast: List[Dict[str, Any]], processed_crew: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    【V-Rebuild - 重建版】根据最小化模板构建电影 all.json。
    无论输入数据结构如何，都强制输出干净、统一的格式。
    """
    # 1. 从原始数据中安全地提取所有必需的字段
    #    使用 .get() 并提供默认值，确保即使原始数据缺少某些键也不会出错。
    final_json = {
      "id": source_data.get("id"),
      "imdb_id": source_data.get("imdb_id"),
      "title": source_data.get("title", ""),
      "original_title": source_data.get("original_title", ""),
      "overview": source_data.get("overview", ""),
      "tagline": source_data.get("tagline", ""),
      "release_date": source_data.get("release_date", ""),
      "vote_average": source_data.get("vote_average", 0.0),
      "production_countries": source_data.get("production_countries", []),
      "production_companies": source_data.get("production_companies", []),
      "genres": source_data.get("genres", []),
      # 以下字段是为了兼容性，即使模板中没有也建议保留
      "belongs_to_collection": source_data.get("belongs_to_collection"),
      "videos": source_data.get("videos", {"results": []}),
      "external_ids": source_data.get("external_ids", {})
    }

    # 2. ★★★ 核心：强制创建 "casts" 键，并填入处理好的演员和职员数据 ★★★
    final_json["casts"] = {
        "cast": processed_cast,
        "crew": processed_crew
    }
    
    # 3. (可选但推荐) 清理一下可能为空的ID
    if not final_json.get("imdb_id"):
        final_json["imdb_id"] = final_json.get("external_ids", {}).get("imdb_id", "")

    return final_json

def _build_series_json(source_data: Dict[str, Any], processed_cast: List[Dict[str, Any]], processed_crew: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    【V-Final Emby-Native】根据 Emby 友好格式构建 series.json。
    此版本假设输入数据已被上游归一化。
    """
    final_json = {
        "id": source_data.get("id"),
        "name": source_data.get("name", ""),
        "original_name": source_data.get("original_name", ""),
        "overview": source_data.get("overview", ""),
        "vote_average": source_data.get("vote_average", 0.0),
        "episode_run_time": source_data.get("episode_run_time", []),
        "first_air_date": source_data.get("first_air_date"),
        "last_air_date": source_data.get("last_air_date"),
        "status": source_data.get("status", ""),
        "genres": source_data.get("genres", []),
        "external_ids": source_data.get("external_ids", {}),
        "videos": source_data.get("videos", {"results": []}),
        "content_ratings": source_data.get("content_ratings", {"results": []}),
        
        # 【关键】: 现在只从归一化后的键取值
        "networks": source_data.get("networks", []),
        "origin_country": source_data.get("origin_country", [])
    }

    # 创建 credits 键
    final_json["credits"] = {
        "cast": processed_cast,
        "crew": processed_crew
    }
    
    return final_json

def _build_season_json(source_data: Dict[str, Any], processed_cast: List[Dict[str, Any]], processed_crew: List[Dict[str, Any]]) -> Dict[str, Any]:
    """【V3 最终修复版】根据最小化模板构建季 season-X.json，确保Emby兼容性。"""
    return {
      # "id" 和 "_id" 都不会被包含，因为模板里没有
      "name": source_data.get("name", ""),
      "overview": source_data.get("overview", ""),
      "air_date": source_data.get("air_date", "1970-01-01T00:00:00.000Z"),
      "external_ids": source_data.get("external_ids", {"tvdb_id": None}),
      "credits": {
        "cast": processed_cast,
        "crew": processed_crew
      }
      # "episodes" 数组不会被包含，因为模板里没有
    }

def _build_episode_json(source_data: Dict[str, Any], processed_cast: List[Dict[str, Any]], processed_crew: List[Dict[str, Any]]) -> Dict[str, Any]:
    """【V3 最终修复版】根据最小化模板构建集 season-X-episode-Y.json，确保Emby兼容性。"""
    return {
      # "id" 和 "_id" 都不会被包含
      "name": source_data.get("name", ""),
      "overview": source_data.get("overview", ""),
      "videos": source_data.get("videos", {"results": []}),
      "external_ids": source_data.get("external_ids", {"tvdb_id": None, "tvrage_id": None, "imdb_id": ""}),
      "air_date": source_data.get("air_date", "1970-01-01T00:00:00.000Z"),
      "vote_average": source_data.get("vote_average", 0.0),
      "credits": {
        "cast": processed_cast,
        "guest_stars": [], # 保持清空
        "crew": processed_crew
      }
    }

# ==========================================================================================
# +++ 新增：演员聚合函数 +++
# 从内存中的TMDB数据聚合演员，而不是从文件
# ==========================================================================================
def _aggregate_series_cast_from_tmdb_data(series_data: Dict[str, Any], all_episodes_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    【新】从内存中的TMDB数据聚合一个剧集的所有演员。
    """
    logger.debug(f"【演员聚合】开始为 '{series_data.get('name')}' 从内存中的TMDB数据聚合演员...")
    aggregated_cast_map = {}

    # 1. 优先处理主剧集的演员列表
    main_cast = series_data.get("credits", {}).get("cast", [])
    for actor in main_cast:
        actor_id = actor.get("id")
        if actor_id:
            aggregated_cast_map[actor_id] = actor
    logger.debug(f"  -> 从主剧集数据中加载了 {len(aggregated_cast_map)} 位主演员。")

    # 2. 聚合所有分集的演员和客串演员
    for episode_data in all_episodes_data:
        credits_data = episode_data.get("credits", {})
        actors_to_process = credits_data.get("cast", []) + credits_data.get("guest_stars", [])
        
        for actor in actors_to_process:
            actor_id = actor.get("id")
            if actor_id and actor_id not in aggregated_cast_map:
                if 'order' not in actor:
                    actor['order'] = 999  # 为客串演员设置高order值
                aggregated_cast_map[actor_id] = actor

    full_aggregated_cast = list(aggregated_cast_map.values())
    full_aggregated_cast.sort(key=lambda x: x.get('order', 999))
    
    logger.info(f"【演员聚合】完成。共为 '{series_data.get('name')}' 聚合了 {len(full_aggregated_cast)} 位独立演员。")
    return full_aggregated_cast
class MediaProcessor:
    def __init__(self, config: Dict[str, Any]):
        # ★★★ 然后，从这个 config 字典里，解析出所有需要的属性 ★★★
        self.config = config
        self.db_path = config.get('db_path')
        if not self.db_path:
            raise ValueError("数据库路径 (db_path) 未在配置中提供。")

        # 初始化我们的数据库管理员
        self.actor_db_manager = ActorDBManager(self.db_path)
        self.log_db_manager = LogDBManager(self.db_path)

        # 从 config 中获取所有其他配置
        self.douban_api = None
        if getattr(constants, 'DOUBAN_API_AVAILABLE', False):
            try:
                # --- ✨✨✨ 核心修改区域 START ✨✨✨ ---

                # 1. 从配置中获取冷却时间 
                douban_cooldown = self.config.get(constants.CONFIG_OPTION_DOUBAN_DEFAULT_COOLDOWN, 2.0)
                
                # 2. 从配置中获取 Cookie，使用我们刚刚在 constants.py 中定义的常量
                douban_cookie = self.config.get(constants.CONFIG_OPTION_DOUBAN_COOKIE, "")
                
                # 3. 添加一个日志，方便调试
                if not douban_cookie:
                    logger.debug(f"配置文件中未找到或未设置 '{constants.CONFIG_OPTION_DOUBAN_COOKIE}'。如果豆瓣API返回'need_login'错误，请配置豆瓣cookie。")
                else:
                    logger.debug("已从配置中加载豆瓣 Cookie。")

                # 4. 将所有参数传递给 DoubanApi 的构造函数
                self.douban_api = DoubanApi(
                    cooldown_seconds=douban_cooldown,
                    user_cookie=douban_cookie  # <--- 将 cookie 传进去
                )
                logger.trace("DoubanApi 实例已在 MediaProcessorAPI 中创建。")
                
                # --- ✨✨✨ 核心修改区域 END ✨✨✨ ---

            except Exception as e:
                logger.error(f"MediaProcessorAPI 初始化 DoubanApi 失败: {e}", exc_info=True)
        else:
            logger.warning("DoubanApi 常量指示不可用，将不使用豆瓣功能。")
        self.emby_url = self.config.get("emby_server_url")
        self.emby_api_key = self.config.get("emby_api_key")
        self.emby_user_id = self.config.get("emby_user_id")
        self.tmdb_api_key = self.config.get("tmdb_api_key", "")
        self.local_data_path = self.config.get("local_data_path", "").strip()
        self.sync_images_enabled = self.config.get(constants.CONFIG_OPTION_SYNC_IMAGES, False)
        
        self.ai_enabled = self.config.get("ai_translation_enabled", False)
        self.ai_translator = AITranslator(self.config) if self.ai_enabled else None
        
        self._stop_event = threading.Event()
        self.processed_items_cache = self._load_processed_log_from_db()
        self.manual_edit_cache = TTLCache(maxsize=10, ttl=600)
        logger.trace("核心处理器初始化完成。")
    # --- 清除已处理记录 ---
    def clear_processed_log(self):
        """
        【已改造】清除数据库和内存中的已处理记录。
        使用中央数据库连接函数。
        """
        try:
            # 1. ★★★ 调用中央函数，并传入 self.db_path ★★★
            with get_central_db_connection(self.db_path) as conn:
                cursor = conn.cursor()
                
                logger.debug("正在从数据库删除 processed_log 表中的所有记录...")
                cursor.execute("DELETE FROM processed_log")
                # with 语句会自动处理 conn.commit()
            
            logger.info("数据库中的已处理记录已清除。")

            # 2. 清空内存缓存
            self.processed_items_cache.clear()
            logger.info("内存中的已处理记录缓存已清除。")

        except Exception as e:
            logger.error(f"清除数据库或内存已处理记录时失败: {e}", exc_info=True)
            # 3. ★★★ 重新抛出异常，通知上游调用者操作失败 ★★★
            raise
    # ★★★★★★★★★★★★★★★ 新增的、优雅的内部辅助方法 ★★★★★★★★★★★★★★★
    def _enrich_cast_from_db_and_api(self, cast_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        【V-Final Hybrid with Dict Conversion】终极混合动力增强模块。
        在内部处理 sqlite3.Row，但对外返回标准的 dict 列表，确保下游兼容性。
        """
        if not cast_list:
            return []
        
        logger.info(f"🚀 混合动力增强模块启动，处理 {len(cast_list)} 位演员...")

        original_actor_map = {str(actor.get("Id")): actor for actor in cast_list if actor.get("Id")}
        
        # --- 阶段一：从本地数据库获取数据 ---
        enriched_actors_map = {}
        ids_found_in_db = set()
        
        try:
            # ★★★★★★★★★★★★★★★ 关键修改：在这里获取连接并设置 row_factory ★★★★★★★★★★★★★★★
            with get_central_db_connection(self.db_path) as conn:
                # conn.row_factory = sqlite3.Row # 假设 get_central_db_connection 已经设置了
                cursor = conn.cursor()
                person_ids = list(original_actor_map.keys())
                if person_ids:
                    placeholders = ','.join('?' for _ in person_ids)
                    query = f"SELECT * FROM person_identity_map WHERE emby_person_id IN ({placeholders})"
                    cursor.execute(query, person_ids)
                    db_results = cursor.fetchall()

                    for row in db_results:
                        # ★★★★★★★★★★★★★★★ 关键修改：立即将 sqlite3.Row 转换为 dict ★★★★★★★★★★★★★★★
                        db_data = dict(row)
                        
                        actor_id = str(db_data["emby_person_id"])
                        ids_found_in_db.add(actor_id)
                        
                        provider_ids = {}
                        # 现在可以安全地使用 .get() 方法了
                        if db_data.get("tmdb_person_id"): provider_ids["Tmdb"] = str(db_data.get("tmdb_person_id"))
                        if db_data.get("imdb_id"): provider_ids["Imdb"] = db_data.get("imdb_id")
                        if db_data.get("douban_celebrity_id"): provider_ids["Douban"] = str(db_data.get("douban_celebrity_id"))
                        
                        enriched_actor = original_actor_map[actor_id].copy()
                        enriched_actor["ProviderIds"] = provider_ids
                        enriched_actors_map[actor_id] = enriched_actor
        except Exception as e:
            logger.error(f"混合动力增强：数据库查询阶段失败: {e}", exc_info=True)

        logger.info(f"  -> 阶段一 (数据库) 完成：找到了 {len(ids_found_in_db)} 位演员的缓存信息。")

        # --- 阶段二：为未找到的演员实时查询 Emby API (这部分逻辑不变) ---
        ids_to_fetch_from_api = [pid for pid in original_actor_map.keys() if pid not in ids_found_in_db]
        
        if ids_to_fetch_from_api:
            logger.info(f"  -> 阶段二 (API查询) 开始：为 {len(ids_to_fetch_from_api)} 位新演员实时获取信息...")
            for i, actor_id in enumerate(ids_to_fetch_from_api):
                # ... (这里的 API 调用逻辑保持不变) ...
                full_detail = emby_handler.get_emby_item_details(
                    item_id=actor_id,
                    emby_server_url=self.emby_url,
                    emby_api_key=self.emby_api_key,
                    user_id=self.emby_user_id,
                    fields="ProviderIds,Name" # 只请求最关键的信息
                )
                if full_detail and full_detail.get("ProviderIds"):
                    enriched_actor = original_actor_map[actor_id].copy()
                    enriched_actor["ProviderIds"] = full_detail["ProviderIds"]
                    enriched_actors_map[actor_id] = enriched_actor
                else:
                    logger.warning(f"    未能从 API 获取到演员 ID {actor_id} 的 ProviderIds。")
        else:
            logger.info("  -> 阶段二 (API查询) 跳过：所有演员均在本地数据库中找到。")

        # --- 阶段三：合并最终结果 (这部分逻辑不变) ---
        final_enriched_cast = []
        for original_actor in cast_list:
            actor_id = str(original_actor.get("Id"))
            final_enriched_cast.append(enriched_actors_map.get(actor_id, original_actor))

        logger.info("🚀 混合动力增强模块完成。")
        return final_enriched_cast
    # ★★★ 公开的、独立的追剧判断方法 ★★★
    def check_and_add_to_watchlist(self, item_details: Dict[str, Any]):
        """
        检查一个媒体项目是否为剧集，如果是，则执行智能追剧判断并添加到待看列表。
        此方法被设计为由外部事件（如Webhook）显式调用。
        """
        item_name_for_log = item_details.get("Name", f"未知项目(ID:{item_details.get('Id')})")
        
        if item_details.get("Type") != "Series":
            # 如果不是剧集，直接返回，不打印非必要的日志
            return

        logger.info(f"Webhook触发：开始为新入库剧集 '{item_name_for_log}' 进行追剧状态判断...")
        try:
            # 实例化 WatchlistProcessor 并执行添加操作
            watchlist_proc = WatchlistProcessor(self.config)
            watchlist_proc.add_series_to_watchlist(item_details)
        except Exception as e_watchlist:
            logger.error(f"在自动添加 '{item_name_for_log}' 到追剧列表时发生错误: {e_watchlist}", exc_info=True)

    def signal_stop(self):
        self._stop_event.set()

    def clear_stop_signal(self):
        self._stop_event.clear()

    def get_stop_event(self) -> threading.Event:
        """返回内部的停止事件对象，以便传递给其他函数。"""
        return self._stop_event

    def is_stop_requested(self) -> bool:
        return self._stop_event.is_set()

    def _load_processed_log_from_db(self) -> Dict[str, str]:
        log_dict = {}
        try:
            # 1. ★★★ 使用 with 语句和中央函数 ★★★
            with get_central_db_connection(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 2. 执行查询
                cursor.execute("SELECT item_id, item_name FROM processed_log")
                rows = cursor.fetchall()
                
                # 3. 处理结果
                for row in rows:
                    if row['item_id'] and row['item_name']:
                        log_dict[row['item_id']] = row['item_name']
            
            # 4. with 语句会自动处理所有事情，代码干净利落！

        except Exception as e:
            # 5. ★★★ 记录更详细的异常信息 ★★★
            logger.error(f"从数据库读取已处理记录失败: {e}", exc_info=True)
        return log_dict

    # ✨ 从 SyncHandler 迁移并改造，用于在本地缓存中查找豆瓣JSON文件
    def _find_local_douban_json(self, imdb_id: Optional[str], douban_id: Optional[str], douban_cache_dir: str) -> Optional[str]:
        """根据 IMDb ID 或 豆瓣 ID 在本地缓存目录中查找对应的豆瓣JSON文件。"""
        if not os.path.exists(douban_cache_dir):
            return None
        
        # 优先使用 IMDb ID 匹配，更准确
        if imdb_id:
            for dirname in os.listdir(douban_cache_dir):
                if dirname.startswith('0_'): continue
                if imdb_id in dirname:
                    dir_path = os.path.join(douban_cache_dir, dirname)
                    for filename in os.listdir(dir_path):
                        if filename.endswith('.json'):
                            return os.path.join(dir_path, filename)
                            
        # 其次使用豆瓣 ID 匹配
        if douban_id:
            for dirname in os.listdir(douban_cache_dir):
                if dirname.startswith(f"{douban_id}_"):
                    dir_path = os.path.join(douban_cache_dir, dirname)
                    for filename in os.listdir(dir_path):
                        if filename.endswith('.json'):
                            return os.path.join(dir_path, filename)
        return None

    # ✨ 封装了“优先本地缓存，失败则在线获取”的逻辑
    def _get_douban_data_with_local_cache(self, media_info: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Optional[float]]:
        """
        【V3 - 最终版】获取豆瓣数据（演员+评分）。优先本地缓存，失败则回退到功能完整的在线API路径。
        返回: (演员列表, 豆瓣评分) 的元组。
        """
        # 1. 准备查找所需的信息
        provider_ids = media_info.get("ProviderIds", {})
        item_name = media_info.get("Name", "")
        imdb_id = provider_ids.get("Imdb")
        douban_id_from_provider = provider_ids.get("Douban")
        item_type = media_info.get("Type")
        item_year = str(media_info.get("ProductionYear", ""))

        # 2. 尝试从本地缓存查找
        douban_cache_dir_name = "douban-movies" if item_type == "Movie" else "douban-tv"
        douban_cache_path = os.path.join(self.local_data_path, "cache", douban_cache_dir_name)
        local_json_path = self._find_local_douban_json(imdb_id, douban_id_from_provider, douban_cache_path)

        if local_json_path:
            logger.debug(f"发现本地豆瓣缓存文件，将直接使用: {local_json_path}")
            douban_data = _read_local_json(local_json_path)
            if douban_data:
                cast = douban_data.get('actors', [])
                rating_str = douban_data.get("rating", {}).get("value")
                rating_float = None
                if rating_str:
                    try: rating_float = float(rating_str)
                    except (ValueError, TypeError): pass
                return cast, rating_float
            else:
                logger.warning(f"本地豆瓣缓存文件 '{local_json_path}' 无效，将回退到在线API。")
        
        # 3. 如果本地未找到，回退到功能完整的在线API路径
        logger.info("未找到本地豆瓣缓存，将通过在线API获取演员和评分信息。")

        # 3.1 匹配豆瓣ID和类型。现在 match_info 返回的结果是完全可信的。
        match_info_result = self.douban_api.match_info(
            name=item_name, imdbid=imdb_id, mtype=item_type, year=item_year
        )

        if match_info_result.get("error") or not match_info_result.get("id"):
            logger.warning(f"在线匹配豆瓣ID失败 for '{item_name}': {match_info_result.get('message', '未找到ID')}")
            return [], None

        douban_id = match_info_result["id"]
        # ✨✨✨ 直接信任从 douban.py 返回的类型 ✨✨✨
        douban_type = match_info_result.get("type")

        if not douban_type:
            logger.error(f"从豆瓣匹配结果中未能获取到媒体类型 for ID {douban_id}。处理中止。")
            return [], None

        # 3.2 获取演职员 (使用完全可信的类型)
        cast_data = self.douban_api.get_acting(
            name=item_name, 
            douban_id_override=douban_id, 
            mtype=douban_type
        )
        douban_cast_raw = cast_data.get("cast", [])

        # 3.3 获取详情（为了评分），同样使用可信的类型
        details_data = self.douban_api._get_subject_details(douban_id, douban_type)
        douban_rating = None
        if details_data and not details_data.get("error"):
            rating_str = details_data.get("rating", {}).get("value")
            if rating_str:
                try:
                    douban_rating = float(rating_str)
                    logger.info(f"在线获取到豆瓣评分 for '{item_name}': {douban_rating}")
                except (ValueError, TypeError):
                    pass

        return douban_cast_raw, douban_rating
    # --- 通过豆瓣ID查找映射表 ---
    def _find_person_in_map_by_douban_id(self, douban_id: str, cursor: sqlite3.Cursor) -> Optional[sqlite3.Row]:
        """
        根据豆瓣名人ID在 person_identity_map 表中查找对应的记录。
        """
        if not douban_id:
            return None
        try:
            cursor.execute(
                "SELECT * FROM person_identity_map WHERE douban_celebrity_id = ?",
                (douban_id,)
            )
            return cursor.fetchone()
        except sqlite3.Error as e:
            logger.error(f"通过豆瓣ID '{douban_id}' 查询 person_identity_map 时出错: {e}")
            return None
    # --- 通过ImbdID查找映射表 ---
    def _find_person_in_map_by_imdb_id(self, imdb_id: str, cursor: sqlite3.Cursor) -> Optional[sqlite3.Row]:
        """
        根据 IMDb ID 在 person_identity_map 表中查找对应的记录。
        """
        if not imdb_id:
            return None
        try:
            # 核心改动：将查询字段从 douban_celebrity_id 改为 imdb_id
            cursor.execute(
                "SELECT * FROM person_identity_map WHERE imdb_id = ?",
                (imdb_id,)
            )
            return cursor.fetchone()
        except sqlite3.Error as e:
            logger.error(f"通过 IMDb ID '{imdb_id}' 查询 person_identity_map 时出错: {e}")
            return None
    # --- 补充新增演员额外数据 ---
    def _get_actor_metadata_from_cache(self, tmdb_id: int, cursor: sqlite3.Cursor) -> Optional[Dict]:
        """根据TMDb ID从ActorMetadata缓存表中获取演员的元数据。"""
        if not tmdb_id:
            return None
        cursor.execute("SELECT * FROM ActorMetadata WHERE tmdb_id = ?", (tmdb_id,))
        metadata_row = cursor.fetchone()  # fetchone() 返回一个 sqlite3.Row 对象或 None
        if metadata_row:
            return dict(metadata_row)  # 将其转换为字典，方便使用
        return None
    # --- 批量注入分集演员表 ---
    def _batch_update_episodes_cast(self, series_id: str, series_name: str, final_cast_list: List[Dict[str, Any]]):
        """
        【V1 - 批量写入模块】
        将一个最终处理好的演员列表，高效地写入指定剧集下的所有分集。
        """
        logger.info(f"🚀 开始为剧集 '{series_name}' (ID: {series_id}) 批量更新所有分集的演员表...")
        
        # 1. 获取所有分集的 ID
        # 我们只需要 ID，所以可以请求更少的字段以提高效率
        episodes = emby_handler.get_series_children(
            series_id=series_id,
            base_url=self.emby_url,
            api_key=self.emby_api_key,
            user_id=self.emby_user_id,
            series_name_for_log=series_name,
            include_item_types="Episode" # ★★★ 明确指定只获取分集
        )
        
        if not episodes:
            logger.info("  -> 未找到任何分集，批量更新结束。")
            return

        total_episodes = len(episodes)
        logger.info(f"  -> 共找到 {total_episodes} 个分集需要更新。")
        
        # 2. 准备好要写入的数据 (所有分集都用同一份演员表)
        cast_for_emby_handler = []
        for actor in final_cast_list:
            cast_for_emby_handler.append({
                "name": actor.get("name"),
                "character": actor.get("character"),
                "emby_person_id": actor.get("emby_person_id"),
                "provider_ids": actor.get("provider_ids")
            })

        # 3. 遍历并逐个更新分集
        # 这里仍然需要逐个更新，因为 Emby API 不支持一次性更新多个项目的演员表
        # 但我们已经把最耗时的数据处理放在了循环外面
        for i, episode in enumerate(episodes):
            if self.is_stop_requested():
                logger.warning("分集批量更新任务被中止。")
                break
            
            episode_id = episode.get("Id")
            episode_name = episode.get("Name", f"分集 {i+1}")
            logger.debug(f"  ({i+1}/{total_episodes}) 正在更新分集 '{episode_name}' (ID: {episode_id})...")
            
            emby_handler.update_emby_item_cast(
                item_id=episode_id,
                new_cast_list_for_handler=cast_for_emby_handler,
                emby_server_url=self.emby_url,
                emby_api_key=self.emby_api_key,
                user_id=self.emby_user_id
            )
            # 加入一个微小的延迟，避免请求过于密集
            time.sleep(0.2)

        logger.info(f"🚀 剧集 '{series_name}' 的分集批量更新完成。")
    # --- 核心处理总管 ---
    def process_single_item(self, emby_item_id: str,
                            force_reprocess_this_item: bool = False,
                            force_fetch_from_tmdb: bool = False):
        """
        【V-API-Ready 最终版 - 带跳过功能】
        这个函数是API模式的入口，它会先检查是否需要跳过已处理的项目。
        """
        # 1. 保安检查：除非强制，否则跳过已处理的
        if not force_reprocess_this_item and emby_item_id in self.processed_items_cache:
            item_name_from_cache = self.processed_items_cache.get(emby_item_id, f"ID:{emby_item_id}")
            logger.info(f"媒体 '{item_name_from_cache}' 已在处理记录中，跳过。")
            return True

        # 2. 检查停止信号
        if self.is_stop_requested():
            return False

        # 3. 获取Emby详情，这是后续所有操作的基础
        item_details = emby_handler.get_emby_item_details(emby_item_id, self.emby_url, self.emby_api_key, self.emby_user_id)
        if not item_details:
            logger.error(f"process_single_item: 无法获取 Emby 项目 {emby_item_id} 的详情。")
            return False

        # 4. 将任务交给核心处理函数
        return self._process_item_core_logic_api_version(
            item_details_from_emby=item_details,
            force_reprocess_this_item=force_reprocess_this_item,
            force_fetch_from_tmdb=force_fetch_from_tmdb
        )

        # --- 核心处理流程 ---
    
    # ---核心处理流程 ---
    def _process_item_core_logic_api_version(self, item_details_from_emby: Dict[str, Any], force_reprocess_this_item: bool, force_fetch_from_tmdb: bool = False):
        """
        【V-Final Clarity - 清晰最终版】
        确保数据流清晰、单向，并从根源上解决所有已知问题。
        """
        item_id = item_details_from_emby.get("Id")
        item_name_for_log = item_details_from_emby.get("Name", f"未知项目(ID:{item_id})")
        tmdb_id = item_details_from_emby.get("ProviderIds", {}).get("Tmdb")
        item_type = item_details_from_emby.get("Type")

        if not tmdb_id:
            logger.error(f"项目 '{item_name_for_log}' 缺少 TMDb ID，无法处理。")
            return False

        try:
            # ======================================================================
            # 阶段 1: Emby 现状数据准备 (永远是第一步)
            # ======================================================================
            logger.info(f"【API模式】开始处理 '{item_name_for_log}' (TMDb ID: {tmdb_id})")
            
            current_emby_cast_raw = item_details_from_emby.get("People", [])
            enriched_emby_cast = self._enrich_cast_from_db_and_api(current_emby_cast_raw)
            original_emby_actor_count = len(enriched_emby_cast)
            logger.info(f"  -> 从 Emby 获取并增强后，得到 {original_emby_actor_count} 位现有演员用于后续所有操作。")

            # ======================================================================
            # 阶段 2: 权威数据源采集 (Authoritative Data Acquisition)
            # ======================================================================
            authoritative_cast_source = []

            # --- 电影处理逻辑 ---
            if item_type == "Movie":
                if force_fetch_from_tmdb and self.tmdb_api_key:
                    logger.info("  -> 电影策略: 强制从 TMDB API 获取元数据...")
                    movie_details = tmdb_handler.get_movie_details(tmdb_id, self.tmdb_api_key)
                    if movie_details:
                        credits_data = movie_details.get("credits") or movie_details.get("casts")
                        if credits_data: authoritative_cast_source = credits_data.get("cast", [])
            
            # --- 剧集处理逻辑 ---
            elif item_type == "Series":
                if force_fetch_from_tmdb and self.tmdb_api_key:
                    logger.info("  -> 剧集策略: 强制从 TMDB API 并发聚合...")
                    aggregated_tmdb_data = tmdb_handler.aggregate_full_series_data_from_tmdb(
                        tv_id=int(tmdb_id), api_key=self.tmdb_api_key, max_workers=5
                    )
                    if aggregated_tmdb_data:
                        all_episodes = list(aggregated_tmdb_data.get("episodes_details", {}).values())
                        authoritative_cast_source = _aggregate_series_cast_from_tmdb_data(aggregated_tmdb_data["series_details"], all_episodes)

            # ★★★★★★★★★★★★★★★ 最终的、正确的保底策略 ★★★★★★★★★★★★★★★
            # 如果强制刷新失败，或者没有强制刷新，则使用我们已经增强过的 Emby 列表作为权威数据源
            if not authoritative_cast_source:
                logger.info("  -> 保底策略: 未强制刷新或刷新失败，将使用增强后的 Emby 演员列表作为权威数据源。")
                authoritative_cast_source = enriched_emby_cast
            # ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★

            logger.info(f"  -> 数据采集阶段完成，最终选定 {len(authoritative_cast_source)} 位权威演员。")

            # ======================================================================
            # 阶段 3: 豆瓣及后续处理
            # ======================================================================
            douban_cast_raw, _ = self._get_douban_data_with_local_cache(item_details_from_emby)

            with get_central_db_connection(self.db_path) as conn:
                cursor = conn.cursor()
                
                final_processed_cast = self._process_cast_list_from_api(
                    tmdb_cast_people=authoritative_cast_source,
                    emby_cast_people=enriched_emby_cast,
                    douban_cast_list=douban_cast_raw,
                    item_details_from_emby=item_details_from_emby,
                    cursor=cursor,
                    tmdb_api_key=self.tmdb_api_key,
                    stop_event=self.get_stop_event()
                )

                # ======================================================================
                # 阶段 3: 数据写回 (Data Write-back)
                # ======================================================================
                logger.info("演员列表处理完成，准备通过 API 直接更新 Emby...")
                cast_for_emby_handler = []
                for actor in final_processed_cast:
                    cast_for_emby_handler.append({
                        "name": actor.get("name"),
                        "character": actor.get("character"),
                        "emby_person_id": actor.get("emby_person_id"),
                        "provider_ids": actor.get("provider_ids")
                    })

                update_success = emby_handler.update_emby_item_cast(
                    item_id=item_id,
                    new_cast_list_for_handler=cast_for_emby_handler,
                    emby_server_url=self.emby_url,
                    emby_api_key=self.emby_api_key,
                    user_id=self.emby_user_id
                )

                if item_type == "Series" and update_success:
                    self._batch_update_episodes_cast(
                        series_id=item_id,
                        series_name=item_name_for_log,
                        final_cast_list=final_processed_cast
                    )

                if not update_success:
                    logger.error(f"更新 Emby 项目 '{item_name_for_log}' 演员信息失败，记录到待复核列表。")
                    self.log_db_manager.save_to_failed_log(cursor, item_id, item_name_for_log, "API更新演员信息失败", item_type)
                    conn.commit()
                    return False

                logger.info("API 更新成功，触发一次轻量级刷新来更新 Emby 界面...")
                emby_handler.refresh_emby_item_metadata(
                    item_emby_id=item_id,
                    emby_server_url=self.emby_url,
                    emby_api_key=self.emby_api_key,
                    replace_all_metadata_param=False,
                    replace_all_images_param=False,
                    item_name_for_log=item_name_for_log,
                    user_id_for_unlock=self.emby_user_id
                )

                # ======================================================================
                # 阶段 4: 后续处理 (Post-processing)
                # ======================================================================
                genres = item_details_from_emby.get("Genres", [])
                is_animation = "Animation" in genres or "动画" in genres or "Documentary" in genres or "纪录" in genres
                processing_score = actor_utils.evaluate_cast_processing_quality(
                    final_cast=final_processed_cast,
                    original_cast_count=original_emby_actor_count,
                    expected_final_count=len(final_processed_cast),
                    is_animation=is_animation
                )

                min_score_for_review = float(self.config.get("min_score_for_review", constants.DEFAULT_MIN_SCORE_FOR_REVIEW))
                if processing_score < min_score_for_review:
                    reason = f"处理评分 ({processing_score:.2f}) 低于阈值 ({min_score_for_review})。"
                    self.log_db_manager.save_to_failed_log(cursor, item_id, item_name_for_log, reason, item_type, score=processing_score)
                else:
                    self.log_db_manager.save_to_processed_log(cursor, item_id, item_name_for_log, score=processing_score)
                    self.log_db_manager.remove_from_failed_log(cursor, item_id)
                    self.processed_items_cache[item_id] = item_name_for_log
                    logger.debug(f"已将 '{item_name_for_log}' (ID: {item_id}) 添加到内存缓存，下次将跳过。")

                conn.commit()

        except (ValueError, InterruptedError) as e:
            logger.warning(f"处理 '{item_name_for_log}' 的过程中断: {e}")
            return False
        except Exception as outer_e:
            logger.error(f"API模式核心处理流程中发生未知严重错误 for '{item_name_for_log}': {outer_e}", exc_info=True)
            try:
                with get_central_db_connection(self.db_path) as conn_fail:
                    self.log_db_manager.save_to_failed_log(conn_fail.cursor(), item_id, item_name_for_log, f"核心处理异常: {str(outer_e)}", item_type)
            except Exception as log_e:
                logger.error(f"写入失败日志时再次发生错误: {log_e}")
            return False

        logger.info(f"✨✨✨ API 模式处理完成 '{item_name_for_log}' ✨✨✨")
        return True

    # --- 核心处理器 ---
    def _process_cast_list_from_api(self, tmdb_cast_people: List[Dict[str, Any]],
                                    emby_cast_people: List[Dict[str, Any]],
                                    douban_cast_list: List[Dict[str, Any]],
                                    item_details_from_emby: Dict[str, Any],
                                    cursor: sqlite3.Cursor,
                                    tmdb_api_key: Optional[str],
                                    stop_event: Optional[threading.Event]) -> List[Dict[str, Any]]:
        """
        在函数开头增加一个“数据适配层”，将API数据转换为你现有逻辑期望的格式，
        然后原封不动地执行你所有经过打磨的核心代码。
        """
        logger.debug("API模式：进入数据适配层...")
         # ======================= 最终确认日志 =======================
        logger.info("  [最终确认] 进入 _process_cast_list_from_api 时，emby_cast_people 的第一个元素是:")
        if emby_cast_people:
            import json
            logger.info(f"  {json.dumps(emby_cast_people[0], ensure_ascii=False, indent=2)}")
        else:
            logger.warning("  [最终确认] emby_cast_people 为空！")
        # ==========================================================

        # +++ 新增诊断日志 +++
        logger.debug(f"诊断：从Emby接收到的原始演员列表 (前2条): {emby_cast_people[:2]}")
        # +++ 结束新增 +++

        emby_tmdb_to_person_id_map = {
            person.get("ProviderIds", {}).get("Tmdb"): person.get("Id")
            for person in emby_cast_people if person.get("ProviderIds", {}).get("Tmdb")
        }
        # +++ 新增诊断日志 +++
        logger.debug(f"诊断：构建的 TMDB ID -> Emby Person ID 映射表: {emby_tmdb_to_person_id_map}")
        # +++ 结束新增 +++

        local_cast_list = []
        for person_data in tmdb_cast_people: # tmdb_cast_people 现在是 authoritative_cast_source
            
            # 智能地从数据源中提取 TMDB ID
            tmdb_id = None
            # 优先检查 TMDB/神医缓存 标准格式
            if "id" in person_data:
                tmdb_id = str(person_data.get("id"))
            # 其次检查 Emby People 列表格式
            elif "ProviderIds" in person_data and person_data.get("ProviderIds", {}).get("Tmdb"):
                tmdb_id = str(person_data["ProviderIds"]["Tmdb"])
            
            if not tmdb_id or tmdb_id == 'None':
                continue

            new_actor_entry = person_data.copy()
            
            # 注入 emby_person_id
            new_actor_entry["emby_person_id"] = emby_tmdb_to_person_id_map.get(tmdb_id)
            
            # 统一数据结构，确保下游代码能正常工作
            if "id" not in new_actor_entry:
                new_actor_entry["id"] = tmdb_id
            if "name" not in new_actor_entry:
                new_actor_entry["name"] = new_actor_entry.get("Name")
            if "character" not in new_actor_entry:
                new_actor_entry["character"] = new_actor_entry.get("Role")

            local_cast_list.append(new_actor_entry)
        # ★★★★★★★★★★★★★★★ 全新的、更智能的数据适配层 END ★★★★★★★★★★★★★★★

        logger.debug(f"数据适配完成，生成了 {len(local_cast_list)} 条基准演员数据。")
        # ======================================================================
        # 步骤 2: ★★★ 原封不动地执行你所有的“原厂逻辑” ★★★
        # (下面的代码，是我根据你上次发的函数，整理出的最接近你原版的逻辑)
        # ======================================================================

        douban_candidates = actor_utils.format_douban_cast(douban_cast_list)

        # --- 你的“一对一匹配”逻辑 ---
        unmatched_local_actors = list(local_cast_list)  # ★★★ 使用我们适配好的数据源 ★★★
        merged_actors = []
        unmatched_douban_actors = []
        # 3. 遍历豆瓣演员，尝试在“未匹配”的本地演员中寻找配对
        for d_actor in douban_candidates:
            douban_name_zh = d_actor.get("Name", "").lower().strip()
            douban_name_en = d_actor.get("OriginalName", "").lower().strip()

            match_found_for_this_douban_actor = False

            for i, l_actor in enumerate(unmatched_local_actors):
                local_name = str(l_actor.get("name") or "").lower().strip()
                local_original_name = str(l_actor.get("original_name") or "").lower().strip()
                is_match, match_reason = False, ""
                if douban_name_zh and (douban_name_zh == local_name or douban_name_zh == local_original_name):
                    is_match, match_reason = True, "精确匹配 (豆瓣中文名)"
                elif douban_name_en and (douban_name_en == local_name or douban_name_en == local_original_name):
                    is_match, match_reason = True, "精确匹配 (豆瓣外文名)"
                if is_match:
                    logger.debug(f"  匹配成功 (对号入座): 豆瓣演员 '{d_actor.get('Name')}' -> 本地演员 '{l_actor.get('name')}' (ID: {l_actor.get('id')})")

                    l_actor["name"] = d_actor.get("Name")
                    cleaned_douban_character = utils.clean_character_name_static(d_actor.get("Role"))
                    l_actor["character"] = actor_utils.select_best_role(l_actor.get("character"), cleaned_douban_character)
                    if d_actor.get("DoubanCelebrityId"):
                        l_actor["douban_id"] = d_actor.get("DoubanCelebrityId")

                    merged_actors.append(unmatched_local_actors.pop(i))
                    match_found_for_this_douban_actor = True
                    break

            if not match_found_for_this_douban_actor:
                unmatched_douban_actors.append(d_actor)

        # 这里先把旧演员合并成列表，供后续新增和处理使用
        current_cast_list = merged_actors + unmatched_local_actors

        # ★★★ 【核心修复：把新增演员直接加入current_cast_list，统一处理】 ★★★
        # 先构造 final_cast_map，包含旧演员
        final_cast_map = {str(actor['id']): actor for actor in current_cast_list if actor.get('id') and str(actor.get('id')) != 'None'}
        # 新增阶段开始
        limit = self.config.get(constants.CONFIG_OPTION_MAX_ACTORS_TO_PROCESS, 30)
        try:
            limit = int(limit)
            if limit <= 0:
                limit = 30
        except (ValueError, TypeError):
            limit = 30

        current_actor_count = len(final_cast_map)
        if current_actor_count >= limit:
            logger.info(f"当前演员数 ({current_actor_count}) 已达上限 ({limit})，跳过所有新增演员的流程。")
        else:
            logger.info(f"当前演员数 ({current_actor_count}) 低于上限 ({limit})，进入补充模式（处理来自豆瓣的新增演员）。")

            logger.debug(f"--- 匹配阶段 2: 用豆瓣ID查 person_identity_map ({len(unmatched_douban_actors)} 位演员) ---")
            still_unmatched = []
            for d_actor in unmatched_douban_actors:
                if self.is_stop_requested():
                    raise InterruptedError("任务中止")
                d_douban_id = d_actor.get("DoubanCelebrityId")
                match_found = False
                if d_douban_id:
                    entry = self._find_person_in_map_by_douban_id(d_douban_id, cursor)
                    if entry and entry["tmdb_person_id"]:
                        tmdb_id_from_map = str(entry["tmdb_person_id"])
                        if tmdb_id_from_map not in final_cast_map:
                            logger.debug(f"  匹配成功 (通过 豆瓣ID映射): 豆瓣演员 '{d_actor.get('Name')}' -> 加入最终演员表")
                            cached_metadata = self._get_actor_metadata_from_cache(tmdb_id_from_map, cursor) or {}
                            new_actor_entry = {
                                "id": tmdb_id_from_map,
                                "name": d_actor.get("Name"),
                                "original_name": cached_metadata.get("original_name") or d_actor.get("OriginalName"),
                                "character": d_actor.get("Role"),
                                "adult": cached_metadata.get("adult", False),
                                "gender": cached_metadata.get("gender", 0),
                                "known_for_department": "Acting",
                                "popularity": cached_metadata.get("popularity", 0.0),
                                "profile_path": cached_metadata.get("profile_path"),
                                "cast_id": None,
                                "credit_id": None,
                                "order": 999,
                                "imdb_id": entry["imdb_id"] if "imdb_id" in entry else None,
                                "douban_id": d_douban_id,
                                "_is_newly_added": True
                            }
                            final_cast_map[tmdb_id_from_map] = new_actor_entry
                        match_found = True
                if not match_found:
                    still_unmatched.append(d_actor)
            unmatched_douban_actors = still_unmatched

            # --- 步骤 3 & 4: IMDb ID 反查 新增操作 ---
            logger.debug(f"--- 匹配阶段 3 & 4: 用IMDb ID进行最终匹配和新增 ({len(unmatched_douban_actors)} 位演员) ---")
            still_unmatched_final = []
            for i, d_actor in enumerate(unmatched_douban_actors):
                if self.is_stop_requested():
                    raise InterruptedError("任务中止")

                if len(final_cast_map) >= limit:
                    logger.info(f"演员数已达上限 ({limit})，跳过剩余 {len(unmatched_douban_actors) - i} 位演员的API查询。")
                    still_unmatched_final.extend(unmatched_douban_actors[i:])
                    break
                d_douban_id = d_actor.get("DoubanCelebrityId")
                match_found = False
                if d_douban_id and self.douban_api and self.tmdb_api_key:
                    if self.is_stop_requested():
                        logger.info("任务在处理豆瓣演员时被中止 (豆瓣API调用前)。")
                        raise InterruptedError("任务中止")
                    details = self.douban_api.celebrity_details(d_douban_id)
                    time.sleep(0.3)

                    d_imdb_id = None
                    if details and not details.get("error"):
                        try:
                            info_list = details.get("extra", {}).get("info", [])
                            if isinstance(info_list, list):
                                for item in info_list:
                                    if isinstance(item, list) and len(item) == 2 and item[0] == 'IMDb编号':
                                        d_imdb_id = item[1]
                                        break
                        except Exception as e_parse:
                            logger.warning(f"    -> 解析 IMDb ID 时发生意外错误: {e_parse}")

                    if d_imdb_id:
                        logger.debug(f"    -> 为 '{d_actor.get('Name')}' 获取到 IMDb ID: {d_imdb_id}，开始匹配...")

                        entry_from_map = self._find_person_in_map_by_imdb_id(d_imdb_id, cursor)

                        if entry_from_map and entry_from_map["tmdb_person_id"]:
                            tmdb_id_from_map = str(entry_from_map["tmdb_person_id"])

                            if tmdb_id_from_map not in final_cast_map:
                                logger.debug(f"  匹配成功 (通过 IMDb映射): 豆瓣演员 '{d_actor.get('Name')}' -> 加入最终演员表")
                                cached_metadata = self._get_actor_metadata_from_cache(tmdb_id_from_map, cursor) or {}
                                new_actor_entry = {
                                    "id": tmdb_id_from_map,
                                    "name": d_actor.get("Name"),
                                    "original_name": cached_metadata.get("original_name") or d_actor.get("OriginalName"),
                                    "character": d_actor.get("Role"),
                                    "adult": cached_metadata.get("adult", False),
                                    "gender": cached_metadata.get("gender", 0),
                                    "known_for_department": "Acting",
                                    "popularity": cached_metadata.get("popularity", 0.0),
                                    "profile_path": cached_metadata.get("profile_path"),
                                    "cast_id": None,
                                    "credit_id": None,
                                    "order": 999,
                                    "imdb_id": d_imdb_id,
                                    "douban_id": d_douban_id,
                                    "_is_newly_added": True
                                }
                                final_cast_map[tmdb_id_from_map] = new_actor_entry

                            logger.debug(f"    -> [实时反哺] 将新发现的映射关系 (Douban ID: {d_douban_id}) 保存回数据库...")
                            self.actor_db_manager.upsert_person(
                                cursor,
                                {
                                    "tmdb_id": tmdb_id_from_map,
                                    "imdb_id": d_imdb_id,
                                    "douban_id": d_douban_id,
                                     "name": d_actor.get("Name") or (entry_from_map["primary_name"] if "primary_name" in entry_from_map else None)
                                }
                            )
                            match_found = True

                        if not match_found:
                            logger.debug(f"    -> 数据库未找到 {d_imdb_id} 的映射，开始通过 TMDb API 反查...")
                            if self.is_stop_requested():
                                logger.info("任务在处理豆瓣演员时被中止 (TMDb API调用前)。")
                                raise InterruptedError("任务中止")

                            person_from_tmdb = tmdb_handler.find_person_by_external_id(d_imdb_id, self.tmdb_api_key, "imdb_id")
                            if person_from_tmdb and person_from_tmdb.get("id"):
                                tmdb_id_from_find = str(person_from_tmdb.get("id"))

                                if tmdb_id_from_find not in final_cast_map:
                                    logger.debug(f"  匹配成功 (通过 TMDb反查): 豆瓣演员 '{d_actor.get('Name')}' -> 加入最终演员表")
                                    cached_metadata = self._get_actor_metadata_from_cache(tmdb_id_from_find, cursor) or {}
                                    new_actor_entry = {
                                        "id": tmdb_id_from_find,
                                        "name": d_actor.get("Name"),
                                        "original_name": cached_metadata.get("original_name") or d_actor.get("OriginalName"),
                                        "character": d_actor.get("Role"),
                                        "adult": cached_metadata.get("adult", False),
                                        "gender": cached_metadata.get("gender", 0),
                                        "known_for_department": "Acting",
                                        "popularity": cached_metadata.get("popularity", 0.0),
                                        "profile_path": cached_metadata.get("profile_path"),
                                        "cast_id": None,
                                        "credit_id": None,
                                        "order": 999,
                                        "imdb_id": d_imdb_id,
                                        "douban_id": d_douban_id,
                                        "_is_newly_added": True
                                    }
                                    final_cast_map[tmdb_id_from_find] = new_actor_entry
                                    self.actor_db_manager.upsert_person(
                                        cursor,
                                        {
                                            "tmdb_id": tmdb_id_from_find,
                                            "imdb_id": d_imdb_id,
                                            "douban_id": d_douban_id,
                                            "name": d_actor.get("Name")
                                        }
                                    )
                                match_found = True
                if not match_found:
                    still_unmatched_final.append(d_actor)
            if still_unmatched_final:
                discarded_names = [d.get('Name') for d in still_unmatched_final]
                logger.info(f"--- 最终丢弃 {len(still_unmatched_final)} 位无匹配的豆瓣演员 ---")
            unmatched_douban_actors = still_unmatched_final

        # 将最终演员列表取自 final_cast_map，包含所有旧＋新演员
        current_cast_list = list(final_cast_map.values())

        # ★★★ 在截断前进行一次全量反哺 ★★★
        logger.debug(f"截断前：将 {len(current_cast_list)} 位演员的完整映射关系反哺到数据库...")
        for actor_data in current_cast_list:
            self.actor_db_manager.upsert_person(
                cursor,
                {
                    "tmdb_id": actor_data.get("id"),
                    "name": actor_data.get("name"),
                    "imdb_id": actor_data.get("imdb_id"),
                    "douban_id": actor_data.get("douban_id"),
                },
            )
        logger.trace("所有演员的ID映射关系已保存。")

        # 步骤 演员列表截断 (先截断！)
        max_actors = self.config.get(constants.CONFIG_OPTION_MAX_ACTORS_TO_PROCESS, 30)
        try:
            limit = int(max_actors)
            if limit <= 0:
                limit = 30
        except (ValueError, TypeError):
            limit = 30

        original_count = len(current_cast_list)
        if original_count > limit:
            logger.info(f"演员列表总数 ({original_count}) 超过上限 ({limit})，将在翻译前进行截断。")
            # 按 order 排序
            current_cast_list.sort(key=lambda x: x.get('order') if x.get('order') is not None and x.get('order') >= 0 else 999)
            cast_to_process = current_cast_list[:limit]
        else:
            cast_to_process = current_cast_list

        logger.info(f"将对 {len(cast_to_process)} 位演员进行最终的翻译和格式化处理...")

        # ======================================================================
        # 步骤 B: 翻译准备与执行 (后收集，并检查缓存！)
        # ======================================================================
        ai_translation_succeeded = False
        translation_cache = {}  # ★★★ 核心修正1：将缓存初始化在最外面
        texts_to_collect = set()
        texts_to_send_to_api = set()

        if self.ai_translator and self.config.get(constants.CONFIG_OPTION_AI_TRANSLATION_ENABLED, False):
            logger.info("AI翻译已启用，优先尝试批量翻译模式。")

            try:
                translation_mode = self.config.get(constants.CONFIG_OPTION_AI_TRANSLATION_MODE, "fast")

                for actor in cast_to_process:
                    name = actor.get('name')
                    if name and not utils.contains_chinese(name):
                        texts_to_collect.add(name)

                    character = actor.get('character')
                    if character:
                        cleaned_character = utils.clean_character_name_static(character)
                        if cleaned_character and not utils.contains_chinese(cleaned_character):
                            texts_to_collect.add(cleaned_character)

                if translation_mode == 'fast':
                    logger.debug("[翻译模式] 正在检查全局翻译缓存...")
                    for text in texts_to_collect:
                        cached_entry = self.actor_db_manager.get_translation_from_db(cursor=cursor, text=text)
                        if cached_entry:
                            translation_cache[text] = cached_entry.get("translated_text")
                        else:
                            texts_to_send_to_api.add(text)
                else:
                    logger.debug("[顾问模式] 跳过缓存检查，直接翻译所有词条。")
                    texts_to_send_to_api = texts_to_collect
                if texts_to_send_to_api:
                    item_title = item_details_from_emby.get("Name")
                    item_year = item_details_from_emby.get("ProductionYear")

                    logger.info(f"将 {len(texts_to_send_to_api)} 个词条提交给AI (模式: {translation_mode})。")

                    translation_map_from_api = self.ai_translator.batch_translate(
                        texts=list(texts_to_send_to_api),
                        mode=translation_mode,
                        title=item_title,
                        year=item_year
                    )

                    if translation_map_from_api:
                        translation_cache.update(translation_map_from_api)
                        if translation_mode == 'fast':
                            for original, translated in translation_map_from_api.items():
                                self.actor_db_manager.save_translation_to_db(
                                    cursor=cursor,
                                    original_text=original,
                                    translated_text=translated,
                                    engine_used=self.ai_translator.provider
                                )

                ai_translation_succeeded = True
            except Exception as e:
                logger.error(f"调用AI批量翻译时发生严重错误: {e}", exc_info=True)
                ai_translation_succeeded = False
        else:
            logger.info("AI翻译未启用，将保留演员和角色名原文。")

        # --- ★★★ 核心修正2：无论AI是否成功，都执行清理与回填，降级逻辑只在AI失败时触发 ★★★

        if ai_translation_succeeded:
            logger.info("------------ AI翻译流程成功，开始应用结果 ------------")

            if not texts_to_collect:
                logger.info("  所有演员名和角色名均已是中文，无需翻译。")
            elif not texts_to_send_to_api:
                logger.info(f"  所有 {len(texts_to_collect)} 个待翻译词条均从数据库缓存中获取，无需调用AI。")
            else:
                logger.info(f"  AI翻译完成，共处理 {len(translation_cache)} 个词条。")

            # 无条件执行回填，因为translation_cache包含所有需数据（来自缓存或API）。
            for actor in cast_to_process:
                # 1. 处理演员名
                original_name = actor.get('name')
                translated_name = translation_cache.get(original_name, original_name)
                if original_name != translated_name:
                    logger.debug(f"  演员名翻译: '{original_name}' -> '{translated_name}'")
                actor['name'] = translated_name

                # 2. 处理角色名
                original_character = actor.get('character')
                if original_character:
                    cleaned_character = utils.clean_character_name_static(original_character)
                    translated_character = translation_cache.get(cleaned_character, cleaned_character)
                    if translated_character != original_character:
                        actor_name_for_log = actor.get('name', '未知演员')
                        logger.debug(f"  角色名翻译: '{original_character}' -> '{translated_character}' (演员: {actor_name_for_log})")
                    actor['character'] = translated_character
                else:
                    # 保证字段始终有字符串，避免漏网
                    actor['character'] = ''

            logger.info("----------------------------------------------------")
        else:
            # AI失败时保留原文，不做翻译改写
            if self.config.get(constants.CONFIG_OPTION_AI_TRANSLATION_ENABLED, False):
                logger.warning("AI批量翻译失败，将保留演员和角色名原文。")

        # 3.1 【助理上场】在格式化前，备份所有工牌 (emby_person_id)
        logger.debug("调用 actor_utils.format_and_complete_cast_list 进行最终格式化...")
        is_animation = "Animation" in item_details_from_emby.get("Genres", [])
        final_cast_perfect = actor_utils.format_and_complete_cast_list(
            cast_to_process, is_animation, self.config, mode='auto'
        )

        # 3.2 【助理收尾】直接准备 provider_ids
        # emby_person_id 已经由上一步函数完整地保留下来了
        logger.debug("格式化完成，准备最终的 provider_ids...")
        for actor in final_cast_perfect:
            # 顺便把 provider_ids 准备好，下游函数会用到
            actor["provider_ids"] = {
                "Tmdb": str(actor.get("id")), # 确保是字符串
                "Imdb": actor.get("imdb_id"),
                "Douban": actor.get("douban_id")
            }
            # (可选) 增加一条诊断日志，确认 emby_person_id 真的还在
            if actor.get("emby_person_id"):
                logger.trace(f"  演员 '{actor.get('name')}' 保留了 Emby Person ID: {actor.get('emby_person_id')}")

        return final_cast_perfect

    
    def process_full_library(self, update_status_callback: Optional[callable] = None, force_reprocess_all: bool = False, force_fetch_from_tmdb: bool = False):
        """
        【V3 - 最终完整版】
        这是所有全量处理的唯一入口，它自己处理所有与“强制”相关的逻辑。
        """
        self.clear_stop_signal()
        
        logger.info(f"进入核心执行层: process_full_library, 接收到的 force_reprocess_all = {force_reprocess_all}, force_fetch_from_tmdb = {force_fetch_from_tmdb}")

        if force_reprocess_all:
            logger.info("检测到“强制重处理”选项，正在清空已处理日志...")
            try:
                self.clear_processed_log()
            except Exception as e:
                logger.error(f"在 process_full_library 中清空日志失败: {e}", exc_info=True)
                if update_status_callback: update_status_callback(-1, "清空日志失败")
                return

        # --- ★★★ 补全了这部分代码 ★★★ ---
        libs_to_process_ids = self.config.get("libraries_to_process", [])
        if not libs_to_process_ids:
            logger.warning("未在配置中指定要处理的媒体库。")
            return

        logger.info("正在尝试从Emby获取媒体项目...")
        all_emby_libraries = emby_handler.get_emby_libraries(self.emby_url, self.emby_api_key, self.emby_user_id) or []
        library_name_map = {lib.get('Id'): lib.get('Name', '未知库名') for lib in all_emby_libraries}
        
        movies = emby_handler.get_emby_library_items(self.emby_url, self.emby_api_key, "Movie", self.emby_user_id, libs_to_process_ids, library_name_map=library_name_map) or []
        series = emby_handler.get_emby_library_items(self.emby_url, self.emby_api_key, "Series", self.emby_user_id, libs_to_process_ids, library_name_map=library_name_map) or []
        
        if movies:
            source_movie_lib_names = sorted(list({library_name_map.get(item.get('_SourceLibraryId')) for item in movies if item.get('_SourceLibraryId')}))
            logger.info(f"从媒体库【{', '.join(source_movie_lib_names)}】获取到 {len(movies)} 个电影项目。")

        if series:
            source_series_lib_names = sorted(list({library_name_map.get(item.get('_SourceLibraryId')) for item in series if item.get('_SourceLibraryId')}))
            logger.info(f"从媒体库【{', '.join(source_series_lib_names)}】获取到 {len(series)} 个电视剧项目。")

        all_items = movies + series
        total = len(all_items)
        # --- ★★★ 补全结束 ★★★ ---
        
        if total == 0:
            logger.info("在所有选定的库中未找到任何可处理的项目。")
            if update_status_callback: update_status_callback(100, "未找到可处理的项目。")
            return

        for i, item in enumerate(all_items):
            if self.is_stop_requested(): break
            
            item_id = item.get('Id')
            item_name = item.get('Name', f"ID:{item_id}")

            if not force_reprocess_all and item_id in self.processed_items_cache:
                logger.info(f"正在跳过已处理的项目: {item_name}")
                if update_status_callback:
                    update_status_callback(int(((i + 1) / total) * 100), f"跳过: {item_name}")
                continue

            if update_status_callback:
                update_status_callback(int(((i + 1) / total) * 100), f"处理中 ({i+1}/{total}): {item_name}")
            
            self.process_single_item(
                item_id, 
                force_reprocess_this_item=force_reprocess_all,
                force_fetch_from_tmdb=force_fetch_from_tmdb
            )
            
            time.sleep(float(self.config.get("delay_between_items_sec", 0.5)))
        
        if not self.is_stop_requested() and update_status_callback:
            update_status_callback(100, "全量处理完成")
    # --- 一键翻译 ---
    def translate_cast_list_for_editing(self, 
                                    cast_list: List[Dict[str, Any]], 
                                    title: Optional[str] = None, 
                                    year: Optional[int] = None,
                                    tmdb_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        【V13 - 返璞归真双核版】为手动编辑页面提供的一键翻译功能。
        根据用户配置，智能选择带全局缓存的翻译模式，或无缓存的顾问模式。
        """
        if not cast_list:
            return []
            
        # 从配置中读取模式，这是决定后续所有行为的总开关
        translation_mode = self.config.get(constants.CONFIG_OPTION_AI_TRANSLATION_MODE, "fast")
        
        context_log = f" (上下文: {title} {year})" if title and translation_mode == 'quality' else ""
        logger.info(f"手动编辑-一键翻译：开始批量处理 {len(cast_list)} 位演员 (模式: {translation_mode}){context_log}。")
        
        translated_cast = [dict(actor) for actor in cast_list]
        
        # --- 批量翻译逻辑 ---
        ai_translation_succeeded = False
        
        if self.ai_translator and self.config.get(constants.CONFIG_OPTION_AI_TRANSLATION_ENABLED, False):
            with get_central_db_connection(self.db_path) as conn:
                cursor = conn.cursor()
                
                translation_cache = {} # 本次运行的内存缓存
                texts_to_translate = set()

                # 1. 收集所有需要翻译的词条
                texts_to_collect = set()
                for actor in translated_cast:
                    for field_key in ['name', 'role']:
                        text = actor.get(field_key, '').strip()
                        if field_key == 'role':
                            # 无论是演员名还是角色名，都先清洗一遍，确保拿到的是核心文本
                            # 对于演员名，这个清洗通常无影响，但对于角色名至关重要
                            text = utils.clean_character_name_static(text)
                        if text and not utils.contains_chinese(text):
                            texts_to_collect.add(text)

                # 2. 根据模式决定是否使用缓存
                if translation_mode == 'fast':
                    logger.debug("[翻译模式] 正在检查全局翻译缓存...")
                    for text in texts_to_collect:
                        # 翻译模式只读写全局缓存
                        cached_entry = self.actor_db_manager.get_translation_from_db(cursor=cursor, text=text)
                        if cached_entry:
                            translation_cache[text] = cached_entry.get("translated_text")
                        else:
                            texts_to_translate.add(text)
                else: # 'quality' mode
                    logger.debug("[顾问模式] 跳过缓存检查，直接翻译所有词条。")
                    texts_to_translate = texts_to_collect

                # 3. 如果有需要翻译的词条，调用AI
                if texts_to_translate:
                    logger.info(f"手动编辑-翻译：将 {len(texts_to_translate)} 个词条提交给AI (模式: {translation_mode})。")
                    try:
                        translation_map_from_api = self.ai_translator.batch_translate(
                            texts=list(texts_to_translate),
                            mode=translation_mode,
                            title=title,
                            year=year
                        )
                        if translation_map_from_api:
                            translation_cache.update(translation_map_from_api)
                            
                            # 只有在翻译模式下，才将结果写入全局缓存
                            if translation_mode == 'fast':
                                for original, translated in translation_map_from_api.items():
                                    self.actor_db_manager.save_translation_to_db(
                                        cursor=cursor,
                                        original_text=original, 
                                        translated_text=translated, 
                                        engine_used=self.ai_translator.provider
                                    )
                            
                            ai_translation_succeeded = True
                        else:
                            logger.warning("手动编辑-翻译：AI批量翻译未返回结果。")
                    except Exception as e:
                        logger.error(f"手动编辑-翻译：调用AI批量翻译时出错: {e}", exc_info=True)
                else:
                    logger.info("手动编辑-翻译：所有词条均在缓存中找到，无需调用API。")
                    ai_translation_succeeded = True

                # 4. 回填所有翻译结果
                if translation_cache:
                    for i, actor in enumerate(translated_cast):
                        original_name = actor.get('name', '').strip()
                        if original_name in translation_cache:
                            translated_cast[i]['name'] = translation_cache[original_name]
                        
                        original_role_raw = actor.get('role', '').strip()
                        # 使用与收集时完全相同的清理逻辑
                        cleaned_original_role = utils.clean_character_name_static(original_role_raw)
                        
                        # 用清理后的名字作为key去查找
                        if cleaned_original_role in translation_cache:
                            translated_cast[i]['role'] = translation_cache[cleaned_original_role]
                        
                        # 如果发生了翻译，更新状态以便前端高亮
                        if translated_cast[i].get('name') != actor.get('name') or translated_cast[i].get('role') != actor.get('role'):
                            translated_cast[i]['matchStatus'] = '已翻译'
        
        # 如果AI翻译未启用或失败，则降级到传统引擎
        if not ai_translation_succeeded:
            if self.config.get("ai_translation_enabled", False):
                logger.info("手动编辑-翻译：AI翻译失败，降级到传统引擎逐个翻译。")
            else:
                logger.info("手动编辑-翻译：AI未启用，使用传统引擎逐个翻译。")
                
            try:
                with get_central_db_connection(self.db_path) as conn:
                    cursor = conn.cursor()

                    for i, actor in enumerate(translated_cast):
                        if self.is_stop_requested():
                            logger.warning(f"一键翻译（降级模式）被用户中止。")
                            break # 这里使用 break 更安全，可以直接跳出循环
                        # 【【【 修复点 3：使用正确的参数调用 translate_actor_field 】】】
                        
                        # 翻译演员名
                        name_to_translate = actor.get('name', '').strip()
                        if name_to_translate and not utils.contains_chinese(name_to_translate):
                            translated_name = actor_utils.translate_actor_field(
                                text=name_to_translate,
                                db_manager=self.actor_db_manager,
                                db_cursor=cursor,
                                ai_translator=self.ai_translator,
                                translator_engines=self.translator_engines,
                                ai_enabled=self.ai_enabled
                            )
                            if translated_name and translated_name != name_to_translate:
                                translated_cast[i]['name'] = translated_name

                        # 翻译角色名
                        role_to_translate = actor.get('role', '').strip()
                        if role_to_translate and not utils.contains_chinese(role_to_translate):
                            translated_role = actor_utils.translate_actor_field(
                                text=role_to_translate,
                                db_manager=self.actor_db_manager,
                                db_cursor=cursor,
                                ai_translator=self.ai_translator,
                                translator_engines=self.translator_engines,
                                ai_enabled=self.ai_enabled
                            )
                            if translated_role and translated_role != role_to_translate:
                                translated_cast[i]['role'] = translated_role

                        if translated_cast[i].get('name') != actor.get('name') or translated_cast[i].get('role') != actor.get('role'):
                            translated_cast[i]['matchStatus'] = '已翻译'
            
            except Exception as e:
                logger.error(f"一键翻译（降级模式）时发生错误: {e}", exc_info=True)

        logger.info("手动编辑-翻译完成。")
        return translated_cast
    # ✨✨✨手动处理✨✨✨
    def process_item_with_manual_cast(self, item_id: str, manual_cast_list: List[Dict[str, Any]], item_name: str) -> bool:
        """
        【V5 - 格式化增强版】使用前端提交的轻量级修改，与内存中的完整数据合并，并应用最终的格式化步骤。
        """
        logger.info(f"手动处理流程启动 (后端缓存模式)：ItemID: {item_id} ('{item_name}')")
        try:
            # ✨✨✨ 1. 使用 with 语句，在所有操作开始前获取数据库连接 ✨✨✨
            with get_central_db_connection(self.db_path) as conn:
                cursor = conn.cursor()

                # ✨✨✨ 2. 手动开启一个事务 ✨✨✨
                cursor.execute("BEGIN TRANSACTION;")
                logger.debug(f"手动处理 (ItemID: {item_id}) 的数据库事务已开启。")
            
                try:
                    # ★★★ 1. 从内存缓存中获取这个会话的完整原始演员列表 ★★★
                    original_full_cast = self.manual_edit_cache.get(item_id)
                    if not original_full_cast:
                        raise ValueError(f"在内存缓存中找不到 ItemID {item_id} 的原始演员数据。请重新进入编辑页面。")

                    # 2. 获取基础信息
                    item_details = emby_handler.get_emby_item_details(item_id, self.emby_url, self.emby_api_key, self.emby_user_id)
                    if not item_details: raise ValueError(f"无法获取项目 {item_id} 的详情。")
                    tmdb_id = item_details.get("ProviderIds", {}).get("Tmdb")
                    item_type = item_details.get("Type")
                    if not tmdb_id: raise ValueError(f"项目 {item_id} 缺少 TMDb ID。")

                    # 3. 构建一个以 TMDb ID 为键的原始数据映射表，方便查找
                    reliable_cast_map = {str(actor['id']): actor for actor in original_full_cast if actor.get('id')}

                    # 4. 遍历前端传来的轻量级列表，安全地合并修改，生成一个中间状态的列表
                    intermediate_cast = []
                    for actor_from_frontend in manual_cast_list:
                        frontend_tmdb_id = actor_from_frontend.get("tmdbId")
                        if not frontend_tmdb_id: continue

                        original_actor_data = reliable_cast_map.get(str(frontend_tmdb_id))
                        if not original_actor_data:
                            logger.warning(f"在原始缓存中找不到 TMDb ID {frontend_tmdb_id}，跳过此演员。")
                            continue
                        # 3.1 获取并清理原始角色名和新角色名
                        new_role = actor_from_frontend.get('role', '')      # 从前端数据中获取新角色名
                        original_role = original_actor_data.get('character', '') # 从原始数据中获取旧角色名
                        if new_role != original_role:
                            # 清洗新旧角色名
                            cleaned_original_role = utils.clean_character_name_static(original_role)
                            cleaned_new_role = utils.clean_character_name_static(new_role)

                            # 只有在清洗后的新角色名有效，且与清洗后的旧角色名确实不同时，才进行操作
                            if cleaned_new_role and cleaned_new_role != cleaned_original_role:
                                try:
                                    # 使用“修改前的中文名”（例如 "杰克萨利"）进行反向查找
                                    cache_entry = self.actor_db_manager.get_translation_from_db(
                                        text=cleaned_original_role,
                                        by_translated_text=True,  # <--- 关键！开启反查模式
                                        cursor=cursor
                                    )

                                    # 函数会返回一个包含原文和译文的字典，或者 None
                                    if cache_entry and 'original_text' in cache_entry:
                                        # 如果成功找到了缓存记录，就从中提取出原始的英文 Key
                                        original_text_key = cache_entry['original_text']

                                        # 现在，我们用正确的 Key ("Jake Sully") 和新的 Value ("杰克") 去更新缓存
                                        self.actor_db_manager.save_translation_to_db(
                                            cursor=cursor, # <--- ★★★ 将 cursor 放在正确的位置 ★★★
                                            original_text=original_text_key,
                                            translated_text=cleaned_new_role,
                                            engine_used="manual"
                                        )
                                        logger.debug(f"  AI缓存通过反查更新: '{original_text_key}' -> '{cleaned_new_role}'")
                                        cache_update_succeeded = True
                                    else:
                                        # 如果反查失败（未找到或有其他问题），记录日志并继续
                                        logger.warning(f"无法为修改 '{cleaned_original_role}' -> '{cleaned_new_role}' 更新缓存，因为在缓存中未找到其对应的原文。")

                                except Exception as e_cache:
                                    logger.warning(f"更新AI翻译缓存失败: {e_cache}")
                        
                        updated_actor_data = copy.deepcopy(original_actor_data)
                        role_from_frontend = actor_from_frontend.get('role')
                        cleaned_role = utils.clean_character_name_static(role_from_frontend)
                        
                        updated_actor_data['name'] = actor_from_frontend.get('name')
                        updated_actor_data['character'] = cleaned_role
                        
                        intermediate_cast.append(updated_actor_data)

                    # =================================================================
                    # ★★★ 4.5.【【【 新增核心步骤 】】】★★★
                    #      应用与自动处理流程完全相同的最终格式化和补全逻辑
                    # =================================================================
                    genres = item_details.get("Genres", [])
                    is_animation = "Animation" in genres or "动画" in genres or "Documentary" in genres or "纪录" in genres
                    
                    logger.debug("正在对合并后的演员列表应用最终的格式化、补全和排序...")
                    final_cast_perfect = actor_utils.format_and_complete_cast_list(
                        intermediate_cast, 
                        is_animation, 
                        self.config,
                        mode='manual'
                    )
                    logger.info(f"手动格式化与补全完成，最终演员数量: {len(final_cast_perfect)}。")


                    # =================================================================
                    # ★★★ 5. 【【【 核心逻辑简化：直接修改，不再重建 】】】 ★★★
                    # =================================================================
                    cache_folder_name = "tmdb-movies2" if item_type == "Movie" else "tmdb-tv"
                    base_cache_dir = os.path.join(self.local_data_path, "cache", cache_folder_name, tmdb_id)
                    base_override_dir = os.path.join(self.local_data_path, "override", cache_folder_name, tmdb_id)
                    image_override_dir = os.path.join(base_override_dir, "images")
                    os.makedirs(base_override_dir, exist_ok=True)
                    base_json_filename = "all.json" if item_type == "Movie" else "series.json"
                    
                    # 5.1 读取神医插件生成的原始JSON文件
                    json_file_path = os.path.join(base_cache_dir, base_json_filename)
                    if not os.path.exists(json_file_path):
                        raise FileNotFoundError(f"手动处理失败，因为神医插件的缓存文件不存在: {json_file_path}")
                    
                    base_json_data_original = _read_local_json(json_file_path)
                    if not base_json_data_original:
                        raise ValueError(f"无法读取或解析JSON文件: {json_file_path}")

                    # 5.2 直接在原始数据上替换演员表部分
                    logger.trace("将直接修改缓存元数据中的演员表，并保留所有其他字段。")
                    base_json_data_for_override = base_json_data_original # 直接引用，不再重建

                    if item_type == "Movie":
                        base_json_data_for_override.setdefault("casts", {})["cast"] = final_cast_perfect
                    else: # Series
                        base_json_data_for_override.setdefault("credits", {})["cast"] = final_cast_perfect
                    
                    # 5.3 将修改后的完整JSON写入覆盖文件
                    override_json_path = os.path.join(base_override_dir, base_json_filename)
                    
                    temp_json_path = f"{override_json_path}.{random.randint(1000, 9999)}.tmp"
                    with open(temp_json_path, 'w', encoding='utf-8') as f:
                        json.dump(base_json_data_for_override, f, ensure_ascii=False, indent=4)
                    os.replace(temp_json_path, override_json_path)
                    logger.debug(f"手动处理：成功生成覆盖元数据文件: {override_json_path}")

                    #---处理剧集
                    if item_type == "Series":
                        logger.info(f"手动处理：开始为所有分集注入手动编辑后的演员表...")
                        # (这部分逻辑已经很精简了，直接复用即可)
                        for filename in os.listdir(base_cache_dir):
                            if self.is_stop_requested():
                                logger.warning(f"手动保存的分集处理循环被用户中止。")
                                raise InterruptedError("任务中止")
                            if filename.startswith("season-") and filename.lower().endswith('.json'):
                                child_json_original = _read_local_json(os.path.join(base_cache_dir, filename))
                                if child_json_original:
                                    child_json_for_override = child_json_original # 直接引用
                                    child_json_for_override.setdefault("credits", {})["cast"] = final_cast_perfect
                                    child_json_for_override["guest_stars"] = []
                                    override_child_path = os.path.join(base_override_dir, filename)
                                    try:
                                        # 使用原子写入，防止意外中断
                                        temp_child_path = f"{override_child_path}.{random.randint(1000, 9999)}.tmp"
                                        with open(temp_child_path, 'w', encoding='utf-8') as f:
                                            json.dump(child_json_for_override, f, ensure_ascii=False, indent=4)
                                        os.replace(temp_child_path, override_child_path)
                                    except Exception as e:
                                        logger.error(f"手动处理：写入子项目JSON失败: {override_child_path}, {e}")
                                        if os.path.exists(temp_child_path): os.remove(temp_child_path)

                    #---同步图片 
                    if self.sync_images_enabled:
                        if self.is_stop_requested(): raise InterruptedError("任务被中止")
                        
                        # 直接调用我们新的、可复用的图片同步方法
                        # 注意：item_details_from_emby 就是它需要的参数
                        self.sync_item_images(item_details)

                    logger.info(f"手动处理：准备刷新 Emby 项目 {item_name}...")
                    refresh_success = emby_handler.refresh_emby_item_metadata(
                        item_emby_id=item_id,
                        emby_server_url=self.emby_url,
                        emby_api_key=self.emby_api_key,
                        replace_all_metadata_param=True,
                        item_name_for_log=item_name,
                        user_id_for_unlock=self.emby_user_id
                    )
                    if not refresh_success:
                        logger.warning(f"手动处理：文件已生成，但触发 Emby 刷新失败。你可能需要稍后在 Emby 中手动刷新。")

                    # 更新处理日志
                    self.log_db_manager.save_to_processed_log(cursor, item_id, item_name, score=10.0) # 手动处理直接给满分
                    self.log_db_manager.remove_from_failed_log(cursor, item_id)
                    
                    # ✨✨✨ 提交事务 ✨✨✨
                    conn.commit()
                    logger.info(f"✅ 手动处理 '{item_name}' 流程完成，数据库事务已提交。")
                    return True
                
                except Exception as inner_e:
                    # 如果在事务中发生任何错误，回滚
                    logger.error(f"手动处理事务中发生错误 for {item_name}: {inner_e}", exc_info=True)
                    conn.rollback()
                    logger.warning(f"由于发生错误，针对 '{item_name}' 的数据库更改已回滚。")
                    # 重新抛出，让外层捕获
                    raise

        except Exception as outer_e:
            logger.error(f"手动处理 '{item_name}' 时发生顶层错误: {outer_e}", exc_info=True)
            return False
        finally:
            # ★★★ 清理本次编辑会话的缓存 ★★★
            if item_id in self.manual_edit_cache:
                del self.manual_edit_cache[item_id]
                logger.debug(f"已清理 ItemID {item_id} 的内存缓存。")
        if cache_update_succeeded:
            updated_cache_count += 1
    # --- 从本地 cache 文件获取演员列表用于编辑 ---
    def get_cast_for_editing(self, item_id: str) -> Optional[Dict[str, Any]]:
        """
        【V5 - 最终版】为手动编辑准备数据。
        1. 根据类型，从本地 cache 加载最完整的演员列表（电影直接加载，电视剧智能聚合）。
        2. 将完整列表缓存在内存中，供后续保存时使用。
        3. 只向前端发送轻量级数据 (ID, name, role, profile_path)。
        """
        logger.info(f"为编辑页面准备数据 (后端缓存模式)：ItemID {item_id}")
        
        try:
            # 1. 获取基础信息
            emby_details = emby_handler.get_emby_item_details(item_id, self.emby_url, self.emby_api_key, self.emby_user_id)
            if not emby_details: raise ValueError(f"在Emby中未找到项目 {item_id}")

            tmdb_id = emby_details.get("ProviderIds", {}).get("Tmdb")
            item_type = emby_details.get("Type")
            item_name_for_log = emby_details.get("Name", f"未知(ID:{item_id})")
            if not tmdb_id: raise ValueError(f"项目 {item_id} 缺少 TMDb ID")

            # 2. 从本地 cache 文件读取最可靠的演员列表
            cache_folder_name = "tmdb-movies2" if item_type == "Movie" else "tmdb-tv"
            base_cache_dir = os.path.join(self.local_data_path, "cache", cache_folder_name, tmdb_id)
            base_json_filename = "all.json" if item_type == "Movie" else "series.json"
            full_cast_from_cache = []
            
            if item_type == "Movie":
                tmdb_data = _read_local_json(os.path.join(base_cache_dir, base_json_filename))
                if not tmdb_data: raise ValueError("未找到本地 TMDb 缓存文件")
                full_cast_from_cache = tmdb_data.get("casts", {}).get("cast", [])
            
            elif item_type == "Series":
                # ✨✨✨ 同样直接调用新的聚合函数 ✨✨✨
                full_cast_from_cache = self._aggregate_series_cast_from_cache(
                    base_cache_dir=base_cache_dir,
                    item_name_for_log=item_name_for_log
                )

            # 3. 将完整的演员列表存入内存缓存
            self.manual_edit_cache[item_id] = full_cast_from_cache
            logger.debug(f"已为 ItemID {item_id} 缓存了 {len(full_cast_from_cache)} 条完整演员数据。")

            # 4. 构建并发送“轻量级”数据给前端
            cast_for_frontend = []
            for actor_data in full_cast_from_cache:
                actor_tmdb_id = actor_data.get('id')
                if not actor_tmdb_id: continue
                
                profile_path = actor_data.get('profile_path')
                image_url = f"https://image.tmdb.org/t/p/w185{profile_path}" if profile_path else None

                # ✨✨✨ 1. 获取从缓存文件中读出的、可能带有前缀的角色名
                role_from_cache = actor_data.get('character', '')
                
                # ✨✨✨ 2. 调用清理函数，得到干净的角色名
                cleaned_role_for_display = utils.clean_character_name_static(role_from_cache)

                cast_for_frontend.append({
                    "tmdbId": actor_tmdb_id,
                    "name": actor_data.get('name'),
                    "role": cleaned_role_for_display,
                    "imageUrl": image_url,
                })
            
            # 5. 获取失败日志信息和组合 response_data
            failed_log_info = {}
            with get_central_db_connection(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT error_message, score FROM failed_log WHERE item_id = ?", (item_id,))
                row = cursor.fetchone()
                if row: failed_log_info = dict(row)

            response_data = {
                "item_id": item_id,
                "item_name": emby_details.get("Name"),
                "item_type": item_type,
                "image_tag": emby_details.get('ImageTags', {}).get('Primary'),
                "original_score": failed_log_info.get("score"),
                "review_reason": failed_log_info.get("error_message"),
                "current_emby_cast": cast_for_frontend,
                "search_links": {
                    "google_search_wiki": utils.generate_search_url('wikipedia', emby_details.get("Name"), emby_details.get("ProductionYear"))
                }
            }
            return response_data

        except Exception as e:
            logger.error(f"获取编辑数据失败 for ItemID {item_id}: {e}", exc_info=True)
            return None
    # ★★★ 全量图片同步的核心逻辑 ★★★
    def sync_all_images(self, update_status_callback: Optional[callable] = None):
        """
        【最终正确版】遍历所有已处理的媒体项，将它们在 Emby 中的当前图片下载到本地 override 目录。
        """
        logger.info("--- 开始执行全量海报同步任务 ---")
        
        try:
            with get_central_db_connection(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT item_id, item_name FROM processed_log")
                items_to_process = cursor.fetchall()
        except Exception as e:
            logger.error(f"获取已处理项目列表时发生数据库错误: {e}", exc_info=True)
            if update_status_callback:
                update_status_callback(-1, "数据库错误")
            return

        total = len(items_to_process)
        if total == 0:
            logger.info("没有已处理的项目，无需同步图片。")
            if update_status_callback:
                update_status_callback(100, "没有项目")
            return

        logger.info(f"共找到 {total} 个已处理项目需要同步图片。")

        for i, db_row in enumerate(items_to_process):
            if self.is_stop_requested():
                logger.info("全量图片同步任务被中止。")
                break

            item_id = db_row['item_id']
            item_name_from_db = db_row['item_name']
            
            if not item_id:
                logger.warning(f"数据库中发现一条没有 item_id 的记录，跳过。Name: {item_name_from_db}")
                continue

            if update_status_callback:
                update_status_callback(int((i / total) * 100), f"同步图片 ({i+1}/{total}): {item_name_from_db}")

            try:
                item_details = emby_handler.get_emby_item_details(item_id, self.emby_url, self.emby_api_key, self.emby_user_id)
                
                if not item_details:
                    logger.warning(f"跳过 {item_name_from_db} (ID: {item_id})，无法从 Emby 获取其详情。")
                    continue

                tmdb_id = item_details.get("ProviderIds", {}).get("Tmdb")
                item_type = item_details.get("Type")
                
                if not tmdb_id:
                    logger.warning(f"跳过 '{item_name_from_db}'，因为它缺少 TMDb ID。")
                    continue
                override_path = utils.get_override_path_for_item(item_type, tmdb_id, self.config)

                if not override_path:
                    logger.warning(f"跳过 '{item_name_from_db}'，无法为其生成有效的 override 路径 (可能是未知类型或配置问题)。")
                    continue

                image_override_dir = os.path.join(override_path, "images")
                os.makedirs(image_override_dir, exist_ok=True)

                image_map = {"Primary": "poster.jpg", "Backdrop": "fanart.jpg", "Logo": "clearlogo.png"}
                if item_type == "Movie":
                    image_map["Thumb"] = "landscape.jpg"
                
                logger.debug(f"项目 '{item_name_from_db}': 准备下载图片集到 '{image_override_dir}'")

                for image_type, filename in image_map.items():
                    emby_handler.download_emby_image(
                        item_id, 
                        image_type, 
                        os.path.join(image_override_dir, filename), 
                        self.emby_url, 
                        self.emby_api_key
                    )
                
                if item_type == "Series":
                    logger.info(f"开始为剧集 '{item_name_from_db}' 同步季海报...")
                    children = emby_handler.get_series_children(item_id, self.emby_url, self.emby_api_key, self.emby_user_id) or []
                    
                    for child in children:
                        # 只处理类型为 "Season" 的子项目，完全忽略 "Episode"
                        if child.get("Type") == "Season":
                            season_number = child.get("IndexNumber")
                            if season_number is not None:
                                logger.info(f"  正在同步第 {season_number} 季的海报...")
                                emby_handler.download_emby_image(
                                    child.get("Id"), 
                                    "Primary", # 季项目通常只有 Primary 图片
                                    os.path.join(image_override_dir, f"season-{season_number}.jpg"),
                                    self.emby_url, 
                                    self.emby_api_key
                                )
                
                logger.info(f"成功同步了 '{item_name_from_db}' 的图片。")

            except Exception as e:
                logger.error(f"同步项目 '{item_name_from_db}' (ID: {item_id}) 的图片时发生错误: {e}", exc_info=True)
            
            time.sleep(0.2)

        logger.info("--- 全量海报同步任务结束 ---")
    # --- 图片同步 ---
    def sync_item_images(self, item_details: Dict[str, Any], update_description: Optional[str] = None) -> bool:
        """
        【新增-重构】这个方法负责同步一个媒体项目的所有相关图片。
        它从 _process_item_core_logic 中提取出来，以便复用。
        """
        item_id = item_details.get("Id")
        item_type = item_details.get("Type")
        item_name_for_log = item_details.get("Name", f"未知项目(ID:{item_id})")
        
        if not all([item_id, item_type, self.local_data_path]):
            logger.error(f"[图片同步] 跳过 '{item_name_for_log}'，因为缺少ID、类型或未配置本地数据路径。")
            return False

        try:
            # --- 准备工作 (目录、TMDb ID等) ---
            log_prefix = "[图片同步]"
            tmdb_id = item_details.get("ProviderIds", {}).get("Tmdb")
            if not tmdb_id:
                logger.warning(f"{log_prefix} 项目 '{item_name_for_log}' 缺少TMDb ID，无法确定覆盖目录，跳过。")
                return False
            
            cache_folder_name = "tmdb-movies2" if item_type == "Movie" else "tmdb-tv"
            base_override_dir = os.path.join(self.local_data_path, "override", cache_folder_name, tmdb_id)
            image_override_dir = os.path.join(base_override_dir, "images")
            os.makedirs(image_override_dir, exist_ok=True)

            # --- 定义所有可能的图片映射 ---
            full_image_map = {"Primary": "poster.jpg", "Backdrop": "fanart.jpg", "Logo": "clearlogo.png"}
            if item_type == "Movie":
                full_image_map["Thumb"] = "landscape.jpg"

            # ★★★ 全新逻辑分发 ★★★
            images_to_sync = {}
            
            # 模式一：精准同步 (当描述存在时)
            if update_description:
                log_prefix = "[精准图片同步]"
                logger.debug(f"{log_prefix} 正在解析描述: '{update_description}'")
                
                # 定义关键词到Emby图片类型的映射 (使用小写以方便匹配)
                keyword_map = {
                    "primary": "Primary",
                    "backdrop": "Backdrop",
                    "logo": "Logo",
                    "thumb": "Thumb", # 电影缩略图
                    "banner": "Banner" # 剧集横幅 (如果需要可以添加)
                }
                
                desc_lower = update_description.lower()
                found_specific_image = False
                for keyword, image_type_api in keyword_map.items():
                    if keyword in desc_lower and image_type_api in full_image_map:
                        images_to_sync[image_type_api] = full_image_map[image_type_api]
                        logger.debug(f"{log_prefix} 匹配到关键词 '{keyword}'，将只同步 {image_type_api} 图片。")
                        found_specific_image = True
                        break # 找到第一个匹配就停止，避免重复
                
                if not found_specific_image:
                    logger.warning(f"{log_prefix} 未能在描述中找到可识别的图片关键词，将回退到完全同步。")
                    images_to_sync = full_image_map # 回退
            
            # 模式二：完全同步 (默认或回退)
            else:
                log_prefix = "[完整图片同步]"
                logger.debug(f"{log_prefix} 未提供更新描述，将同步所有类型的图片。")
                images_to_sync = full_image_map

            # --- 执行下载 ---
            logger.info(f"{log_prefix} 开始为 '{item_name_for_log}' 下载 {len(images_to_sync)} 张图片至 {image_override_dir}...")
            for image_type, filename in images_to_sync.items():
                if self.is_stop_requested():
                    logger.warning(f"{log_prefix} 收到停止信号，中止图片下载。")
                    return False
                emby_handler.download_emby_image(item_id, image_type, os.path.join(image_override_dir, filename), self.emby_url, self.emby_api_key)
            
            # --- 分集图片逻辑 (只有在完全同步时才考虑执行) ---
            if images_to_sync == full_image_map and item_type == "Series":
            
                children = emby_handler.get_series_children(item_id, self.emby_url, self.emby_api_key, self.emby_user_id, series_name_for_log=item_name_for_log) or []
                for child in children:
                    if self.is_stop_requested():
                        logger.warning(f"{log_prefix} 收到停止信号，中止子项目图片下载。")
                        return False
                    child_type, child_id = child.get("Type"), child.get("Id")
                    if child_type == "Season":
                        season_number = child.get("IndexNumber")
                        if season_number is not None:
                            emby_handler.download_emby_image(child_id, "Primary", os.path.join(image_override_dir, f"season-{season_number}.jpg"), self.emby_url, self.emby_api_key)
                    elif child_type == "Episode":
                        season_number, episode_number = child.get("ParentIndexNumber"), child.get("IndexNumber")
                        if season_number is not None and episode_number is not None:
                            emby_handler.download_emby_image(child_id, "Primary", os.path.join(image_override_dir, f"season-{season_number}-episode-{episode_number}.jpg"), self.emby_url, self.emby_api_key)
            
            logger.info(f"{log_prefix} ✅ 成功完成 '{item_name_for_log}' 的图片同步。")
            return True
        except Exception as e:
            logger.error(f"{log_prefix} 为 '{item_name_for_log}' 同步图片时发生未知错误: {e}", exc_info=True)
            return False
    # --- 聚合演员表 ---
    def _aggregate_series_cast_from_cache(self, base_cache_dir: str, item_name_for_log: str) -> List[Dict[str, Any]]:
        """
        【V3 - 最终修复版】聚合一个剧集所有本地缓存JSON文件中的演员列表。

        此函数会扫描指定TMDb缓存目录，读取series.json、所有season-*.json和
        season-*-episode-*.json文件，提取其中的演员和客串演员，
        然后去重并形成一个完整的演员列表。

        Args:
            base_cache_dir (str): 剧集的TMDb缓存根目录路径。
            item_name_for_log (str): 用于日志记录的媒体项目名称。

        Returns:
            List[Dict[str, Any]]: 聚合、去重并排序后的完整演员列表。
        """
        logger.info(f"【演员聚合】开始为 '{item_name_for_log}' 聚合所有JSON文件中的演员...")
        
        aggregated_cast_map = {}
        
        # 1. 优先处理主文件
        base_json_filename = "series.json"
        main_series_json_path = os.path.join(base_cache_dir, base_json_filename)
        
        main_data = _read_local_json(main_series_json_path)
        if main_data:
            # 主演列表的优先级最高
            main_cast = main_data.get("credits", {}).get("cast", [])
            for actor in main_cast:
                actor_id = actor.get("id")
                if actor_id:
                    aggregated_cast_map[actor_id] = actor
            logger.debug(f"  -> 从 {base_json_filename} 中加载了 {len(aggregated_cast_map)} 位主演员。")
        else:
            logger.warning(f"  -> 未找到主剧集文件: {main_series_json_path}，将只处理子文件。")

        # 2. 扫描并聚合所有子文件（分季、分集）
        try:
            # 获取所有需要处理的子文件名
            child_json_files = [
                f for f in os.listdir(base_cache_dir) 
                if f != base_json_filename and f.startswith("season-") and f.lower().endswith(".json")
            ]
            
            if child_json_files:
                logger.debug(f"  -> 发现 {len(child_json_files)} 个额外的季/集JSON文件需要处理。")

                for json_filename in sorted(child_json_files):
                    file_path = os.path.join(base_cache_dir, json_filename)
                    child_data = _read_local_json(file_path)
                    if not child_data:
                        continue

                    # ✨✨✨ 核心修复：从 "credits" 对象中安全地获取 cast 和 guest_stars ✨✨✨
                    credits_data = child_data.get("credits", {})
                    
                    # 将两个列表安全地合并成一个待处理列表
                    actors_to_process = credits_data.get("cast", []) + credits_data.get("guest_stars", [])
                    
                    if not actors_to_process:
                        continue

                    # 遍历并添加新演员
                    for actor in actors_to_process:
                        actor_id = actor.get("id")
                        # 确保演员有ID
                        if not actor_id:
                            continue
                        
                        # 如果演员ID还未记录，就添加他/她
                        if actor_id not in aggregated_cast_map:
                            # ✨ 新增调试日志，用于追踪
                            logger.trace(f"    -> 新增演员 (ID: {actor_id}): {actor.get('name')}")
                            
                            # 为客串演员设置一个默认的高 'order' 值，确保他们排在主演后面
                            if 'order' not in actor:
                                actor['order'] = 999 
                            aggregated_cast_map[actor_id] = actor

        except FileNotFoundError:
            logger.warning(f"  -> 缓存目录 {base_cache_dir} 不存在，无法聚合子项目演员。")
        
        # 3. 将最终结果从字典转为列表并排序
        full_aggregated_cast = list(aggregated_cast_map.values())
        full_aggregated_cast.sort(key=lambda x: x.get('order', 999))
        
        logger.info(f"【演员聚合】完成。共为 '{item_name_for_log}' 聚合了 {len(full_aggregated_cast)} 位独立演员。")
        
        return full_aggregated_cast
    
    def close(self):
        if self.douban_api: self.douban_api.close()
