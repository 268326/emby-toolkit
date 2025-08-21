# db_handler.py
import sqlite3
import json
from datetime import date, timedelta, datetime
import logging
from typing import Optional, Dict, Any, List, Tuple
from flask import jsonify
import emby_handler
from utils import contains_chinese
logger = logging.getLogger(__name__)

# ======================================================================
# 模块 1: 数据库管理器 (The Unified Data Access Layer)
# ======================================================================

# --- 数据库表结构 ---
TABLE_PRIMARY_KEYS = {
    "person_identity_map": "tmdb_person_id",
    "ActorMetadata": "tmdb_id",
    "translation_cache": "original_text",
    "collections_info": "emby_collection_id",
    "watchlist": "item_id",
    "actor_subscriptions": "tmdb_person_id",
    "tracked_actor_media": ("subscription_id", "tmdb_media_id"),
    "processed_log": "item_id",
    "failed_log": "item_id",
    "users": "username",
    "custom_collections": "name", 
    "media_metadata": ("tmdb_id", "item_type"),
}
# --- 状态中文翻译字典 ---
STATUS_TRANSLATION_MAP = {
    'in_library': '已入库',
    'subscribed': '已订阅',
    'missing': '缺失',
    'unreleased': '未上映'
}
def get_db_connection(db_path: str) -> sqlite3.Connection:
    """
    【中央函数】获取一个配置好 WAL 模式和 row_factory 的数据库连接。
    这是整个应用获取数据库连接的唯一入口。
    """
    if not db_path:
        logger.error("尝试获取数据库连接，但未提供 db_path。")
        raise ValueError("数据库路径 (db_path) 不能为空。")
        
    try:
        conn = sqlite3.connect(db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        return conn
    except sqlite3.Error as e:
        logger.error(f"获取数据库连接失败: {e}", exc_info=True)
        raise

# ======================================================================
# 模块 2: 演员数据访问层 (Actor Data Access Layer)
# ======================================================================

class ActorDBManager:
    """
    一个专门负责与演员身份相关的数据库表进行交互的类。
    这是所有数据库操作的唯一入口，确保逻辑统一。
    """
    def __init__(self, db_path: str):
        self.db_path = db_path
        logger.trace(f"ActorDBManager 初始化，使用数据库: {self.db_path}")

    def get_translation_from_db(self, cursor: sqlite3.Cursor, text: str, by_translated_text: bool = False) -> Optional[Dict[str, Any]]:
        """
        【V2 - 自我净化版】从数据库获取翻译缓存。
        如果发现缓存中的译文不含中文（历史遗留的坏数据），
        则会顺手将其销毁并返回 None，触发重新翻译。
        
        :param cursor: 必须提供外部数据库游标。
        :param text: 要查询的文本 (可以是原文或译文)。
        :param by_translated_text: 如果为 True，则通过译文反查原文。
        :return: 包含原文、译文和引擎的字典，或 None。
        """
        try:
            if by_translated_text:
                sql = "SELECT original_text, translated_text, engine_used FROM translation_cache WHERE translated_text = ?"
            else:
                sql = "SELECT original_text, translated_text, engine_used FROM translation_cache WHERE original_text = ?"

            cursor.execute(sql, (text,))
            row = cursor.fetchone()

            if not row:
                # 1. 如果本来就没找到，直接返回 None
                return None

            # ★★★ 核心修改：在这里加入“质检与销毁”逻辑 ★★★
            translated_text = row['translated_text']
            
            # 2. 质检：检查翻译结果是否合格
            #    - translated_text 存在
            #    - 且它不是中文
            if translated_text and not contains_chinese(translated_text):
                original_text_key = row['original_text']
                logger.warning(
                    f"发现无效的历史翻译缓存: '{original_text_key}' -> '{translated_text}'。将自动销毁此记录以待重新翻译。"
                )
                
                # 3. 销毁：从数据库中删除这条坏记录
                try:
                    # 我们使用 original_text 作为主键来删除，这是最可靠的
                    cursor.execute("DELETE FROM translation_cache WHERE original_text = ?", (original_text_key,))
                    # 注意：这里不需要 commit，因为这个函数总是在一个更大的事务中被调用。
                    # 事务的提交由外层逻辑（如 _process_cast_list_from_api）负责。
                except Exception as e_delete:
                    logger.error(f"销毁无效缓存 '{original_text_key}' 时失败: {e_delete}")

                # 4. 报告：向上层假装没找到，这样就能触发重新翻译
                return None
            
            # 5. 如果质检通过，正常返回结果
            return dict(row)

        except Exception as e:
            logger.error(f"DB读取翻译缓存时发生错误 for '{text}': {e}", exc_info=True)
            return None

    def save_translation_to_db(self, cursor: sqlite3.Cursor, original_text: str, translated_text: Optional[str], engine_used: Optional[str]):
        """
        【V2 - 增加中文校验】将翻译结果保存到数据库。
        在保存前会检查译文是否包含中文字符，如果不包含则丢弃。
        :param cursor: 必须提供外部数据库游标。
        """
        # ★★★ 核心修改：在保存前进行校验 ★★★
        # 1. 如果译文非空
        if translated_text and translated_text.strip():
            # 2. 检查是否包含中文字符
            if not contains_chinese(translated_text):
                # 3. 如果不包含，则记录日志并直接返回，不保存到数据库
                logger.warning(
                    f"翻译结果 '{translated_text}' 不含中文，已丢弃，不写入缓存。原文: '{original_text}'"
                )
                return # 提前退出函数

        # 如果校验通过（例如译文为空或包含中文），则执行保存操作
        try:
            cursor.execute(
                "REPLACE INTO translation_cache (original_text, translated_text, engine_used, last_updated_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
                (original_text, translated_text, engine_used)
            )
            logger.trace(f"翻译缓存存DB: '{original_text}' -> '{translated_text}' (引擎: {engine_used})")
        except Exception as e:
            logger.error(f"DB保存翻译缓存失败 for '{original_text}': {e}", exc_info=True)

    def find_person_by_any_id(self, cursor: sqlite3.Cursor, **kwargs) -> Optional[sqlite3.Row]:
        search_criteria = [
            ("tmdb_person_id", kwargs.get("tmdb_id")),
            ("emby_person_id", kwargs.get("emby_id")),
            ("imdb_id", kwargs.get("imdb_id")),
            ("douban_celebrity_id", kwargs.get("douban_celebrity_id")),
        ]
        for column, value in search_criteria:
            if not value: continue
            try:
                cursor.execute(f"SELECT * FROM person_identity_map WHERE {column} = ?", (value,))
                result = cursor.fetchone()
                if result:
                    logger.debug(f"通过 {column}='{value}' 找到了演员记录 (map_id: {result['map_id']})。")
                    return result
            except sqlite3.Error as e:
                logger.error(f"查询 person_identity_map 时出错 ({column}={value}): {e}")
        return None

    def upsert_person(self, cursor: sqlite3.Cursor, person_data: Dict[str, Any], **kwargs):
        """
        【V8 - 最终安全版 Pro】
        在V7基础上增加最终更新前的一致性检查，完美处理因数据污染导致的跨记录ID冲突。
        1. 优先通过所有可用ID进行精确查找。
        2. 仅在ID查找失败时，才通过名字进行辅助查找。
        3. 对名字匹配的结果进行严格的ID冲突检查。
        4. 【新增】在执行UPDATE前，对最终合并的数据进行一次全面的唯一性冲突预检查。
        5. 如果预检查发现冲突，则放弃本次操作并记录警告，避免程序崩溃。
        6. 最终确认安全后，才进行合并更新或创建新记录。
        """
        # 1. 标准化输入数据
        new_data = {
            "primary_name": str(person_data.get("name") or '').strip(),
            "emby_person_id": str(person_data.get("emby_id") or '').strip() or None,
            "tmdb_person_id": str(person_data.get("tmdb_id") or '').strip() or None,
            "imdb_id": str(person_data.get("imdb_id") or '').strip() or None,
            "douban_celebrity_id": str(person_data.get("douban_id") or '').strip() or None,
        }
        id_fields = ["emby_person_id", "tmdb_person_id", "imdb_id", "douban_celebrity_id"]
        
        new_ids = {k: v for k, v in new_data.items() if k in id_fields and v}

        if not new_data["primary_name"] and not new_ids:
            return -1

        existing_record = None
        
        # ======================================================================
        # 策略层 1: 通过所有提供的 ID 进行精确查找
        # ======================================================================
        if new_ids:
            query_parts = [f"{key} = ?" for key in new_ids.keys()]
            query_values = list(new_ids.values())
            sql_find_by_id = f"SELECT * FROM person_identity_map WHERE {' OR '.join(query_parts)}"
            cursor.execute(sql_find_by_id, tuple(query_values))
            
            found_by_id = cursor.fetchone()
            if found_by_id:
                existing_record = dict(found_by_id)
                # logger.trace(f"通过ID找到匹配记录 (map_id: {existing_record['map_id']})，准备合并。")

        # ======================================================================
        # 策略层 2: 仅在ID查找失败时，才通过名字进行辅助查找
        # ======================================================================
        if not existing_record and new_data["primary_name"]:
            cursor.execute("SELECT * FROM person_identity_map WHERE primary_name = ?", (new_data["primary_name"],))
            found_by_name = cursor.fetchone()

            if found_by_name:
                # logger.trace(f"未通过ID找到匹配，但通过名字 '{new_data['primary_name']}' 找到候选记录 (map_id: {found_by_name['map_id']})。")
                
                is_safe_to_merge = True
                for key, new_id_val in new_ids.items():
                    existing_id_val = found_by_name[key]
                    if existing_id_val and new_id_val != existing_id_val:
                        # logger.warning(
                        #     f"检测到同名异人！新数据 '{new_data['primary_name']}' ({key}: {new_id_val}) "
                        #     f"与数据库记录 (map_id: {found_by_name['map_id']}) 的 ({key}: {existing_id_val}) 冲突。将创建新记录。"
                        # )
                        is_safe_to_merge = False
                        break
                
                if is_safe_to_merge:
                    # logger.trace("安全检查通过，确认为同一个人，准备合并。")
                    existing_record = dict(found_by_name)

        # ======================================================================
        # 执行层: 根据查找结果，执行更新或插入
        # ======================================================================
        try:
            if existing_record:
                # --- 合并与更新 ---
                merged_data = existing_record.copy()
                for key, value in new_data.items():
                    if value:
                        merged_data[key] = value
                
                # ======================================================================
                # 【核心改动】最终更新前的冲突预检查
                # 检查即将更新的这些ID，是否已经存在于数据库的 *其他* 记录中
                # ======================================================================
                merged_ids = {k: v for k, v in merged_data.items() if k in id_fields and v}
                if merged_ids:
                    conflict_check_parts = [f"{key} = ?" for key in merged_ids.keys()]
                    conflict_check_values = list(merged_ids.values())
                    
                    # 查询条件要排除当前正在操作的记录 (existing_record['map_id'])
                    sql_conflict_check = f"SELECT map_id, primary_name FROM person_identity_map WHERE ({' OR '.join(conflict_check_parts)}) AND map_id != ?"
                    conflict_check_values.append(existing_record['map_id'])
                    
                    cursor.execute(sql_conflict_check, tuple(conflict_check_values))
                    conflicting_record = cursor.fetchone()

                    if conflicting_record:
                        # logger.error(
                        #     f"数据更新被中止！为演员 '{merged_data['primary_name']}' (map_id: {existing_record['map_id']}) 合并数据时，"
                        #     f"发现其ID与另一条记录 (map_id: {conflicting_record['map_id']}, name: '{conflicting_record['primary_name']}') 存在UNIQUE冲突。"
                        #     f"这通常是由于数据库中存在重复的脏数据导致。本次更新将被忽略。"
                        # )
                        # 按要求忽略本次操作，直接返回现有记录的ID
                        return existing_record['map_id']
                # --- 预检查结束 ---

                update_clauses = [f"{key} = ?" for key in merged_data.keys() if key != 'map_id']
                update_values = [v for k, v in merged_data.items() if k != 'map_id']
                update_values.append(existing_record['map_id'])

                sql_update = f"UPDATE person_identity_map SET {', '.join(update_clauses)}, last_updated_at = CURRENT_TIMESTAMP WHERE map_id = ?"
                cursor.execute(sql_update, tuple(update_values))
                return existing_record['map_id']
            else:
                # --- 创建新记录 ---
                # logger.trace(f"未找到任何可安全合并的记录，为 '{new_data['primary_name']}' 创建新条目。")
                
                # 【可选优化】在创建新记录前，也进行一次冲突检查，避免插入已存在的ID
                if new_ids:
                    conflict_check_parts = [f"{key} = ?" for key in new_ids.keys()]
                    sql_conflict_check = f"SELECT map_id, primary_name FROM person_identity_map WHERE {' OR '.join(conflict_check_parts)}"
                    cursor.execute(sql_conflict_check, tuple(new_ids.values()))
                    conflicting_record = cursor.fetchone()
                    if conflicting_record:
                        # logger.error(
                        #     f"数据插入被中止！尝试为 '{new_data['primary_name']}' 创建新记录时，"
                        #     f"发现其ID已存在于记录 (map_id: {conflicting_record['map_id']}, name: '{conflicting_record['primary_name']}')。"
                        #     f"本次插入将被忽略。"
                        # )
                        return conflicting_record['map_id'] # 返回已存在记录的ID

                cols_to_insert = list(new_data.keys())
                vals_to_insert = list(new_data.values())

                cols_to_insert.extend(["last_synced_at", "last_updated_at"])
                placeholders = ["?" for _ in vals_to_insert] + ["CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP"]

                sql_insert = f"INSERT INTO person_identity_map ({', '.join(cols_to_insert)}) VALUES ({', '.join(placeholders)})"
                cursor.execute(sql_insert, tuple(vals_to_insert))
                return cursor.lastrowid

        except sqlite3.Error as e:
            # logger.error(f"为演员 '{new_data.get('primary_name')}' 执行 upsert 操作时发生数据库错误: {e}", exc_info=True)
            # 尽管我们加了预检查，但为了以防万一（例如并发操作），保留这个异常捕获是好习惯。
            raise
        
# ======================================================================
# 模块 3: 日志表数据访问 (Log Tables Data Access)
# ======================================================================

def get_review_items_paginated(db_path: str, page: int, per_page: int, query_filter: str) -> Tuple[List, int]:
    """
    从数据库获取待复核项目列表（分页）。
    返回 (项目列表, 总项目数)。
    """
    offset = (page - 1) * per_page
    items_to_review = []
    total_matching_items = 0
    
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            
            where_clause = ""
            sql_params = []

            if query_filter:
                where_clause = "WHERE item_name LIKE ?"
                sql_params.append(f"%{query_filter}%")

            count_sql = f"SELECT COUNT(*) FROM failed_log {where_clause}"
            cursor.execute(count_sql, tuple(sql_params))
            count_row = cursor.fetchone()
            if count_row:
                total_matching_items = count_row[0]

            items_sql = f"""
                SELECT item_id, item_name, failed_at, reason, item_type, score 
                FROM failed_log 
                {where_clause}
                ORDER BY failed_at DESC 
                LIMIT ? OFFSET ?
            """
            params_for_page_query = sql_params + [per_page, offset]
            cursor.execute(items_sql, tuple(params_for_page_query))
            
            items_to_review = [dict(row) for row in cursor.fetchall()]
            
        return items_to_review, total_matching_items
    except Exception as e:
        logger.error(f"DB: 获取待复核列表失败: {e}", exc_info=True)
        # 向上抛出异常，让调用者处理
        raise


def mark_review_item_as_processed(db_path: str, item_id: str) -> bool:
    """
    将单个待复核项目标记为已处理。
    - 从 failed_log 删除。
    - 将信息添加到 processed_log。
    返回 True 表示成功，False 表示项目未找到。
    """
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute("BEGIN TRANSACTION;")
            
            cursor.execute("SELECT item_name, item_type, score FROM failed_log WHERE item_id = ?", (item_id,))
            failed_item_info = cursor.fetchone()
            
            if not failed_item_info:
                conn.rollback()
                return False # 项目不存在

            cursor.execute("DELETE FROM failed_log WHERE item_id = ?", (item_id,))
            
            score_to_save = failed_item_info["score"] if failed_item_info["score"] is not None else 10.0
            item_name = failed_item_info["item_name"]
            
            cursor.execute(
                "REPLACE INTO processed_log (item_id, item_name, processed_at, score) VALUES (?, ?, CURRENT_TIMESTAMP, ?)",
                (item_id, item_name, score_to_save)
            )
            
            conn.commit()
            logger.info(f"DB: 项目 {item_id} ('{item_name}') 已成功移至已处理日志。")
            return True
    except Exception as e:
        logger.error(f"DB: 标记项目 {item_id} 为已处理时失败: {e}", exc_info=True)
        raise


def clear_all_review_items(db_path: str) -> int:
    """
    【V3 - 健壮版】清空所有待复核项目，并将它们全部标记为已处理。
    采用“先查询，后操作”的模式，保证“写入”与“删除”的原子性。
    返回被成功处理的项目数量。
    出现错误时抛出异常。
    """
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()

            # 1. 第一步：锁定目标
            # 将所有需要移动的项目完整地查询到内存中。
            # `items_to_move` 现在是我们唯一信任的“操作清单”。
            cursor.execute("SELECT item_id, item_name, score FROM failed_log")
            items_to_move = cursor.fetchall()
            
            initial_count = len(items_to_move)
            if initial_count == 0:
                logger.info("操作完成，待复核列表本就是空的。")
                return 0

            logger.info(f"查询到 {initial_count} 条待复核记录，准备开始移动...")

            # 2. 第二步：准备“写入”的数据
            # 根据我们的“操作清单”，创建准备写入 `processed_log` 的精确数据。
            data_for_processed_log = [
                (row['item_id'], row['item_name'], row['score'] or 10.0)
                for row in items_to_move
            ]

            # 3. 第三步：在“保险箱”（事务）中执行所有操作
            try:
                # 3.1 执行写入：将我们准备好的数据写入 `processed_log`
                # 这一步如果失败（比如表结构不对、约束冲突等），会立刻抛出异常，
                # 然后被下面的 `except` 捕获，直接进入回滚流程。
                copy_sql = """
                    REPLACE INTO processed_log (item_id, item_name, score, processed_at)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                """
                cursor.executemany(copy_sql, data_for_processed_log)
                
                # 3.2 执行删除：清空 `failed_log` 表
                # 只有在写入成功后，才会执行到这一步。
                cursor.execute("DELETE FROM failed_log")
                deleted_count = cursor.rowcount # 对于简单的 DELETE，rowcount 是可靠的

                # 4. 第四步：进行最终的、可靠的验证
                # 我们只验证一件事：删除的行数，是否等于我们一开始想要处理的行数？
                if deleted_count == initial_count:
                    # 如果相等，说明从我们查询到删除的这段时间里，没有其他程序干扰 `failed_log` 表。
                    # 同时，因为写入操作没有抛出异常，所以我们可以确信写入也成功了。
                    # 万无一失，提交事务！
                    conn.commit() 
                    logger.info(f"成功移动 {deleted_count} 条记录，事务已提交。")
                    return deleted_count
                else:
                    # 如果不相等，说明发生了并发问题，必须回滚以保证数据安全。
                    conn.rollback()
                    logger.error(f"数据不一致，事务回滚！初始查询到 {initial_count} 条，但最终删除了 {deleted_count} 条。")
                    raise Exception("数据不一致，操作回滚！")

            except Exception as e_inner:
                # 捕获事务中的任何失败，回滚并报告错误。
                logger.error(f"事务执行失败，正在回滚: {e_inner}", exc_info=True)
                conn.rollback()
                raise

    except Exception as e_outer:
        logger.error(f"清空并标记待复核列表时发生顶层异常：{e_outer}", exc_info=True)
        raise
# ======================================================================
# 模块 4: 智能追剧列表数据访问 (Watchlist Data Access)
# ======================================================================

def get_all_watchlist_items(db_path: str) -> List[Dict[str, Any]]:
    """获取所有追剧列表中的项目。"""
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM watchlist ORDER BY added_at DESC")
            items = [dict(row) for row in cursor.fetchall()]
            return items
    except Exception as e:
        logger.error(f"DB: 获取追剧列表失败: {e}", exc_info=True)
        raise


def get_watchlist_item_name(db_path: str, item_id: str) -> Optional[str]:
    """根据 item_id 获取单个追剧项目的名称。"""
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT item_name FROM watchlist WHERE item_id = ?", (item_id,))
            row = cursor.fetchone()
            return row['item_name'] if row else None
    except Exception as e:
        logger.warning(f"DB: 获取项目 {item_id} 名称时出错: {e}")
        return None


def add_item_to_watchlist(db_path: str, item_id: str, tmdb_id: str, item_name: str, item_type: str) -> bool:
    """
    添加一个新项目到追剧列表。
    如果项目已存在，则会替换它。
    返回 True 表示成功。
    """
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO watchlist (item_id, tmdb_id, item_name, item_type, status, last_checked_at)
                VALUES (?, ?, ?, ?, 'Watching', NULL)
            """, (item_id, tmdb_id, item_name, item_type))
            conn.commit()
            logger.info(f"DB: 项目 '{item_name}' (ID: {item_id}) 已成功添加/更新到追剧列表。")
            return True
    except Exception as e:
        logger.error(f"DB: 手动添加项目到追剧列表时发生错误: {e}", exc_info=True)
        raise


def update_watchlist_item_status(db_path: str, item_id: str, new_status: str) -> bool:
    """
    更新追剧列表中某个项目的状态。
    返回 True 表示成功，False 表示项目未找到。
    """
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE watchlist SET status = ? WHERE item_id = ?",
                (new_status, item_id)
            )
            conn.commit()
            if cursor.rowcount > 0:
                logger.info(f"DB: 项目 {item_id} 的追剧状态已更新为 '{new_status}'。")
                return True
            else:
                logger.warning(f"DB: 尝试更新追剧状态，但未在列表中找到项目 {item_id}。")
                return False
    except Exception as e:
        logger.error(f"DB: 更新追剧状态时发生错误: {e}", exc_info=True)
        raise


def remove_item_from_watchlist(db_path: str, item_id: str) -> bool:
    """
    从追剧列表中移除一个项目。
    返回 True 表示成功，False 表示项目未找到。
    """
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM watchlist WHERE item_id = ?", (item_id,))
            conn.commit()
            if cursor.rowcount > 0:
                logger.trace(f"DB: 项目 {item_id} 已从追剧列表移除。")
                return True
            else:
                logger.warning(f"DB: 尝试删除项目 {item_id}，但在追剧列表中未找到。")
                return False
    except sqlite3.OperationalError as e:
        if "database is locked" in str(e).lower():
            logger.error(f"DB: 从追剧列表移除项目时发生数据库锁定错误: {e}", exc_info=True)
        else:
            logger.error(f"DB: 从追剧列表移除项目时发生数据库操作错误: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"DB: 从追剧列表移除项目时发生未知错误: {e}", exc_info=True)
        raise

# 批量强制完结的逻辑
def batch_force_end_watchlist_items(db_path: str, item_ids: List[str]) -> int:
    """
    【V2】批量将追剧项目标记为“强制完结”。
    这会将项目状态设置为 'Ended'，并将 'force_ended' 标志位设为 True。
    这样可以防止常规刷新错误地复活剧集，但允许新一季的检查使其复活。
    返回成功更新的行数。
    """
    if not item_ids:
        return 0
    
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            placeholders = ','.join('?' for _ in item_ids)
            # 将状态更新为 Ended，并设置 force_ended 标志
            sql = f"UPDATE watchlist SET status = 'Completed', force_ended = 1 WHERE item_id IN ({placeholders})"
            
            cursor.execute(sql, item_ids)
            conn.commit()
            
            updated_count = cursor.rowcount
            if updated_count > 0:
                logger.info(f"DB: 批量强制完结了 {updated_count} 个追剧项目。")
            else:
                logger.warning(f"DB: 尝试批量强制完结，但提供的ID在列表中均未找到。")
            return updated_count
    except Exception as e:
        logger.error(f"DB: 批量强制完结追剧项目时发生错误: {e}", exc_info=True)
        raise
# ★★★ 批量更新追剧状态的数据库函数 ★★★
def batch_update_watchlist_status(db_path: str, item_ids: list, new_status: str) -> int:
    """
    批量更新指定项目ID列表的追剧状态。
    
    当状态更新为 'Watching' (例如“重新追剧”) 时，此函数会自动：
    1. 清除暂停日期 (`paused_until`)。
    2. 重置强制完结标志 (`force_ended`)。
    
    Args:
        db_path: 数据库文件路径。
        item_ids: 需要更新的项目ID列表。
        new_status: 要设置的新状态 ('Watching', 'Paused', 'Completed')。
        
    Returns:
        成功更新的行数。
    """
    if not item_ids:
        return 0
        
    try:
        with get_db_connection(db_path) as conn: # 假设你有一个 get_db_connection 函数
            cursor = conn.cursor()
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # 准备要更新的字段和值
            updates = {
                "status": new_status,
                "last_checked_at": current_time
            }
            
            # 核心逻辑：如果是“重新追剧”，则需要重置相关状态，让剧集恢复活力
            if new_status == 'Watching':
                updates["paused_until"] = None
                updates["force_ended"] = 0
            
            set_clauses = [f"{key} = ?" for key in updates.keys()]
            values = list(updates.values())
            
            # 使用参数化查询来防止SQL注入，这是处理列表的标准做法
            placeholders = ', '.join(['?'] * len(item_ids))
            sql = f"UPDATE watchlist SET {', '.join(set_clauses)} WHERE item_id IN ({placeholders})"
            
            # 将 item_ids 添加到值列表的末尾以匹配占位符
            values.extend(item_ids)
            
            cursor.execute(sql, tuple(values))
            conn.commit()
            
            logger.info(f"DB: 成功将 {cursor.rowcount} 个项目的状态批量更新为 '{new_status}'。")
            return cursor.rowcount
            
    except Exception as e:
        logger.error(f"批量更新项目状态时数据库出错: {e}", exc_info=True)
        # 重新抛出异常，让上层(API路由)可以捕获并返回500错误
        raise

# --- 水印 ---
def get_watching_tmdb_ids(db_path: str) -> set:
    """
    获取所有正在追看（状态为 'Watching'）的剧集的 TMDB ID 集合。
    返回一个集合(set)以便进行高效查询。
    """
    watching_ids = set()
    try:
        # 复用1: 使用标准的数据库连接获取方式
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            # 复用2: 借鉴现有的SQL查询逻辑
            cursor.execute("SELECT tmdb_id FROM watchlist WHERE status = 'Watching'")
            rows = cursor.fetchall()
            # 复用3: 借鉴数据处理方式，但适配新需求
            for row in rows:
                # 我们只需要 tmdb_id，并将其转换为字符串放入集合
                watching_ids.add(str(row['tmdb_id']))
    except Exception as e:
        # 使用你项目中已有的 logger
        logger.error(f"从数据库获取正在追看的TMDB ID时出错: {e}", exc_info=True)
        # 即使出错也返回空集合，保证上层逻辑的健壮性
    return watching_ids

# ======================================================================
# 模块 5: 电影合集数据访问 (Collections Data Access)
# ======================================================================

def get_all_collections(db_path: str) -> List[Dict[str, Any]]:
    """获取数据库中所有电影合集的信息。"""
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM collections_info WHERE tmdb_collection_id IS NOT NULL ORDER BY name")
            
            final_results = []
            for row in cursor.fetchall():
                row_dict = dict(row)
                # 在数据访问层直接处理 JSON 解析，让上层更省心
                try:
                    row_dict['missing_movies'] = json.loads(row_dict.get('missing_movies_json', '[]'))
                except (json.JSONDecodeError, TypeError):
                    row_dict['missing_movies'] = []
                del row_dict['missing_movies_json'] # 删除原始json字段
                final_results.append(row_dict)
                
            return final_results
    except Exception as e:
        logger.error(f"DB: 读取合集状态时发生严重错误: {e}", exc_info=True)
        raise

def get_all_custom_collection_emby_ids(db_path: str) -> set:
    """
    从 custom_collections 表中获取所有非空的 emby_collection_id。
    返回一个集合(set)以便进行高效的成员资格检查和集合运算。
    """
    ids = set()
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            # 只选择非NULL的ID
            cursor.execute("SELECT emby_collection_id FROM custom_collections WHERE emby_collection_id IS NOT NULL")
            rows = cursor.fetchall()
            for row in rows:
                ids.add(row['emby_collection_id'])
        logger.debug(f"从数据库中获取到 {len(ids)} 个由本程序管理的自定义合集ID。")
        return ids
    except sqlite3.Error as e:
        logger.error(f"获取所有自定义合集Emby ID时发生数据库错误: {e}", exc_info=True)
        return ids # 即使出错也返回一个空集合，保证上层逻辑不会崩溃

def get_collections_with_missing_movies(db_path: str) -> List[Dict[str, Any]]:
    """获取所有包含缺失电影的合集信息。"""
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT emby_collection_id, name, missing_movies_json FROM collections_info WHERE has_missing = 1")
            # 返回原始行，让业务逻辑层处理 JSON
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"DB: 获取有缺失电影的合集时失败: {e}", exc_info=True)
        raise


def update_collection_movies(db_path: str, collection_id: str, movies: List[Dict[str, Any]]):
    """
    更新指定合集的电影列表和缺失状态。
    """
    try:
        with get_db_connection(db_path) as conn:
            # 业务逻辑：根据更新后的电影列表，重新判断是否还有缺失
            still_has_missing = any(m.get('status') == 'missing' for m in movies)
            new_missing_json = json.dumps(movies, ensure_ascii=False)
            
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE collections_info SET missing_movies_json = ?, has_missing = ? WHERE emby_collection_id = ?",
                (new_missing_json, still_has_missing, collection_id)
            )
            conn.commit()
            logger.info(f"DB: 已更新合集 {collection_id} 的电影列表。")
    except Exception as e:
        logger.error(f"DB: 更新合集 {collection_id} 的电影列表时失败: {e}", exc_info=True)
        raise


def update_single_movie_status_in_collection(db_path: str, collection_id: str, movie_tmdb_id: str, new_status: str) -> bool:
    """
    更新合集中单个电影的状态。
    返回 True 表示成功，False 表示合集或电影未找到。
    """
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN TRANSACTION;")
            
            cursor.execute("SELECT missing_movies_json FROM collections_info WHERE emby_collection_id = ?", (collection_id,))
            row = cursor.fetchone()
            if not row:
                conn.rollback()
                return False # 合集未找到

            try:
                movies = json.loads(row['missing_movies_json'])
            except (json.JSONDecodeError, TypeError):
                movies = []

            movie_found = False
            for movie in movies:
                if str(movie.get('tmdb_id')) == str(movie_tmdb_id):
                    movie['status'] = new_status
                    movie_found = True
                    break
            
            if not movie_found:
                conn.rollback()
                return False # 电影未找到

            # 状态更新后，重新计算合集的 has_missing 标志
            still_has_missing = any(m.get('status') == 'missing' for m in movies)
            new_missing_json = json.dumps(movies, ensure_ascii=False)
            
            cursor.execute(
                "UPDATE collections_info SET missing_movies_json = ?, has_missing = ? WHERE emby_collection_id = ?", 
                (new_missing_json, still_has_missing, collection_id)
            )
            conn.commit()
            logger.info(f"DB: 已更新合集 {collection_id} 中电影 {movie_tmdb_id} 的状态为 '{new_status}'。")
            return True
    except Exception as e:
        logger.error(f"DB: 更新电影状态时发生数据库错误: {e}", exc_info=True)
        if conn and conn.in_transaction:
            conn.rollback()
        raise

# ★★★ 新增：批量将指定合集中的'missing'电影状态更新为'subscribed' ★★★
def batch_mark_movies_as_subscribed_in_collections(db_path: str, collection_ids: List[str]) -> int:
    """
    批量将指定合集列表中的所有'missing'状态的电影更新为'subscribed'。
    这是一个纯粹的数据库操作，不会触发任何外部订阅。

    :param db_path: 数据库路径。
    :param collection_ids: 需要操作的合集 Emby ID 列表。
    :return: 成功更新状态的电影总数。
    """
    if not collection_ids:
        return 0

    total_updated_movies = 0
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            
            # 使用参数化查询获取所有相关的合集
            placeholders = ','.join('?' for _ in collection_ids)
            sql_select = f"SELECT emby_collection_id, missing_movies_json FROM collections_info WHERE emby_collection_id IN ({placeholders})"
            cursor.execute(sql_select, collection_ids)
            collections_to_process = cursor.fetchall()

            if not collections_to_process:
                return 0

            cursor.execute("BEGIN TRANSACTION;")
            try:
                for collection_row in collections_to_process:
                    collection_id = collection_row['emby_collection_id']
                    
                    try:
                        movies = json.loads(collection_row['missing_movies_json'] or '[]')
                    except (json.JSONDecodeError, TypeError):
                        continue

                    movies_changed_in_this_collection = False
                    for movie in movies:
                        if movie.get('status') == 'missing':
                            movie['status'] = 'subscribed'
                            total_updated_movies += 1
                            movies_changed_in_this_collection = True
                    
                    # 只有当这个合集确实有状态被改变时，才回写数据库
                    if movies_changed_in_this_collection:
                        # 既然所有 missing 都被改了，has_missing 标志肯定为 False
                        new_missing_json = json.dumps(movies, ensure_ascii=False)
                        cursor.execute(
                            "UPDATE collections_info SET missing_movies_json = ?, has_missing = 0 WHERE emby_collection_id = ?",
                            (new_missing_json, collection_id)
                        )
                
                conn.commit()
                logger.info(f"DB: 成功将 {len(collection_ids)} 个合集中的 {total_updated_movies} 部缺失电影标记为已订阅。")

            except Exception as e_trans:
                conn.rollback()
                logger.error(f"批量标记已订阅的数据库事务失败，已回滚: {e_trans}", exc_info=True)
                raise
        
        return total_updated_movies

    except Exception as e:
        logger.error(f"DB: 批量标记电影为已订阅时发生错误: {e}", exc_info=True)
        raise
# ======================================================================
# 模块 6: 演员订阅数据访问 (Actor Subscriptions Data Access)
# ======================================================================

def get_all_actor_subscriptions(db_path: str) -> List[Dict[str, Any]]:
    """获取所有演员订阅的简略列表。"""
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            # 只选择列表页需要展示的核心字段
            cursor.execute("SELECT id, tmdb_person_id, actor_name, profile_path, status, last_checked_at FROM actor_subscriptions ORDER BY added_at DESC")
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"DB: 获取演员订阅列表失败: {e}", exc_info=True)
        raise


def get_single_subscription_details(db_path: str, subscription_id: int) -> Optional[Dict[str, Any]]:
    """获取单个订阅的完整详情，包括其追踪的所有媒体。"""
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            
            # 1. 获取订阅主信息
            cursor.execute("SELECT * FROM actor_subscriptions WHERE id = ?", (subscription_id,))
            subscription = cursor.fetchone()
            if not subscription:
                return None
            
            # 2. 获取关联的已追踪媒体
            cursor.execute("SELECT * FROM tracked_actor_media WHERE subscription_id = ? ORDER BY release_date DESC", (subscription_id,))
            tracked_media = [dict(row) for row in cursor.fetchall()]
            
            # 3. 组合数据
            response_data = dict(subscription)
            response_data['tracked_media'] = tracked_media
            return response_data
    except Exception as e:
        logger.error(f"DB: 获取订阅详情 {subscription_id} 失败: {e}", exc_info=True)
        raise


def safe_json_dumps(value):
    """
    将 Python 对象转换成 JSON 字符串。
    如果传入的是字符串且能被解析成合法 JSON，则先解析再序列化，避免重复转义。
    否则按字符串处理。
    """
    if isinstance(value, str):
        try:
            # 尝试解析字符串（可能是JSON字符串）
            parsed = json.loads(value)
            # 重新序列化，保证仅一层转义
            return json.dumps(parsed, ensure_ascii=False)
        except Exception:
            # 解析失败，按字符串序列化
            return json.dumps(value, ensure_ascii=False)
    else:
        # 普通Python对象，正常序列化
        return json.dumps(value, ensure_ascii=False)

def add_actor_subscription(db_path: str, tmdb_person_id: int, actor_name: str, profile_path: str, config: dict) -> int:
    """
    新增一个演员订阅。
    正确处理配置中的 JSON 字段，避免多层转义。
    """
    start_year = config.get('start_year', 1900)
    media_types = config.get('media_types', 'Movie,TV')
    genres_include = safe_json_dumps(config.get('genres_include_json', []))
    genres_exclude = safe_json_dumps(config.get('genres_exclude_json', []))
    min_rating = config.get('min_rating', 6.0)

    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO actor_subscriptions 
                (tmdb_person_id, actor_name, profile_path, config_start_year, config_media_types, config_genres_include_json, config_genres_exclude_json, config_min_rating)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (tmdb_person_id, actor_name, profile_path, start_year, media_types, genres_include, genres_exclude, min_rating)
            )
            conn.commit()
            new_sub_id = cursor.lastrowid
            logger.info(f"DB: 成功添加演员订阅 '{actor_name}' (ID: {new_sub_id})。")
            return new_sub_id
    except sqlite3.IntegrityError:
        raise
    except Exception as e:
        logger.error(f"DB: 添加演员订阅 '{actor_name}' 时失败: {e}", exc_info=True)
        raise

def update_actor_subscription(db_path: str, subscription_id: int, data: dict) -> bool:
    """
    更新一个演员订阅的状态或配置。
    处理 JSON 字段时避免多层转义。
    """
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM actor_subscriptions WHERE id = ?", (subscription_id,))
            current_sub = cursor.fetchone()
            if not current_sub:
                return False

            new_status = data.get('status', current_sub['status'])
            config = data.get('config')

            if config is not None:
                new_start_year = config.get('start_year', current_sub['config_start_year'])
                new_media_types = config.get('media_types', current_sub['config_media_types'])

                # 先拿配置传入值，没有则尝试从数据库旧值解析Python对象
                genres_include_raw = config.get('genres_include_json', current_sub['config_genres_include_json'])
                genres_exclude_raw = config.get('genres_exclude_json', current_sub['config_genres_exclude_json'])

                new_genres_include = safe_json_dumps(genres_include_raw)
                new_genres_exclude = safe_json_dumps(genres_exclude_raw)

                new_min_rating = config.get('min_rating', current_sub['config_min_rating'])
            else:
                new_start_year = current_sub['config_start_year']
                new_media_types = current_sub['config_media_types']
                new_genres_include = current_sub['config_genres_include_json']
                new_genres_exclude = current_sub['config_genres_exclude_json']
                new_min_rating = current_sub['config_min_rating']

            cursor.execute("""
                UPDATE actor_subscriptions SET
                status = ?, config_start_year = ?, config_media_types = ?, 
                config_genres_include_json = ?, config_genres_exclude_json = ?, config_min_rating = ?
                WHERE id = ?
            """, (new_status, new_start_year, new_media_types, new_genres_include, new_genres_exclude, new_min_rating, subscription_id))
            conn.commit()
            logger.info(f"DB: 成功更新订阅ID {subscription_id}。")
            return True
    except Exception as e:
        logger.error(f"DB: 更新订阅 {subscription_id} 失败: {e}", exc_info=True)
        raise


def delete_actor_subscription(db_path: str, subscription_id: int) -> bool:
    """
    删除一个演员订阅及其所有追踪的媒体。
    返回 True 表示成功。
    """
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            # 由于外键设置了 ON DELETE CASCADE，我们只需要删除主表记录即可
            cursor.execute("DELETE FROM actor_subscriptions WHERE id = ?", (subscription_id,))
            conn.commit()
            logger.info(f"DB: 成功删除订阅ID {subscription_id}。")
            return True
    except Exception as e:
        logger.error(f"DB: 删除订阅 {subscription_id} 失败: {e}", exc_info=True)
        raise

# ======================================================================
# 模块 6: 自定义电影合集数据访问 (custom_collections Data Access)
# ======================================================================

def create_custom_collection(db_path: str, name: str, type: str, definition_json: str) -> int:
    """
    在数据库中创建一个新的自定义合集定义。
    :return: 新创建的合集的ID。
    :raises sqlite3.IntegrityError: 如果合集名称已存在。
    :raises sqlite3.Error: 如果发生其他数据库错误。
    """
    sql = """
        INSERT INTO custom_collections (name, type, definition_json, status, created_at)
        VALUES (?, ?, ?, 'active', CURRENT_TIMESTAMP)
    """
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (name, type, definition_json))
            conn.commit()
            logger.info(f"成功创建自定义合集 '{name}' (类型: {type})。")
            new_id = cursor.lastrowid
            if new_id is None:
                # 这是一个不太可能发生但需要防御的情况
                raise sqlite3.Error("数据库未能返回新创建行的ID。")
            return new_id
    except sqlite3.IntegrityError:
        # ★★★ 捕获到唯一性冲突时，不再记录为错误，而是直接将异常向上抛出 ★★★
        raise
    except sqlite3.Error as e:
        # ★★★ 捕获到其他数据库错误时，记录日志并同样向上抛出 ★★★
        logger.error(f"创建自定义合集 '{name}' 时发生非预期的数据库错误: {e}", exc_info=True)
        raise

def get_all_custom_collections(db_path: str) -> List[Dict[str, Any]]:
    try:
        with get_db_connection(db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM custom_collections
                ORDER BY sort_order ASC, id ASC
            """)
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
    except sqlite3.Error as e:
        logger.error(f"获取所有自定义合集时发生数据库错误: {e}", exc_info=True)
        return []

# ★★★ 获取所有已启用的自定义合集，供“一键生成”任务使用 ★★★
def get_all_active_custom_collections(db_path: str) -> List[Dict[str, Any]]:
    """获取所有状态为 'active' 的自定义合集"""
    try:
        with get_db_connection(db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM custom_collections WHERE status = 'active' ORDER BY sort_order ASC, id ASC")
            rows = cursor.fetchall()
            logger.trace(f"  -> 从数据库找到 {len(rows)} 个已启用的自定义合集。")
            return [dict(row) for row in rows]
    except sqlite3.Error as e:
        logger.error(f"获取所有已启用的自定义合集时发生数据库错误: {e}", exc_info=True)
        return []

def get_custom_collection_by_id(db_path: str, collection_id: int) -> Optional[Dict[str, Any]]:
    """
    根据ID获取单个自定义合集的详细信息。
    :param collection_id: 自定义合集的ID。
    :return: 包含合集信息的字典，如果未找到则返回None。
    """
    try:
        with get_db_connection(db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM custom_collections WHERE id = ?", (collection_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    except sqlite3.Error as e:
        logger.error(f"根据ID {collection_id} 获取自定义合集时发生数据库错误: {e}", exc_info=True)
        return None

def update_custom_collection(db_path: str, collection_id: int, name: str, type: str, definition_json: str, status: str) -> bool:
    """
    更新一个已存在的自定义合集。
    """
    sql = """
        UPDATE custom_collections
        SET name = ?, type = ?, definition_json = ?, status = ?
        WHERE id = ?
    """
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (name, type, definition_json, status, collection_id))
            conn.commit()
            logger.info(f"成功更新自定义合集 ID: {collection_id}。")
            return True
    except sqlite3.Error as e:
        logger.error(f"更新自定义合集 ID {collection_id} 时发生数据库错误: {e}", exc_info=True)
        return False

def delete_custom_collection(db_path: str, collection_id: int) -> bool:
    """
    【V5 - 职责单一版】从数据库中删除一个自定义合集定义。
    此函数只负责数据库删除操作，不再与任何其他表或外部服务交互。
    联动删除Emby实体的逻辑应由调用方（API层）处理。
    
    :param db_path: 数据库路径。
    :param collection_id: 要删除的自定义合集的数据库ID。
    :return: 如果成功删除了记录，返回 True；如果未找到记录或发生错误，返回 False。
    """
    sql = "DELETE FROM custom_collections WHERE id = ?"
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (collection_id,))
            conn.commit()
            # cursor.rowcount > 0 确保确实有一行被删除了
            if cursor.rowcount > 0:
                logger.info(f"✅ 成功从数据库中删除了自定义合集定义 (ID: {collection_id})。")
                return True
            else:
                logger.warning(f"尝试删除自定义合集 (ID: {collection_id})，但在数据库中未找到该记录。")
                return False # 虽然不是错误，但操作未产生效果
    except sqlite3.Error as e:
        logger.error(f"删除自定义合集 (ID: {collection_id}) 时发生数据库错误: {e}", exc_info=True)
        raise # 向上抛出异常，让API层可以捕获并返回500错误

# ★★★ 更新自定义合集排序的函数 ★★★
def update_custom_collections_order(db_path: str, ordered_ids: List[int]) -> bool:
    """
    根据提供的ID列表，批量更新自定义合集的 sort_order。
    :param db_path: 数据库路径。
    :param ordered_ids: 按新顺序排列的合集ID列表。
    :return: 操作是否成功。
    """
    if not ordered_ids:
        return True

    sql = "UPDATE custom_collections SET sort_order = ? WHERE id = ?"
    # 创建一个元组列表，每个元组是 (sort_order, id)
    data_to_update = [(index, collection_id) for index, collection_id in enumerate(ordered_ids)]

    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN TRANSACTION;")
            cursor.executemany(sql, data_to_update)
            conn.commit()
            logger.info(f"成功更新了 {len(ordered_ids)} 个自定义合集的顺序。")
            return True
    except sqlite3.Error as e:
        logger.error(f"批量更新自定义合集顺序时发生数据库错误: {e}", exc_info=True)
        # 发生错误时，事务会自动回滚
        return False

# +++ 自定义合集筛选引擎所需函数 +++
def get_media_metadata_by_tmdb_id(db_path: str, tmdb_id: str) -> Optional[Dict[str, Any]]:
    """
    根据TMDb ID从媒体元数据缓存表中获取单条记录。
    """
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM media_metadata WHERE tmdb_id = ?", (tmdb_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    except sqlite3.Error as e:
        logger.error(f"根据TMDb ID {tmdb_id} 获取媒体元数据时出错: {e}", exc_info=True)
        return None
    
# ★★★ 获取所有媒体元数据 ★★★
def get_all_media_metadata(db_path: str, item_type: str = 'Movie') -> List[Dict[str, Any]]:
    """
    从媒体元数据缓存表中获取指定类型的所有记录。
    :param item_type: 'Movie' 或 'Series'。默认为 'Movie'，因为合集主要是电影。
    """
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM media_metadata WHERE item_type = ?", (item_type,))
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
    except sqlite3.Error as e:
        logger.error(f"获取所有媒体元数据时出错 (类型: {item_type}): {e}", exc_info=True)
        return []
    
# ★★★ 从元数据表中提取所有唯一的类型 ★★★
def get_unique_genres(db_path: str) -> List[str]:
    """
    从 media_metadata 表中扫描所有电影，提取出所有不重复的类型(genres)。
    """
    unique_genres = set()
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            # 我们只需要 genres_json 这一列
            cursor.execute("SELECT genres_json FROM media_metadata WHERE item_type = 'Movie'")
            rows = cursor.fetchall()
            
            for row in rows:
                if row['genres_json']:
                    try:
                        # 解析JSON数组
                        genres = json.loads(row['genres_json'])
                        # 将列表中的每个类型都加入到集合中，集合会自动处理重复
                        for genre in genres:
                            if genre: # 确保不是空字符串
                                unique_genres.add(genre.strip())
                    except (json.JSONDecodeError, TypeError):
                        continue # 如果某行数据有问题，跳过
                        
        # 将集合转换为列表并排序
        sorted_genres = sorted(list(unique_genres))
        logger.trace(f"从数据库中成功提取出 {len(sorted_genres)} 个唯一的电影类型。")
        return sorted_genres
        
    except sqlite3.Error as e:
        logger.error(f"提取唯一电影类型时发生数据库错误: {e}", exc_info=True)
        return []

# ★★★ 从元数据表中提取所有唯一的工作室 ★★★
def get_unique_studios(db_path: str) -> List[str]:
    """
    【V2 - 视野扩展版】
    从 media_metadata 表中扫描所有媒体项（电影和电视剧），
    提取出所有不重复的工作室(studios)。
    """
    unique_studios = set()
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            
            # 【【【手术点：移除 WHERE item_type = 'Movie' 子句】】】
            # 让查询覆盖整个表，不再局限于电影
            cursor.execute("SELECT studios_json FROM media_metadata")
            rows = cursor.fetchall()
            
            for row in rows:
                if row['studios_json']:
                    try:
                        studios = json.loads(row['studios_json'])
                        for studio in studios:
                            if studio:
                                unique_studios.add(studio.strip())
                    except (json.JSONDecodeError, TypeError):
                        continue
                        
        sorted_studios = sorted(list(unique_studios))
        logger.trace(f"从数据库中成功提取出 {len(sorted_studios)} 个跨电影和电视剧的唯一工作室。")
        return sorted_studios
        
    except sqlite3.Error as e:
        logger.error(f"提取唯一工作室时发生数据库错误: {e}", exc_info=True)
        return []
    
# ★★★ 根据关键词搜索唯一的工作室 ★★★
def search_unique_studios(db_path: str, search_term: str, limit: int = 20) -> List[str]:
    """
    (V3 - 智能排序版)
    从数据库中搜索工作室，并优先返回名称以 search_term 开头的结果。
    """
    if not search_term:
        return []
    
    all_studios = get_unique_studios(db_path)
    
    if not all_studios:
        return []

    search_term_lower = search_term.lower()
    
    # ★★★ 核心升级：创建两个列表来存放不同优先级的匹配结果 ★★★
    starts_with_matches = []
    contains_matches = []
    
    for studio in all_studios:
        studio_lower = studio.lower()
        # 1. 优先检查是否以搜索词开头
        if studio_lower.startswith(search_term_lower):
            starts_with_matches.append(studio)
        # 2. 如果不是开头匹配，再检查是否包含
        elif search_term_lower in studio_lower:
            contains_matches.append(studio)
            
    # ★★★ 核心升级：将两个列表合并，高优先级的在前 ★★★
    final_matches = starts_with_matches + contains_matches
    
    logger.trace(f"智能搜索 '{search_term}'，找到 {len(final_matches)} 个匹配项。")
    
    # 只返回限定数量的结果
    return final_matches[:limit]

# --- 搜索演员 ---
def search_unique_actors(db_path: str, search_term: str, limit: int = 20) -> List[str]:
    """
    (V6 - 中英双语兼容搜索版)
    直接从 media_metadata 表中提取演员的 name 和 original_name 进行搜索。
    用户可以用中文译名或原始外文名进行搜索。
    """
    if not search_term:
        return []
    
    # ★★★ 核心修改 1: 使用字典来存储 unique_name -> original_name 的映射 ★★★
    unique_actors_map = {}
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT actors_json FROM media_metadata")
            rows = cursor.fetchall()
            
            for row in rows:
                if row['actors_json']:
                    try:
                        actors = json.loads(row['actors_json'])
                        for actor in actors:
                            actor_name = actor.get('name')
                            original_name = actor.get('original_name')
                            
                            if actor_name and actor_name.strip():
                                # 使用 actor_name 作为键确保唯一性
                                if actor_name not in unique_actors_map:
                                    unique_actors_map[actor_name.strip()] = (original_name or '').strip()

                    except (json.JSONDecodeError, TypeError):
                        continue
        
        if not unique_actors_map:
            return []

        # 步骤 2: 在提取出的名字集合中进行双语搜索
        search_term_lower = search_term.lower()
        starts_with_matches = []
        contains_matches = []
        
        # ★★★ 核心修改 2: 遍历字典，同时检查 name 和 original_name ★★★
        for name, original_name in sorted(unique_actors_map.items()):
            name_lower = name.lower()
            original_name_lower = original_name.lower()

            # 智能排序：优先匹配开头
            if name_lower.startswith(search_term_lower) or (original_name_lower and original_name_lower.startswith(search_term_lower)):
                starts_with_matches.append(name) # 无论哪个匹配，都返回最终的 name
            # 其次匹配包含
            elif search_term_lower in name_lower or (original_name_lower and search_term_lower in original_name_lower):
                contains_matches.append(name)
        
        final_matches = starts_with_matches + contains_matches
        logger.trace(f"双语搜索演员 '{search_term}'，找到 {len(final_matches)} 个匹配项。")
        
        return final_matches[:limit]
        
    except sqlite3.Error as e:
        logger.error(f"提取并搜索唯一演员时发生数据库错误: {e}", exc_info=True)
        return []

# ★★★ 新增：写入或更新一条完整的合集检查信息 ★★★
def upsert_collection_info(db_path: str, collection_data: Dict[str, Any]):
    """
    使用 INSERT OR REPLACE 写入或更新一条合集信息到 collections_info 表。
    """
    sql = """
        INSERT OR REPLACE INTO collections_info 
        (emby_collection_id, name, tmdb_collection_id, item_type, status, has_missing, 
        missing_movies_json, last_checked_at, poster_path, in_library_count)
        VALUES (:emby_collection_id, :name, :tmdb_collection_id, :item_type, :status, :has_missing, 
        :missing_movies_json, :last_checked_at, :poster_path, :in_library_count)
    """
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(sql, collection_data)
            conn.commit()
            logger.info(f"成功写入/更新合集检查信息到数据库 (ID: {collection_data.get('emby_collection_id')})。")
    except sqlite3.Error as e:
        logger.error(f"写入合集检查信息时发生数据库错误: {e}", exc_info=True)
        raise

def update_custom_collection_after_sync(db_path: str, collection_id: int, update_data: Dict[str, Any]) -> bool:
    """
    在同步任务完成后，使用一个包含多个字段的字典来更新自定义合集的状态。
    这是一个灵活的函数，可以动态构建SQL语句。
    """
    if not update_data:
        logger.warning(f"尝试更新自定义合集 {collection_id}，但没有提供任何更新数据。")
        return False

    # 动态构建 SET 子句
    set_clauses = [f"{key} = ?" for key in update_data.keys()]
    values = list(update_data.values())
    
    sql = f"UPDATE custom_collections SET {', '.join(set_clauses)} WHERE id = ?"
    values.append(collection_id)

    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(sql, tuple(values))
            conn.commit()
            logger.trace(f"已更新自定义合集 {collection_id} 的同步后状态。")
            return True
    except sqlite3.Error as e:
        logger.error(f"更新自定义合集 {collection_id} 同步后状态时出错: {e}", exc_info=True)
        return False

def update_single_media_status_in_custom_collection(db_path: str, collection_id: int, media_tmdb_id: str, new_status: str) -> bool:
    """
    更新自定义合集中单个媒体项的状态，并重新计算合集的健康度。
    """
    try:
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN TRANSACTION;")
            
            cursor.execute("SELECT generated_media_info_json FROM custom_collections WHERE id = ?", (collection_id,))
            row = cursor.fetchone()
            if not row:
                conn.rollback()
                return False

            try:
                media_items = json.loads(row['generated_media_info_json'] or '[]')
            except (json.JSONDecodeError, TypeError):
                media_items = []

            item_found = False
            for item in media_items:
                if str(item.get('tmdb_id')) == str(media_tmdb_id):
                    item['status'] = new_status
                    item_found = True
                    break
            
            if not item_found:
                conn.rollback()
                return False

            # 重新计算健康状态
            missing_count = sum(1 for item in media_items if item.get('status') == 'missing')
            new_health_status = 'has_missing' if missing_count > 0 else 'ok'
            
            # 准备更新的数据
            update_data = {
                "generated_media_info_json": json.dumps(media_items, ensure_ascii=False),
                "missing_count": missing_count,
                "health_status": new_health_status
            }
            
            set_clauses = [f"{key} = ?" for key in update_data.keys()]
            values = list(update_data.values())
            sql = f"UPDATE custom_collections SET {', '.join(set_clauses)} WHERE id = ?"
            values.append(collection_id)
            
            cursor.execute(sql, tuple(values))
            conn.commit()
            logger.trace(f"已更新自定义合集 {collection_id} 中媒体 {media_tmdb_id} 的状态为 '{new_status}'。")
            return True
    except Exception as e:
        logger.error(f"DB: 更新自定义合集中媒体状态时发生数据库错误: {e}", exc_info=True)
        if conn and conn.in_transaction:
            conn.rollback()
        raise

# --- 更新榜单合集 ---
def match_and_update_list_collections_on_item_add(db_path: str, new_item_tmdb_id: str, new_item_name: str) -> List[Dict[str, Any]]:
    """
    【V2 - 自动化闭环核心】
    当新媒体入库时，查找所有匹配的'list'类型合集，更新其内部状态，并返回需要被操作的Emby合集信息。
    - 查找所有包含该 new_item_tmdb_id 且状态不为 'in_library' 的活动榜单合集。
    - 在内存中将这些媒体项的状态更新为 'in_library'。
    - 重新计算每个受影响合集的健康度统计（入库数、缺失数）。
    - 将所有更改一次性写入数据库事务。
    
    :param db_path: 数据库路径。
    :param new_item_tmdb_id: 新入库媒体的 TMDb ID。
    :param new_item_name: 新入库媒体的名称（用于日志记录）。
    :return: 一个字典列表，包含所有被成功更新的合集的'emby_collection_id'和'name'，供上层调用Emby API。
    """
    collections_to_update_in_emby = []
    
    try:
        with get_db_connection(db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # 1. 查找所有可能相关的合集
            sql_find = """
                SELECT * FROM custom_collections 
                WHERE type = 'list' AND status = 'active' AND emby_collection_id IS NOT NULL
                AND generated_media_info_json LIKE ?
            """
            cursor.execute(sql_find, (f'%"tmdb_id": "{new_item_tmdb_id}"%',))
            candidate_collections = cursor.fetchall()

            if not candidate_collections:
                logger.debug(f"  -> 未在任何榜单合集中找到 TMDb ID: {new_item_tmdb_id}。")
                return []

            # 2. 在事务中处理所有匹配的合集
            cursor.execute("BEGIN TRANSACTION;")
            try:
                for collection_row in candidate_collections:
                    collection = dict(collection_row)
                    collection_id = collection['id']
                    collection_name = collection['name']
                    
                    try:
                        media_list = json.loads(collection.get('generated_media_info_json') or '[]')
                        item_found_and_updated = False
                        
                        # 在合集的媒体列表中查找新入库的项目
                        for media_item in media_list:
                            if str(media_item.get('tmdb_id')) == str(new_item_tmdb_id) and media_item.get('status') != 'in_library':
                                
                                # --- ✨✨✨ 核心修改：使用翻译字典生成日志 ✨✨✨ ---
                                old_status_key = media_item.get('status', 'unknown')
                                new_status_key = 'in_library'
                                
                                old_status_cn = STATUS_TRANSLATION_MAP.get(old_status_key, old_status_key)
                                new_status_cn = STATUS_TRANSLATION_MAP.get(new_status_key, new_status_key)

                                logger.info(f"  -> 数据库状态更新：项目《{new_item_name}》在合集《{collection_name}》中的状态将从【{old_status_cn}】更新为【{new_status_cn}】。")
                                
                                media_item['status'] = new_status_key # 数据库中仍然存储英文key
                                item_found_and_updated = True
                                break
                        
                        # 如果状态发生了变化，则回写数据库
                        if item_found_and_updated:
                            # 重新计算统计数据
                            new_in_library_count = sum(1 for m in media_list if m.get('status') == 'in_library')
                            new_missing_count = sum(1 for m in media_list if m.get('status') == 'missing')
                            new_health_status = 'has_missing' if new_missing_count > 0 else 'ok'
                            new_json_data = json.dumps(media_list, ensure_ascii=False)
                            
                            # 执行数据库更新
                            cursor.execute("""
                                UPDATE custom_collections
                                SET generated_media_info_json = ?,
                                    in_library_count = ?,
                                    missing_count = ?,
                                    health_status = ?
                                WHERE id = ?
                            """, (new_json_data, new_in_library_count, new_missing_count, new_health_status, collection_id))
                            
                            # 记录需要通知 Emby 的合集信息
                            collections_to_update_in_emby.append({
                                'emby_collection_id': collection['emby_collection_id'],
                                'name': collection_name
                            })

                    except (json.JSONDecodeError, TypeError) as e_json:
                        logger.warning(f"解析或处理榜单合集《{collection_name}》的数据时出错: {e_json}，跳过。")
                        continue
                
                conn.commit() # 提交事务
                
            except Exception as e_trans:
                conn.rollback() # 事务中发生任何错误，回滚
                logger.error(f"在更新榜单合集数据库状态的事务中发生错误: {e_trans}", exc_info=True)
                raise # 向上抛出异常

        return collections_to_update_in_emby

    except sqlite3.Error as e_db:
        logger.error(f"匹配和更新榜单合集时发生数据库错误: {e_db}", exc_info=True)
        raise