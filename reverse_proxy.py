# reverse_proxy.py (最终完美版 V4 - 诊断增强版 - 无水印版)

import logging
import requests
import re
import json
from flask import Flask, request, Response
from urllib.parse import urlparse, urlunparse
from concurrent.futures import ThreadPoolExecutor
import time
import threading
import uuid # <-- 确保导入
from gevent import spawn
from geventwebsocket.websocket import WebSocket
from websocket import create_connection

import config_manager
import db_handler
import extensions
import emby_handler
logger = logging.getLogger(__name__)

# --- 【核心修改】---
# 不再使用字符串前缀，而是定义一个数字转换基数
# 这将把数据库ID (例如 7) 转换为一个唯一的、负数的、看起来像原生ID的数字 (例如 -900007)
MIMICKED_ID_BASE = 900000

def to_mimicked_id(db_id):
    """将数据库内部ID转换为代理使用的外部ID"""
    return str(-(MIMICKED_ID_BASE + db_id))

def from_mimicked_id(mimicked_id):
    """将代理的外部ID转换回数据库内部ID"""
    return -(int(mimicked_id)) - MIMICKED_ID_BASE

def is_mimicked_id(item_id):
    """检查一个ID是否是我们的虚拟ID"""
    try:
        # 我们的ID是负数
        return isinstance(item_id, str) and item_id.startswith('-')
    except:
        return False

# --- 【核心修改】---
# 更新正则表达式以匹配新的数字ID格式（负数）
MIMICKED_ITEMS_RE = re.compile(r'/emby/Users/([^/]+)/Items/(-(\d+))')
MIMICKED_ITEM_DETAILS_RE = re.compile(r'emby/Users/([^/]+)/Items/(-(\d+))$')


def _get_real_emby_url_and_key():
    base_url = config_manager.APP_CONFIG.get("emby_server_url", "").rstrip('/')
    api_key = config_manager.APP_CONFIG.get("emby_api_key", "")
    if not base_url or not api_key:
        raise ValueError("Emby服务器地址或API Key未配置")
    return base_url, api_key

def handle_get_views():
    real_server_id = extensions.EMBY_SERVER_ID
    if not real_server_id:
        return "Proxy is not ready", 503

    try:
        collections = db_handler.get_all_active_custom_collections(config_manager.DB_PATH)
        fake_views_items = []
        for coll in collections:
            db_id = coll['id']
            mimicked_id = to_mimicked_id(db_id) # <-- 【核心修改】使用新的ID生成函数

            real_emby_collection_id = coll.get('emby_collection_id')
            image_tags = {}
            if real_emby_collection_id:
                image_tags["Primary"] = f"{real_emby_collection_id}?timestamp={int(time.time())}"

            definition = json.loads(coll.get('definition_json', '{}'))
            merged_libraries = definition.get('merged_libraries', [])
            name_suffix = f" (合并库: {len(merged_libraries)}个)" if merged_libraries else ""
            
            item_type_from_db = definition.get('item_type', 'Movie')
            if isinstance(item_type_from_db, list) and len(item_type_from_db) > 1:
                collection_type = "mixed"
            else:
                authoritative_type = item_type_from_db[0] if isinstance(item_type_from_db, list) and item_type_from_db else item_type_from_db if isinstance(item_type_from_db, str) else 'Movie'
                collection_type = "tvshows" if authoritative_type == 'Series' else "movies"

            fake_view = {
                "Name": coll['name'] + name_suffix, "ServerId": real_server_id, "Id": mimicked_id, # <-- 【核心修改】使用数字ID
                "Guid": str(uuid.uuid4()), "Etag": f"{db_id}{int(time.time())}",
                "DateCreated": "2025-01-01T00:00:00.0000000Z", "CanDelete": False, "CanDownload": False,
                "SortName": coll['name'], "ExternalUrls": [], "ProviderIds": {}, "IsFolder": True,
                "ParentId": "2", "Type": "Collection",
                "UserData": {"PlaybackPositionTicks": 0, "IsFavorite": False, "Played": False},
                "ChildCount": 1, "DisplayPreferencesId": f"custom-{db_id}",
                "PrimaryImageAspectRatio": 1.7777777777777777, "CollectionType": collection_type,
                "ImageTags": image_tags, "BackdropImageTags": [], "LockedFields": [], "LockData": False
            }
            fake_views_items.append(fake_view)
        logger.debug(f"已生成 {len(fake_views_items)} 个使用数字ID的自定义合集视图。")

        # --- 原生库合并逻辑 (保持不变) ---
        native_views_items = []
        should_merge_native = config_manager.APP_CONFIG.get('proxy_merge_native_libraries', True)
        if should_merge_native:
            user_id_match = re.search(r'/emby/Users/([^/]+)/Views', request.path)
            if user_id_match:
                user_id = user_id_match.group(1)
                
                # ★★★ 核心修改：调用统一的、强大的函数 ★★★
                all_native_views = emby_handler.get_emby_libraries(
                    config_manager.APP_CONFIG.get("emby_server_url", ""),
                    config_manager.APP_CONFIG.get("emby_api_key", ""),
                    user_id
                )
                # 如果获取失败，给一个空列表以防止崩溃
                if all_native_views is None:
                    all_native_views = []

                raw_selection = config_manager.APP_CONFIG.get('proxy_native_view_selection', '')
                selected_native_view_ids = [x.strip() for x in raw_selection.split(',') if x.strip()] if isinstance(raw_selection, str) else raw_selection
                
                if not selected_native_view_ids:
                    native_views_items = all_native_views
                else:
                    # 这里的过滤逻辑可以完美地工作，因为我们现在拥有完整的项目列表
                    native_views_items = [view for view in all_native_views if view.get("Id") in selected_native_view_ids]
        
        final_items = []
        native_order = config_manager.APP_CONFIG.get('proxy_native_view_order', 'before')
        if native_order == 'after':
            final_items.extend(fake_views_items)
            final_items.extend(native_views_items)
        else:
            final_items.extend(native_views_items)
            final_items.extend(fake_views_items)

        final_response = {"Items": final_items, "TotalRecordCount": len(final_items)}
        return Response(json.dumps(final_response), mimetype='application/json')
        
    except Exception as e:
        logger.error(f"[PROXY] 获取视图数据时出错: {e}", exc_info=True)
        return "Internal Proxy Error", 500

def handle_get_mimicked_library_details(user_id, mimicked_id):
    ### UI回退点 ###
    # 不再尝试“偷天换日”，而是创建一个简单、稳定、有效的详情对象。
    try:
        real_db_id = from_mimicked_id(mimicked_id)
        coll = db_handler.get_custom_collection_by_id(config_manager.DB_PATH, real_db_id)
        if not coll: return "Not Found", 404

        real_server_id = extensions.EMBY_SERVER_ID
        real_emby_collection_id = coll.get('emby_collection_id')
        image_tags = {"Primary": real_emby_collection_id} if real_emby_collection_id else {}
        
        definition = json.loads(coll.get('definition_json', '{}'))
        item_type_from_db = definition.get('item_type', 'Movie')
        collection_type = "mixed"
        if not (isinstance(item_type_from_db, list) and len(item_type_from_db) > 1):
             authoritative_type = item_type_from_db[0] if isinstance(item_type_from_db, list) and item_type_from_db else item_type_from_db if isinstance(item_type_from_db, str) else 'Movie'
             collection_type = "tvshows" if authoritative_type == 'Series' else "movies"

        fake_library_details = {
            "Name": coll['name'], "ServerId": real_server_id, "Id": mimicked_id,
            "Type": "CollectionFolder", # 确保稳定性的关键
            "CollectionType": collection_type, "IsFolder": True, "ImageTags": image_tags,
        }
        return Response(json.dumps(fake_library_details), mimetype='application/json')
    except Exception as e:
        logger.error(f"获取伪造库详情时出错: {e}", exc_info=True)
        return "Internal Server Error", 500

def handle_get_mimicked_library_image(path):
    try:
        tag_with_timestamp = request.args.get('tag') or request.args.get('Tag')
        if not tag_with_timestamp: return "Bad Request", 400
        real_emby_collection_id = tag_with_timestamp.split('?')[0]
        base_url, _ = _get_real_emby_url_and_key()
        image_url = f"{base_url}/Items/{real_emby_collection_id}/Images/Primary"
        headers = {key: value for key, value in request.headers if key.lower() != 'host'}
        headers['Host'] = urlparse(base_url).netloc
        resp = requests.get(image_url, headers=headers, stream=True, params=request.args)
        excluded_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
        response_headers = [(name, value) for name, value in resp.raw.headers.items() if name.lower() not in excluded_headers]
        return Response(resp.iter_content(chunk_size=8192), resp.status_code, response_headers)
    except Exception as e:
        return "Internal Proxy Error", 500

# --- ★★★ 核心修复 #1：用下面这个通用的“万能翻译”函数，替换掉旧的 a_prefixes 函数 ★★★ ---
def handle_mimicked_library_metadata_endpoint(path, mimicked_id, params):
    """
    【V2 - 通用版】
    智能处理所有针对虚拟库的元数据类请求 (如 /Genres, /Studios, /Years, /Prefixes 等)。
    """
    try:
        real_db_id = from_mimicked_id(mimicked_id)
        collection_info = db_handler.get_custom_collection_by_id(config_manager.DB_PATH, real_db_id)
        if not collection_info or not collection_info.get('emby_collection_id'):
            return Response(json.dumps([]), mimetype='application/json')

        real_emby_collection_id = collection_info.get('emby_collection_id')
        
        base_url, api_key = _get_real_emby_url_and_key()
        # 直接使用原始请求的路径，确保我们请求的是正确的端点 (e.g., /Genres)
        target_url = f"{base_url}{path}"
        
        headers = {k: v for k, v in request.headers if k.lower() not in ['host']}
        headers['Host'] = urlparse(base_url).netloc
        
        new_params = params.copy()
        new_params['ParentId'] = real_emby_collection_id # “翻译”ParentId
        new_params['api_key'] = api_key
        
        resp = requests.get(target_url, headers=headers, params=new_params, timeout=15)
        resp.raise_for_status()
        
        # 原封不动地返回从真实服务器获取到的数据
        return Response(resp.content, resp.status_code, content_type=resp.headers.get('Content-Type'))

    except Exception as e:
        logger.error(f"处理虚拟库元数据请求 '{path}' 时出错: {e}", exc_info=True)
        return Response(json.dumps([]), mimetype='application/json')
    
def handle_get_mimicked_library_items(user_id, mimicked_id, params):
    try:
        real_db_id = from_mimicked_id(mimicked_id)
        collection_info = db_handler.get_custom_collection_by_id(config_manager.DB_PATH, real_db_id)
        if not collection_info:
            return Response(json.dumps({"Items": [], "TotalRecordCount": 0}), mimetype='application/json')

        # --- 1. 获取媒体内容的逻辑保持不变 ---
        # ... (这部分获取 real_items_map 和合并 final_items 的代码完全不变) ...
        definition = json.loads(collection_info.get('definition_json', '{}'))
        real_items_map = {}
        real_emby_collection_id = collection_info.get('emby_collection_id')
        if real_emby_collection_id:
            base_url, api_key = _get_real_emby_url_and_key()
            target_url = f"{base_url}/emby/Users/{user_id}/Items"
            new_params = params.copy()
            item_type_from_db = definition.get('item_type', [])
            if isinstance(item_type_from_db, list) and len(item_type_from_db) > 1:
                new_params.pop('IncludeItemTypes', None)
            new_params["ParentId"] = real_emby_collection_id
            new_params['Fields'] = new_params.get('Fields', '') + ',ProviderIds,UserData,DateCreated,PremiereDate,CommunityRating,ProductionYear'
            new_params['api_key'] = api_key
            new_params['Limit'] = 5000
            resp = requests.get(target_url, params=new_params, timeout=30.0)
            resp.raise_for_status()
            for item in resp.json().get("Items", []):
                tmdb_id = str(item.get('ProviderIds', {}).get('Tmdb'))
                if tmdb_id: real_items_map[tmdb_id] = item
        all_db_items = json.loads(collection_info.get('generated_media_info_json', '[]'))
        final_items = []
        processed_real_tmdb_ids = set()
        for db_item in all_db_items:
            tmdb_id = str(db_item.get('tmdb_id', ''))
            if not tmdb_id: continue
            if tmdb_id in real_items_map:
                final_items.append(real_items_map[tmdb_id])
                processed_real_tmdb_ids.add(tmdb_id)
        for tmdb_id, real_item in real_items_map.items():
            if tmdb_id not in processed_real_tmdb_ids: final_items.append(real_item)
        
        # --- 2. ★★★ 核心修改：加入“是否劫持”的判断开关 ★★★ ---
        sort_by_field = definition.get('default_sort_by')

        # 只有当用户明确选择了排序字段 (且不是'none') 时，才执行劫持
        if sort_by_field and sort_by_field != 'none':
            sort_order = definition.get('default_sort_order', 'Ascending')
            is_descending = (sort_order == 'Descending')
            logger.debug(f"执行服务器端排序劫持: '{sort_by_field}' ({sort_order})")
            
            default_sort_value = 0 if sort_by_field in ['CommunityRating', 'ProductionYear'] else "0"
            try:
                final_items.sort(
                    key=lambda item: item.get(sort_by_field, default_sort_value),
                    reverse=is_descending
                )
            except TypeError:
                logger.warning(f"排序时遇到类型不匹配问题，已回退到按名称排序。")
                final_items.sort(key=lambda item: item.get('SortName', ''))
        else:
            # 如果用户选择'none'或未设置，则不执行任何排序，直接放行
            logger.debug("未设置服务器端排序，将使用Emby原生排序。")

        # --- 3. 返回最终的列表 (可能是排好序的，也可能是原生的) ---
        final_response = {"Items": final_items, "TotalRecordCount": len(final_items)}
        return Response(json.dumps(final_response), mimetype='application/json')

    except Exception as e:
        logger.error(f"处理伪造库内容时发生严重错误: {e}", exc_info=True)
        return Response(json.dumps({"Items": [], "TotalRecordCount": 0}), mimetype='application/json')

def handle_get_latest_items(user_id, params):
    try:
        base_url, api_key = _get_real_emby_url_and_key()
        virtual_library_id = params.get('ParentId') or params.get('customViewId')

        if virtual_library_id and is_mimicked_id(virtual_library_id): # <-- 【核心修改】
            logger.info(f"处理针对虚拟库 '{virtual_library_id}' 的最新媒体请求...")
            try:
                virtual_library_db_id = from_mimicked_id(virtual_library_id) # <-- 【核心修改】
            except (ValueError, TypeError):
                return Response(json.dumps([]), mimetype='application/json')

            collection_info = db_handler.get_custom_collection_by_id(config_manager.DB_PATH, virtual_library_db_id)
            if not collection_info or not collection_info.get('emby_collection_id'):
                return Response(json.dumps([]), mimetype='application/json')

            real_emby_collection_id = collection_info.get('emby_collection_id')
            latest_params = {
                "ParentId": real_emby_collection_id, "Limit": int(params.get('Limit', '20')),
                "Fields": "PrimaryImageAspectRatio,BasicSyncInfo,DateCreated", "SortBy": "DateCreated", 
                "SortOrder": "Descending", "Recursive": "true", "IncludeItemTypes": "Movie,Series",
                'api_key': api_key,
            }
            target_url = f"{base_url}/emby/Users/{user_id}/Items"
            resp = requests.get(target_url, params=latest_params, timeout=15)
            resp.raise_for_status()
            items_data = resp.json()
            return Response(json.dumps(items_data.get("Items", [])), mimetype='application/json')
        else:
            target_url = f"{base_url}/{request.path.lstrip('/')}"
            forward_headers = {k: v for k, v in request.headers if k.lower() not in ['host', 'accept-encoding']}
            forward_headers['Host'] = urlparse(base_url).netloc
            forward_params = request.args.copy()
            forward_params['api_key'] = api_key
            resp = requests.request(
                method=request.method, url=target_url, headers=forward_headers, params=forward_params,
                data=request.get_data(), stream=True, timeout=30.0
            )
            excluded_resp_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
            response_headers = [(name, value) for name, value in resp.raw.headers.items() if name.lower() not in excluded_resp_headers]
            return Response(resp.iter_content(chunk_size=8192), resp.status_code, response_headers)
    except Exception as e:
        logger.error(f"处理最新媒体时发生未知错误: {e}", exc_info=True)
        return Response(json.dumps([]), mimetype='application/json')

proxy_app = Flask(__name__)

@proxy_app.route('/', defaults={'path': ''})
@proxy_app.route('/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE', 'HEAD', 'OPTIONS'])
def proxy_all(path):
    # --- 1. WebSocket 代理逻辑 (已添加超详细日志) ---
    if 'Upgrade' in request.headers and request.headers.get('Upgrade', '').lower() == 'websocket':
        logger.info("--- 收到一个新的 WebSocket 连接请求 ---")
        ws_client = request.environ.get('wsgi.websocket')
        if not ws_client:
            logger.error("!!! WebSocket请求，但未找到 wsgi.websocket 对象。请确保以正确的 handler_class 运行。")
            return "WebSocket upgrade failed", 400

        try:
            # 1. 记录客户端信息
            logger.debug(f"  [C->P] 客户端路径: /{path}")
            logger.debug(f"  [C->P] 客户端查询参数: {request.query_string.decode()}")
            logger.debug(f"  [C->P] 客户端 Headers: {dict(request.headers)}")

            # 2. 构造目标 URL
            base_url, _ = _get_real_emby_url_and_key()
            parsed_url = urlparse(base_url)
            ws_scheme = 'wss' if parsed_url.scheme == 'https' else 'ws'
            target_ws_url = urlunparse((ws_scheme, parsed_url.netloc, f'/{path}', '', request.query_string.decode(), ''))
            logger.info(f"  [P->S] 准备连接到目标 Emby WebSocket: {target_ws_url}")

            # 3. 提取 Headers 并尝试连接
            headers_to_server = {k: v for k, v in request.headers.items() if k.lower() not in ['host', 'upgrade', 'connection', 'sec-websocket-key', 'sec-websocket-version']}
            logger.debug(f"  [P->S] 转发给服务器的 Headers: {headers_to_server}")
            
            ws_server = None
            try:
                ws_server = create_connection(target_ws_url, header=headers_to_server, timeout=10)
                logger.info("  [P<->S] ✅ 成功连接到远程 Emby WebSocket 服务器。")
            except Exception as e_connect:
                logger.error(f"  [P<->S] ❌ 连接到远程 Emby WebSocket 失败! 错误: {e_connect}", exc_info=True)
                ws_client.close()
                return Response()

            # 4. 创建双向转发协程
            def forward_to_server():
                try:
                    while not ws_client.closed and ws_server.connected:
                        message = ws_client.receive()
                        if message is not None:
                            logger.trace(f"  [C->S] 转发消息: {message[:200] if message else 'None'}") # 只记录前200字符
                            ws_server.send(message)
                        else:
                            logger.info("  [C->P] 客户端连接已关闭 (receive返回None)。")
                            break
                except Exception as e_fwd_s:
                    logger.warning(f"  [C->S] 转发到服务器时出错: {e_fwd_s}")
                finally:
                    if ws_server.connected:
                        ws_server.close()
                        logger.info("  [P->S] 已关闭到服务器的连接。")

            def forward_to_client():
                try:
                    while ws_server.connected and not ws_client.closed:
                        message = ws_server.recv()
                        if message is not None:
                            logger.trace(f"  [S->C] 转发消息: {message[:200] if message else 'None'}") # 只记录前200字符
                            ws_client.send(message)
                        else:
                            logger.info("  [P<-S] 服务器连接已关闭 (recv返回None)。")
                            break
                except Exception as e_fwd_c:
                    logger.warning(f"  [S->C] 转发到客户端时出错: {e_fwd_c}")
                finally:
                    if not ws_client.closed:
                        ws_client.close()
                        logger.info("  [P->C] 已关闭到客户端的连接。")
            
            greenlets = [spawn(forward_to_server), spawn(forward_to_client)]
            from gevent.event import Event
            exit_event = Event()
            def on_exit(g): exit_event.set()
            for g in greenlets: g.link(on_exit)
            
            logger.info("  [P<->S] WebSocket 双向转发已启动。等待连接关闭...")
            exit_event.wait()
            logger.info("--- WebSocket 会话结束 ---")

        except Exception as e:
            logger.error(f"WebSocket 代理主逻辑发生严重错误: {e}", exc_info=True)
        
        return Response()

    # --- 2. HTTP 代理逻辑 (保持不变) ---
    try:
        # 1. 定义所有需要“翻译”ParentId的元数据端点
        METADATA_ENDPOINTS = [
            '/Items/Prefixes', '/Genres', '/Studios', 
            '/Tags', '/OfficialRatings', '/Years'
        ]

        # 2. 优先处理所有元数据请求
        if any(path.endswith(endpoint) for endpoint in METADATA_ENDPOINTS):
            parent_id = request.args.get("ParentId")
            if parent_id and is_mimicked_id(parent_id):
                # 所有这类请求，都交给我们的“万能翻译”函数处理
                return handle_mimicked_library_metadata_endpoint(path, parent_id, request.args)

        # 3. 其次，处理获取库“内容”的请求 (这个逻辑我们之前已经修复好了)
        parent_id = request.args.get("ParentId")
        if parent_id and is_mimicked_id(parent_id):
            user_id_match = re.search(r'/emby/Users/([^/]+)/Items', request.path)
            if user_id_match:
                user_id = user_id_match.group(1)
                return handle_get_mimicked_library_items(user_id, parent_id, request.args)
            else:
                # 这条日志现在只会在极少数未知情况下出现，是我们的最后防线
                logger.warning(f"无法从路径 '{request.path}' 中为虚拟库请求提取user_id。")
                
        if path.startswith('emby/Items/') and '/Images/' in path and is_mimicked_id(path.split('/')[2]):
             return handle_get_mimicked_library_image(path)

        # 检查是否是请求主页媒体库列表
        if path.endswith('/Views') and path.startswith('emby/Users/'):
            return handle_get_views()

        # 检查是否是请求虚拟库的详情
        details_match = MIMICKED_ITEM_DETAILS_RE.search(f'/{path}')
        if details_match:
            user_id = details_match.group(1)
            mimicked_id = details_match.group(2) # 注意，这里是 group(2)
            return handle_get_mimicked_library_details(user_id, mimicked_id)

        # 检查是否是请求最新项目
        if path.endswith('/Items/Latest'):
            user_id_match = re.search(r'/emby/Users/([^/]+)/', f'/{path}')
            if user_id_match:
                return handle_get_latest_items(user_id_match.group(1), request.args)

        # 捕获所有对虚拟库内容的请求
        items_match = MIMICKED_ITEMS_RE.match(f'/{path}')
        if items_match:
            user_id = items_match.group(1)
            mimicked_id = items_match.group(2) # 注意，这里是 group(2)
            return handle_get_mimicked_library_items(user_id, mimicked_id, request.args)

        # --- 默认转发逻辑 (保持不变) ---
        path_to_forward = path
        if path and not path.startswith(('emby/', 'socket.io/', 'Audio/', 'Videos/', 'Items/')):
             path_to_forward = f'web/{path}'
        base_url, api_key = _get_real_emby_url_and_key()
        target_url = f"{base_url}/{path_to_forward}"
        headers = {k: v for k, v in request.headers if k.lower() not in ['host', 'accept-encoding']}
        headers['Host'] = urlparse(base_url).netloc
        headers['Accept-Encoding'] = 'identity'
        params = request.args.copy()
        params['api_key'] = api_key
        resp = requests.request(
            method=request.method, url=target_url, headers=headers, params=params,
            data=request.get_data(), cookies=request.cookies, stream=True, timeout=30.0
        )
        excluded_resp_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
        response_headers = [(name, value) for name, value in resp.raw.headers.items()
                            if name.lower() not in excluded_resp_headers]
        return Response(resp.iter_content(chunk_size=8192), resp.status_code, response_headers)
    except Exception as e:
        logger.error(f"[PROXY] HTTP 代理时发生未知错误: {e}", exc_info=True)
        return "Internal Server Error", 500