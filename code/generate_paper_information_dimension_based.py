import json
import re
import time
import hashlib
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pymongo import MongoClient
from aiTaskRunner import AITaskRunner
from config import (
    OUTPUT_BASE_DIR, CHUNKS_DIR, MERGED_DIR, RESULT_DIR,
    MONGODB_CONFIG, TASK_CONFIG, PROMPT_FILES,
    DEFAULT_EXECUTION_CONFIG, get_api_config, get_optimal_chunk_params,
    get_speculative_config, _ensure_directories_exist
)
from config import SERVICE_MAPPING

# 服务映射配置
SERVICE_MAPPING["find_content_index"] = "doubao"
SERVICE_MAPPING["generate_questions"] = "deepseek"  # 切换到稳定的本地服务
SERVICE_MAPPING["extract_rules"] = "doubao"

# 多模型配置 - 用于维度分配
MULTI_MODEL_CONFIG = {
    "enabled": True,  # 是否启用多模型分配
    "available_services": ["deepseek", "doubao"],  # 可用服务列表（减少kimi使用）
    "allocation_strategy": "round_robin",  # 分配策略：round_robin(轮询), random(随机)
    "exclude_unstable": True  # 是否排除不稳定的服务
}

# 维度大小分类配置
DIMENSION_SIZE_CONFIG = {
    "enabled": True,  # 是否启用按大小分类处理
    "small_threshold": 6000,  # 小维度阈值（字符数）
    "large_threshold": 9000,  # 大维度阈值（字符数）
    "chunk_large_dimensions": True,  # 是否对大维度进行分块
    "chunk_size": 6000,  # 大维度分块大小
    "chunk_overlap": 1000,  # 分块重叠大小
    "small_batch_size": 5,  # 小维度批处理大小
    "small_processing": {
        "preferred_services": ["deepseek", "doubao"],  # 小维度优先服务（减少kimi使用）
        "max_tokens": 6000  # 小维度token数
    },
    "medium_processing": {
        "preferred_services": ["deepseek", "doubao"],  # 中维度服务（移除kimi）
        "max_tokens": 8000  # 中维度token数
    },
    "large_processing": {
        "strategy": "chunk_and_merge",  # 大维度处理策略
        "use_powerful_models": True,  # 大维度使用更强模型
        "preferred_services": ["deepseek"],  # 大维度优先服务
        "max_tokens": 8000  # 大维度最大token数
    }
}

# ============================ 初始化设置 ============================
_ensure_directories_exist()

# ============================ 工具函数 ============================

def log_speculative_status() -> None:
    """记录 Speculative Decoding 状态"""
    print("[Speculative Decoding] 已强制禁用 - 使用标准调用模式")

def log_saved_file(file_path: Path) -> None:
    """将保存的文件路径追加写入日志文件"""
    try:
        log_file = OUTPUT_BASE_DIR / "saved_files.log"
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {file_path}\n")
        print(f"[记录] 已写入日志: {file_path}")
    except Exception as e:
        print(f"[警告] 写入日志失败: {e}")

def allocate_service_for_dimensions(dimensions: List[Dict[str, Any]]) -> List[str]:
    """
    为每个维度分配服务
    Args:
        dimensions: 维度列表
    Returns:
        List[str]: 每个维度对应的服务名称列表
    """
    if not MULTI_MODEL_CONFIG["enabled"]:
        # 如果未启用多模型，所有维度使用默认服务
        default_service = SERVICE_MAPPING.get("generate_questions", "doubao")
        return [default_service] * len(dimensions)

    available_services = MULTI_MODEL_CONFIG["available_services"].copy()
    strategy = MULTI_MODEL_CONFIG["allocation_strategy"]

    # 过滤掉不稳定的服务（如果启用了排除选项）
    if MULTI_MODEL_CONFIG.get("exclude_unstable", False):
        # 可以根据需要定义不稳定的服务
        unstable_services = ["qwen-plus"]  # 之前遇到超时的服务
        available_services = [s for s in available_services if s not in unstable_services]

    allocated_services = []

    if strategy == "round_robin":
        # 轮询分配
        for i, dimension in enumerate(dimensions):
            service = available_services[i % len(available_services)]
            allocated_services.append(service)
            print(f"[多模型分配] 维度 '{dimension['name']}' -> 服务 '{service}'")

    elif strategy == "random":
        # 随机分配
        import random
        for dimension in dimensions:
            service = random.choice(available_services)
            allocated_services.append(service)
            print(f"[多模型分配] 维度 '{dimension['name']}' -> 服务 '{service}'")

    else:
        # 默认轮询
        for i, dimension in enumerate(dimensions):
            service = available_services[i % len(available_services)]
            allocated_services.append(service)

    print(f"[多模型分配] 共分配 {len(dimensions)} 个维度到 {len(set(allocated_services))} 个不同服务")
    return allocated_services

def analyze_dimension_sizes(dimensions: List[Dict[str, Any]], chunks: List[Dict]) -> Dict[str, Any]:
    """
    分析维度大小并分类
    Args:
        dimensions: 维度列表
        chunks: 文档分块
    Returns:
        Dict: 包含分类结果的字典
    """
    if not DIMENSION_SIZE_CONFIG["enabled"]:
        return {
            "small_dimensions": dimensions,
            "medium_dimensions": [],
            "large_dimensions": [],
            "analysis": {}
        }

    small_threshold = DIMENSION_SIZE_CONFIG["small_threshold"]
    large_threshold = DIMENSION_SIZE_CONFIG["large_threshold"]

    small_dimensions = []
    medium_dimensions = []
    large_dimensions = []
    dimension_sizes = []

    print(f"[维度分析] 开始分析 {len(dimensions)} 个维度的大小")
    print(f"[维度分析] 阈值设置: 小维度 < {small_threshold}, 大维度 > {large_threshold}")

    for dimension in dimensions:
        # 计算维度内容大小
        content = extract_dimension_content(chunks, dimension, dimensions)
        content_size = len(content)
        dimension_sizes.append(content_size)

        # 添加大小信息到维度
        dimension["content_size"] = content_size
        dimension["content_preview"] = content[:200] + "..." if len(content) > 200 else content

        # 分类
        if content_size <= small_threshold:
            small_dimensions.append(dimension)
            category = "小维度"
        elif content_size >= large_threshold:
            large_dimensions.append(dimension)
            category = "大维度"
        else:
            medium_dimensions.append(dimension)
            category = "中维度"

        print(f"[维度分析] '{dimension['name']}': {content_size} 字符 ({category})")

    # 统计分析
    analysis = {
        "total_count": len(dimensions),
        "small_count": len(small_dimensions),
        "medium_count": len(medium_dimensions),
        "large_count": len(large_dimensions),
        "sizes": dimension_sizes,
        "avg_size": sum(dimension_sizes) / len(dimension_sizes) if dimension_sizes else 0,
        "max_size": max(dimension_sizes) if dimension_sizes else 0,
        "min_size": min(dimension_sizes) if dimension_sizes else 0
    }

    print(f"[维度分析] 分类结果: 小维度 {analysis['small_count']} 个, 中维度 {analysis['medium_count']} 个, 大维度 {analysis['large_count']} 个")
    print(f"[维度分析] 大小统计: 平均 {analysis['avg_size']:.0f} 字符, 最大 {analysis['max_size']} 字符, 最小 {analysis['min_size']} 字符")

    return {
        "small_dimensions": small_dimensions,
        "medium_dimensions": medium_dimensions,
        "large_dimensions": large_dimensions,
        "analysis": analysis
    }

def find_question_boundaries(content: str) -> List[int]:
    """
    找到题目边界位置
    Returns:
        List[int]: 题目开始位置的索引列表
    """
    import re
    boundaries = [0]  # 开始位置

    # 题目编号模式
    question_patterns = [
        r'\n\s*(\d+)\s*[．.、]\s*',      # 1. 1． 1、
        r'\n\s*\((\d+)\)\s*',           # (1) (2)
        r'\n\s*（(\d+)）\s*',           # （1）（2）
        r'\n\s*[一二三四五六七八九十]+\s*[．.、]\s*',  # 一、二、
        r'\n\s*\([一二三四五六七八九十]+\)\s*',      # (一)(二)
        r'\n\s*（[一二三四五六七八九十]+）\s*',      # （一）（二）
    ]

    for pattern in question_patterns:
        matches = re.finditer(pattern, content)
        for match in matches:
            pos = match.start() + 1  # 跳过换行符
            if pos not in boundaries:
                boundaries.append(pos)

    # 排序并去重
    boundaries = sorted(set(boundaries))
    boundaries.append(len(content))  # 添加结束位置

    return boundaries

def chunk_large_dimension(dimension: Dict[str, Any], content: str) -> List[Dict[str, Any]]:
    """
    语义感知的大维度分块处理
    Args:
        dimension: 维度信息
        content: 维度内容
    Returns:
        List[Dict]: 分块后的子维度列表
    """
    if not DIMENSION_SIZE_CONFIG["chunk_large_dimensions"]:
        return [dimension]

    chunk_size = DIMENSION_SIZE_CONFIG["chunk_size"]
    chunk_overlap = DIMENSION_SIZE_CONFIG["chunk_overlap"]

    if len(content) <= chunk_size:
        return [dimension]

    print(f"[智能分块] 开始分块维度 '{dimension['name']}', 内容长度: {len(content)} 字符")

    # 1. 找到所有题目边界
    question_boundaries = find_question_boundaries(content)
    print(f"[智能分块] 检测到 {len(question_boundaries)-1} 个题目边界")

    # 2. 语义感知分块
    chunks = []
    chunk_index = 1
    current_start = 0

    while current_start < len(content):
        # 寻找最佳分块点
        optimal_end = current_start + chunk_size

        if optimal_end >= len(content):
            # 最后一块
            chunk_content = content[current_start:]
            chunk_dimension = create_chunk_dimension(dimension, chunk_content, chunk_index, current_start, len(content))
            chunks.append(chunk_dimension)
            break

        # 在chunk_size范围内找到最佳的题目边界
        best_boundary = optimal_end
        for boundary in question_boundaries:
            if current_start < boundary <= optimal_end + 1000:  # 允许适当超出
                best_boundary = boundary
            elif boundary > optimal_end + 1000:
                break

        # 如果没有找到好的边界，使用智能切分
        if best_boundary == optimal_end:
            best_boundary = find_safe_split_point(content, current_start, optimal_end)

        # 创建分块
        chunk_content = content[current_start:best_boundary]

        # 质量检查
        if validate_chunk_quality(chunk_content):
            chunk_dimension = create_chunk_dimension(dimension, chunk_content, chunk_index, current_start, best_boundary)
            chunks.append(chunk_dimension)

            # 计算下一个起始点（考虑重叠）
            if best_boundary < len(content):
                overlap_start = max(current_start, best_boundary - chunk_overlap)
                # 寻找重叠区域内的合适起始点
                next_start = find_overlap_start_point(content, overlap_start, best_boundary)
                current_start = next_start
            else:
                current_start = best_boundary

            chunk_index += 1
        else:
            print(f"[智能分块] 分块质量不佳，调整分块点")
            # 如果质量不佳，缩小分块
            best_boundary = find_safe_split_point(content, current_start, current_start + chunk_size // 2)
            chunk_content = content[current_start:best_boundary]
            chunk_dimension = create_chunk_dimension(dimension, chunk_content, chunk_index, current_start, best_boundary)
            chunks.append(chunk_dimension)
            current_start = best_boundary
            chunk_index += 1

    print(f"[智能分块] 维度 '{dimension['name']}' 分成 {len(chunks)} 个语义完整的分块")

    # 分块质量报告
    for i, chunk in enumerate(chunks):
        content_preview = chunk["chunk_content"][:100].replace('\n', ' ')
        print(f"[分块{i+1}] {chunk['content_size']} 字符: {content_preview}...")

    return chunks

def create_chunk_dimension(dimension: Dict[str, Any], chunk_content: str, chunk_index: int, start_pos: int, end_pos: int) -> Dict[str, Any]:
    """创建分块维度对象"""
    chunk_dimension = dimension.copy()
    chunk_dimension["name"] = f"{dimension['name']}_分块{chunk_index}"
    chunk_dimension["original_name"] = dimension["name"]
    chunk_dimension["is_chunk"] = True
    chunk_dimension["chunk_index"] = chunk_index
    chunk_dimension["content_size"] = len(chunk_content)
    chunk_dimension["chunk_content"] = chunk_content
    chunk_dimension["start_position"] = start_pos
    chunk_dimension["end_position"] = end_pos
    return chunk_dimension

def find_safe_split_point(content: str, start: int, target_end: int) -> int:
    """找到安全的分块点，避免破坏语义结构"""
    # 优先级：题目边界 > 段落边界 > 句子边界 > 标点符号

    # 1. 在目标范围内寻找题目开始
    question_pattern = r'(\n\s*[\d（(][\d)）]?\s*[．.、)]?\s*)'
    import re
    matches = list(re.finditer(question_pattern, content[start:target_end]))
    if matches:
        last_match = matches[-1]
        return start + last_match.start() + 1

    # 2. 寻找段落边界（连续换行）
    paragraph_pattern = r'\n\s*\n'
    matches = list(re.finditer(paragraph_pattern, content[start:target_end]))
    if matches:
        last_match = matches[-1]
        return start + last_match.end()

    # 3. 寻找句子边界
    sentence_ends = ['。', '！', '？', '.', '!', '?']
    for i in range(target_end - 1, start, -1):
        if content[i] in sentence_ends and i + 1 < len(content) and content[i + 1] in ['\n', ' ']:
            return i + 1

    # 4. 寻找其他标点符号
    punctuation = ['；', ';', '，', ',', '：', ':']
    for i in range(target_end - 1, start, -1):
        if content[i] in punctuation:
            return i + 1

    # 5. 最后寻找空格
    for i in range(target_end - 1, start, -1):
        if content[i] == ' ':
            return i + 1

    return target_end

def find_overlap_start_point(content: str, overlap_start: int, chunk_end: int) -> int:
    """在重叠区域内找到合适的起始点"""
    # 寻找题目开始作为新的起始点
    import re
    question_pattern = r'(\n\s*[\d（(][\d)）]?\s*[．.、)]?\s*)'
    matches = list(re.finditer(question_pattern, content[overlap_start:chunk_end]))
    if matches:
        # 选择最后一个题目开始
        last_match = matches[-1]
        return overlap_start + last_match.start() + 1

    return overlap_start

def validate_chunk_quality(chunk_content: str) -> bool:
    """验证分块质量"""
    if len(chunk_content.strip()) < 100:  # 太短
        return False

    # 检查是否包含完整的题目
    import re
    question_pattern = r'[\d（(][\d)）]?\s*[．.、)]?\s*.{10,}'
    if not re.search(question_pattern, chunk_content):
        return False

    # 检查是否有明显的截断（如末尾没有标点符号）
    last_char = chunk_content.strip()[-1] if chunk_content.strip() else ''
    if last_char.isalnum():  # 以字母或数字结尾，可能被截断
        return False

    return True

def process_large_dimensions_separately(
    large_dimensions: List[Dict[str, Any]],
    chunks: List[Dict],
    **kwargs
) -> List[Dict[str, Any]]:
    """
    单独处理大维度
    """
    if not large_dimensions:
        return []

    print(f"[大维度处理] 开始处理 {len(large_dimensions)} 个大维度")

    # 对大维度进行分块
    all_chunked_dimensions = []
    for dimension in large_dimensions:
        content = dimension.get("content_preview", "") or extract_dimension_content(chunks, dimension, large_dimensions)
        chunked_dims = chunk_large_dimension(dimension, content)
        all_chunked_dimensions.extend(chunked_dims)

    print(f"[大维度处理] 分块后共 {len(all_chunked_dimensions)} 个处理单元")

    # 为大维度分配更强的服务
    if DIMENSION_SIZE_CONFIG["large_processing"]["use_powerful_models"]:
        preferred_services = DIMENSION_SIZE_CONFIG["large_processing"]["preferred_services"]
        print(f"[大维度处理] 使用优选服务: {preferred_services}")

        # 重新配置服务分配
        original_services = MULTI_MODEL_CONFIG["available_services"].copy()
        MULTI_MODEL_CONFIG["available_services"] = preferred_services

        allocated_services = allocate_service_for_dimensions(all_chunked_dimensions)

        # 恢复原始配置
        MULTI_MODEL_CONFIG["available_services"] = original_services
    else:
        allocated_services = allocate_service_for_dimensions(all_chunked_dimensions)

    # 使用更高的token限制
    max_tokens = DIMENSION_SIZE_CONFIG["large_processing"]["max_tokens"]

    # 并行处理分块
    results = []
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def process_single_large_dimension(dim_and_service):
        dimension, service_name = dim_and_service
        return generate_questions_by_dimension(
            dimension=dimension,
            chunks=chunks,
            all_dimensions=all_chunked_dimensions,
            service_name=service_name,
            max_tokens=max_tokens,
            **kwargs
        )

    dimension_service_pairs = list(zip(all_chunked_dimensions, allocated_services))

    with ThreadPoolExecutor(max_workers=min(len(all_chunked_dimensions), 10)) as executor:
        futures = [executor.submit(process_single_large_dimension, pair) for pair in dimension_service_pairs]
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            dim_name = result.get("dimension", "未知")
            success = result.get("success", False)
            question_count = len(result.get("questions", []))
            print(f"[大维度处理] 分块 '{dim_name}' 完成: {'成功' if success else '失败'}, 题目数: {question_count}")

    return results

def load_chunks_json(file_path: str) -> List[Dict[str, Any]]:
    """加载分块JSON文件"""
    file_path_obj = Path(file_path)
    if not file_path_obj.exists():
        print(f"[错误] chunks 文件不存在: {file_path}")
        return []

    try:
        with open(file_path_obj, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception as e:
        print(f"[错误] 读取 JSON 失败: {e}")
        return []

def create_safe_filename(title: str) -> str:
    """创建安全的文件名"""
    return "".join(c for c in title if c.isalnum() or c in " _-")

def extract_content_by_index_range(chunks: List[Dict], start_idx: int, end_idx: int) -> str:
    """根据索引范围提取内容（不进行边界清理）"""
    content_list = []
    for idx in range(start_idx, end_idx + 1):
        list_idx = idx - 1  # 转换为列表索引
        if 0 <= list_idx < len(chunks):
            content = chunks[list_idx].get("content", "")
            if content.strip():
                content_list.append(content)
        else:
            print(f"[警告] 索引 {idx} 超出范围 (chunks总数={len(chunks)})")

    merged_content = "\n".join(content_list)
    print(f"[内容提取] 索引范围 {start_idx}-{end_idx}，提取内容长度: {len(merged_content)} 字符")
    return merged_content

def extract_dimension_content(chunks: List[Dict], dimension: Dict[str, Any], all_dimensions: List[Dict[str, Any]]) -> str:
    """
    为指定维度提取内容并进行精确的边界清理
    Args:
        chunks: 文档分块
        dimension: 当前维度信息
        all_dimensions: 所有维度列表（用于判断是否为第一个或最后一个）
    """
    start_idx = dimension["start_index"]
    end_idx = dimension["end_index"]

    # 获取原始内容
    raw_content = extract_content_by_index_range(chunks, start_idx, end_idx)

    # 判断维度位置
    is_first = dimension == all_dimensions[0] if all_dimensions else False
    is_last = dimension == all_dimensions[-1] if all_dimensions else False

    # 应用维度级别的边界清理
    cleaned_content = clean_dimension_boundary(
        content=raw_content,
        dimension_info=dimension,
        is_first=is_first,
        is_last=is_last
    )

    return cleaned_content

def clean_dimension_boundary(content: str, dimension_info: Dict[str, Any], is_first: bool = False, is_last: bool = False) -> str:
    """
    根据维度信息精确清理内容边界
    Args:
        content: 原始内容
        dimension_info: 维度信息，包含name等字段
        is_first: 是否为第一个维度
        is_last: 是否为最后一个维度
    """
    if not is_first and not is_last:
        # 中间维度保持完整，不做边界清理
        print(f"[维度边界] 中间维度 '{dimension_info.get('name', '')}' 保持完整内容")
        return content

    lines = content.split('\n')
    dimension_name = dimension_info.get('name', '')

    # 定义评估标题的模式
    evaluation_patterns = [
        r'全市性.*?评估指标$',
        r'.*?评估标准$',
        r'.*?评估办法$',
        r'.*?评估体系$',
        r'.*?管理评估.*?$',
        r'民办非企业单位.*?评估.*?指标$',
        r'基金会.*?评估.*?指标$',
        r'社会团体.*?评估.*?指标$',
        r'异地商会.*?评估.*?指标$',
        r'行业协会.*?评估.*?指标$',
        r'区域性.*?评估.*?$',
        r'.*?商会.*?评估.*?$'
    ]

    cleaned_lines = []
    start_collecting = not is_first  # 如果不是第一个维度，从开始就收集

    for i, line in enumerate(lines):
        line_stripped = line.strip()

        # 第一个维度：查找维度标题，从标题开始收集
        if is_first and not start_collecting:
            # 检查是否找到了当前维度的标题
            if dimension_name in line_stripped:
                print(f"[第一维度] 找到维度标题: {line_stripped}")
                start_collecting = True
                cleaned_lines.append(line)
                continue
            else:
                # 跳过标题前的内容
                continue

        # 最后一个维度：检测下一个评估标题，停止收集
        if is_last:
            found_next_evaluation = False
            for pattern in evaluation_patterns:
                if re.search(pattern, line_stripped):
                    # 验证这确实是一个新的评估标题
                    if (len(line_stripped) < 25 and
                        '评估' in line_stripped and
                        ('指标' in line_stripped or '标准' in line_stripped or '办法' in line_stripped or '体系' in line_stripped)):
                        print(f"[最后维度] 发现下一个评估标题，停止收集: {line_stripped}")
                        found_next_evaluation = True
                        break

            if found_next_evaluation:
                break

        # 收集内容
        if start_collecting:
            cleaned_lines.append(line)

    cleaned_content = '\n'.join(cleaned_lines)

    # 统计信息
    original_len = len(content)
    cleaned_len = len(cleaned_content)
    removed_len = original_len - cleaned_len

    if is_first and removed_len > 0:
        print(f"[第一维度清理] 移除标题前内容: {removed_len} 字符")
    elif is_last and removed_len > 0:
        print(f"[最后维度清理] 移除下一评估标题后内容: {removed_len} 字符")

    print(f"[维度边界] '{dimension_name}' 清理完成: {original_len} -> {cleaned_len} 字符")
    return cleaned_content

def clean_content_boundary(content: str) -> str:
    """
    保持向后兼容的边界清理函数（已弃用，建议使用clean_dimension_boundary）
    """
    print("[警告] clean_content_boundary已弃用，建议使用clean_dimension_boundary")
    return content

# ============================ 数据库操作 ============================

def extract_chunks_to_file(
    mongo_host: str = MONGODB_CONFIG.get("host", "localhost"),
    mongo_port: int = MONGODB_CONFIG.get("port", 27017),
    mongo_user: Optional[str] = MONGODB_CONFIG.get("username"),
    mongo_password: Optional[str] = MONGODB_CONFIG.get("password"),
    db_name: str = MONGODB_CONFIG.get("db_name", "doc_db"),
    collection_name: str = MONGODB_CONFIG.get("collection_name", "doc_batches"),
    output_dir: Path = CHUNKS_DIR,
    target_batch_id: Optional[int] = None
) -> Optional[Dict[str, Any]]:
    """从MongoDB提取分块数据并保存到文件"""
    try:
        # 构建连接
        if mongo_user and mongo_password:
            client = MongoClient(
                host=mongo_host,
                port=mongo_port,
                username=mongo_user,
                password=mongo_password,
                authSource=MONGODB_CONFIG.get("auth_source", "admin"),
                authMechanism=MONGODB_CONFIG.get("auth_mechanism", "SCRAM-SHA-256"),
                serverSelectionTimeoutMS=MONGODB_CONFIG.get("server_selection_timeout_ms", 5000)
            )
        else:
            client = MongoClient(
                host=mongo_host,
                port=mongo_port,
                serverSelectionTimeoutMS=MONGODB_CONFIG.get("server_selection_timeout_ms", 5000)
            )

        client.admin.command("ping")
        collection = client[db_name][collection_name]

        # 获取目标 batch
        if target_batch_id is not None:
            last_record = collection.find_one({"batch_id": target_batch_id})
            if not last_record:
                print(f"[警告] 未找到 batch_id={target_batch_id} 的记录")
                client.close()
                return None
        else:
            last_record = collection.find_one(sort=[("batch_id", -1)])
            if not last_record:
                print(f"[提示] 数据库 {collection_name} 中没有分块数据")
                client.close()
                return None

        batch_id = last_record["batch_id"]
        chunks = last_record.get("chunks", [])
        client.close()

        if not chunks:
            print(f"[提示] batch_id={batch_id} 下未提取到任何分块数据")
            return None

        # 保存到文件
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_subdir = output_dir / f"extract_{timestamp}"
        output_subdir.mkdir(parents=True, exist_ok=True)
        json_file = output_subdir / f"chunks_batch{batch_id}_{timestamp}.json"

        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(chunks, f, ensure_ascii=False, indent=2)

        print(f"[完成] 已保存 batch_id={batch_id} 的分块数据: {json_file} (共 {len(chunks)} 个)")
        log_saved_file(json_file)
        return {"batch_id": batch_id, "chunks": chunks, "chunks_file": str(json_file)}

    except Exception as e:
        print(f"[错误] MongoDB 连接或查询失败: {e}")
        return None

# ============================ 增强的索引提取功能 ============================

def find_content_index_with_dimensions(
    title: str,
    chunks_file: str,
    api_key: str = None,
    prompt_file: str = PROMPT_FILES["index"],
    output_dir: Path = MERGED_DIR,
    base_url: str = None,
    model: str = None,
    retry: int = TASK_CONFIG["retry_count"]
) -> Optional[Dict[str, Any]]:
    """
    查找标题对应的内容索引并识别所有维度
    返回增强的结果，包含维度信息
    """
    chunks = load_chunks_json(chunks_file)
    if not chunks:
        print("[错误] 加载 chunks 失败，无法继续")
        return None

    # 获取 API 配置
    if not api_key or not base_url or not model:
        config = get_api_config("find_content_index")
        api_key = api_key or config["api_key"]
        base_url = base_url or config["base_url"]
        model = model or config["model"]
        print(f"[调试] find_content_index_with_dimensions 使用配置: {base_url} | {model}")

    try:
        with open(prompt_file, "r", encoding="utf-8") as f:
            base_prompt_template = f.read().strip()
    except Exception as e:
        print(f"[错误] 读取提示词文件失败: {e}")
        return None

    # 增强提示词，要求同时识别维度
    enhanced_prompt_template = base_prompt_template + """

**额外要求：维度识别**
在找到目标内容后，请同时分析整个内容范围，识别其中包含的所有评估维度。

请在返回的JSON中增加以下字段：
```json
{
  // ... 原有字段 ...
  "dimensions": [
    {
      "name": "维度名称",
      "start_index": 维度起始索引,
      "end_index": 维度结束索引,
      "description": "维度描述"
    }
  ]
}
```

**维度识别规则：**
1. 维度通常是最大的分类层级（如"一、二、三、"或"（一）（二）（三）"）
2. 每个维度包含一组相关的评估题目
3. 维度边界以下一个同级维度标题为准
4. 确保维度索引范围不重叠且完整覆盖内容范围
"""

    doc_text = "\n".join([
        f"Index {c.get('chunk_index', idx+1)}:\n{c.get('content', '')}"
        for idx, c in enumerate(chunks)
    ])
    prompt = enhanced_prompt_template.replace("{target_title}", title).replace("{doc_text}", doc_text)

    # 获取 Speculative Decoding 配置
    spec_config = get_speculative_config("find_content_index")
    runner = AITaskRunner(
        api_key=api_key,
        base_url=base_url,
        model=model,
        enable_speculative=False,  # 强制禁用Speculative
        speculative_times=1
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    for attempt in range(retry):
        try:
            res = runner.run_task(prompt, response_format="json")
            print(f"[AI 响应 - 尝试 {attempt+1}]: {res}")

            if not isinstance(res, dict):
                continue
            if not res.get("found", False):
                print(f"[警告] AI 未找到标题: {title}")
                continue

            start_idx = res.get("start_index")
            end_idx = res.get("end_index")
            dimensions = res.get("dimensions", [])

            print(f"[调试] AI返回索引范围: start_idx={start_idx}, end_idx={end_idx}")
            print(f"[调试] AI识别的维度数量: {len(dimensions)}")

            if start_idx is None or end_idx is None:
                print("[警告] 未获取到有效的索引范围")
                continue

            # 验证维度信息
            valid_dimensions = []
            for dim in dimensions:
                if (dim.get("start_index") is not None and
                    dim.get("end_index") is not None and
                    dim.get("name")):
                    valid_dimensions.append(dim)
                    print(f"[维度] {dim['name']}: 索引 {dim['start_index']}-{dim['end_index']}")

            if not valid_dimensions:
                print("[警告] 未识别到有效维度，将使用整体内容作为单一维度")
                valid_dimensions = [{
                    "name": title,
                    "start_index": start_idx,
                    "end_index": end_idx,
                    "description": "整体内容"
                }]

            # 提取并保存合并内容
            merged_content = extract_content_by_index_range(chunks, start_idx, end_idx)
            safe_title = create_safe_filename(title)
            merged_file = output_dir / f"{safe_title}_merged.json"

            with open(merged_file, "w", encoding="utf-8") as f:
                json.dump({
                    "title": title,
                    "content": merged_content,
                    "dimensions": valid_dimensions
                }, f, ensure_ascii=False, indent=2)
            log_saved_file(merged_file)

            print(f"[完成] 内容索引和维度识别完成: {merged_file}")

            return {
                "title": title,
                "start_index": start_idx,
                "end_index": end_idx,
                "dimensions": valid_dimensions,
                "merged_content": merged_content,
                "merged_content_file": str(merged_file),
                "chunks": chunks,
                "total_count": res.get("total_count", len(range(start_idx, end_idx + 1))),
                "stop_reason": res.get("stop_reason", ""),
                "content_summary": res.get("content_summary", "")
            }

        except Exception as e:
            print(f"[警告] AI 调用失败 (尝试 {attempt+1}/{retry}): {e}")
            continue

    print("[失败] 所有重试均未成功")
    return None

# ============================ 按维度生成题目 ============================

def generate_questions_by_dimension(
    dimension: Dict[str, Any],
    chunks: List[Dict],
    all_dimensions: List[Dict[str, Any]] = None,
    api_key: str = None,
    prompt_file: str = PROMPT_FILES["question"],
    base_url: str = None,
    model: str = None,
    max_tokens: int = TASK_CONFIG["max_tokens"],
    retry: int = TASK_CONFIG["retry_count"],
    service_name: str = None  # 新增：指定使用的服务
) -> Dict[str, Any]:
    """
    为单个维度生成结构化题目
    """
    dimension_name = dimension["name"]
    start_idx = dimension["start_index"]
    end_idx = dimension["end_index"]

    print(f"[维度处理] 开始处理维度: {dimension_name} (索引 {start_idx}-{end_idx})")

    # 获取 API 配置（支持指定服务）
    if not api_key or not base_url or not model:
        if service_name:
            # 使用指定的服务获取配置
            from config import API_SERVICES, API_KEYS
            if service_name in API_SERVICES and service_name in API_KEYS:
                config = API_SERVICES[service_name].copy()
                config["api_key"] = API_KEYS[service_name]
                print(f"[维度 {dimension_name}] 使用指定服务: {service_name}")
            else:
                print(f"[警告] 服务 {service_name} 不存在，使用默认配置")
                config = get_api_config("generate_questions")
        else:
            config = get_api_config("generate_questions")

        api_key = api_key or config["api_key"]
        base_url = base_url or config["base_url"]
        model = model or config["model"]
        print(f"[维度 {dimension_name}] 使用配置: {base_url} | {model}")

    # 读取提示词模板
    try:
        with open(prompt_file, "r", encoding="utf-8") as f:
            template = f.read().strip()
    except Exception as e:
        return {
            "dimension": dimension_name,
            "questions": [],
            "error": f"无法读取提示词: {e}"
        }

    # 提取维度内容（使用新的边界清理逻辑）
    dimension_content = extract_dimension_content(chunks, dimension, all_dimensions or [])
    if not dimension_content.strip():
        print(f"[警告] 维度 {dimension_name} 内容为空")
        return {
            "dimension": dimension_name,
            "questions": [],
            "error": "维度内容为空"
        }

    print(f"[维度处理] 维度 {dimension_name} 内容长度: {len(dimension_content)} 字符")

    # 增强提示词，添加维度上下文
    enhanced_template = template + f"""

**当前处理维度**: {dimension_name}
**维度说明**: {dimension.get('description', '无')}

请确保提取的题目都属于"{dimension_name}"这个维度，题号应在该维度内保持连续性。
"""

    prompt = enhanced_template.replace("{doc_content}", dimension_content)

    # 获取 Speculative Decoding 配置
    spec_config = get_speculative_config("generate_questions")
    runner = AITaskRunner(
        api_key=api_key,
        base_url=base_url,
        model=model,
        max_tokens=max_tokens,
        enable_speculative=False,  # 强制禁用Speculative
        speculative_times=1
    )

    for attempt in range(retry):
        try:
            res = runner.run_task(prompt, response_format="json")
            print(f"[维度 {dimension_name}] AI响应 (尝试 {attempt+1}): {type(res)}")
            if isinstance(res, str):
                print(f"[调试] 响应内容预览: {res[:200]}...")

            if isinstance(res, str):
                cleaned = re.sub(r"```(?:json)?\n(.*?)```", r"\1", res, flags=re.S).strip()
                cleaned = cleaned.replace("<result>", "").replace("</result>", "").strip()
                try:
                    result_data = json.loads(cleaned)
                except json.JSONDecodeError as e:
                    print(f"[维度 {dimension_name}] JSON解析失败: {e}")
                    print(f"[调试] 原始响应类型: {type(res)}")
                    print(f"[调试] 清理后内容: {cleaned[:500]}...")
                    continue
            else:
                result_data = res

            if not isinstance(result_data, dict):
                continue

            # 尝试多种可能的字段名
            question_data = (result_data.get("question_data", []) or
                           result_data.get("questions", []) or
                           result_data.get("data", []) or
                           result_data.get("items", []) or
                           result_data.get("results", []))

            if not question_data:
                print(f"[维度 {dimension_name}] 未提取到题目数据")
                print(f"[调试] AI返回的字段: {list(result_data.keys())}")
                print(f"[调试] AI返回内容预览: {str(result_data)[:500]}...")
                continue

            # 验证并整理题目数据，确保按照指定格式输出
            valid_questions = []
            total_questions = 0

            # 处理题目数据，不管原始结构如何，都整理成统一格式
            for dim_group in question_data:
                if isinstance(dim_group, dict):
                    # 如果有questions字段，提取其中的题目
                    if "questions" in dim_group:
                        questions = dim_group["questions"]
                        for q in questions:
                            if (q.get("sort_number") is not None and
                                q.get("standard_score") is not None and
                                q.get("stem")):
                                valid_questions.append(q)
                                total_questions += 1
                    # 如果直接就是题目对象
                    elif (dim_group.get("sort_number") is not None and
                          dim_group.get("standard_score") is not None and
                          dim_group.get("stem")):
                        valid_questions.append(dim_group)
                        total_questions += 1

            # 按题号排序，确保连续性
            valid_questions.sort(key=lambda x: x.get("sort_number", 0))

            print(f"[维度 {dimension_name}] 成功提取 {total_questions} 个有效题目")

            # 返回符合用户要求的格式：dimension + questions数组
            return {
                "dimension": dimension_name,
                "questions": valid_questions,
                "success": True
            }

        except Exception as e:
            print(f"[维度 {dimension_name}] 处理失败 (尝试 {attempt+1}/{retry}): {e}")
            continue

    return {
        "dimension": dimension_name,
        "questions": [],
        "error": f"所有重试均失败",
        "success": False
    }

def generate_questions_by_dimensions_parallel(
    dimensions: List[Dict[str, Any]],
    chunks: List[Dict],
    api_key: str = None,
    prompt_file: str = PROMPT_FILES["question"],
    output_dir: Path = RESULT_DIR,
    base_url: str = None,
    model: str = None,
    max_tokens: int = TASK_CONFIG["max_tokens"],
    retry: int = TASK_CONFIG["retry_count"],
    max_workers: int = TASK_CONFIG["max_workers"],
    renumber_questions: bool = True  # 新增：是否重新编号题目
) -> Dict[str, Any]:
    """
    并行处理所有维度，生成结构化题目
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"[并行处理] 开始并行处理 {len(dimensions)} 个维度")

    # 分析维度大小并分类
    dimension_analysis = analyze_dimension_sizes(dimensions, chunks)
    small_dimensions = dimension_analysis["small_dimensions"]
    medium_dimensions = dimension_analysis["medium_dimensions"]
    large_dimensions = dimension_analysis["large_dimensions"]

    print(f"[维度分类] 小维度: {len(small_dimensions)} 个, 中维度: {len(medium_dimensions)} 个, 大维度: {len(large_dimensions)} 个")

    # 🚀 全新并行策略：所有处理单元同时并行执行
    print(f"\n[超级并行] 准备所有处理单元...")

    all_processing_units = []  # 所有需要处理的单元

    # 1. 准备大维度分块单元
    if large_dimensions:
        print(f"[超级并行] 准备大维度分块...")
        for dimension in large_dimensions:
            content = extract_dimension_content(chunks, dimension, large_dimensions)
            chunked_dims = chunk_large_dimension(dimension, content)
            for chunk_dim in chunked_dims:
                # 为大维度分块分配强力服务
                preferred_services = DIMENSION_SIZE_CONFIG["large_processing"]["preferred_services"]
                service = preferred_services[len(all_processing_units) % len(preferred_services)]
                all_processing_units.append({
                    "dimension": chunk_dim,
                    "service": service,
                    "max_tokens": DIMENSION_SIZE_CONFIG["large_processing"]["max_tokens"],
                    "type": "large_chunk"
                })

    # 2. 准备小维度单元
    if small_dimensions:
        print(f"[超级并行] 准备小维度...")
        preferred_services = DIMENSION_SIZE_CONFIG["small_processing"]["preferred_services"]
        for i, dimension in enumerate(small_dimensions):
            service = preferred_services[i % len(preferred_services)]
            all_processing_units.append({
                "dimension": dimension,
                "service": service,
                "max_tokens": DIMENSION_SIZE_CONFIG["small_processing"]["max_tokens"],
                "type": "small"
            })

    # 3. 准备中维度单元
    if medium_dimensions:
        print(f"[超级并行] 准备中维度...")
        preferred_services = DIMENSION_SIZE_CONFIG["medium_processing"]["preferred_services"]
        for i, dimension in enumerate(medium_dimensions):
            service = preferred_services[i % len(preferred_services)]
            all_processing_units.append({
                "dimension": dimension,
                "service": service,
                "max_tokens": DIMENSION_SIZE_CONFIG["medium_processing"]["max_tokens"],
                "type": "medium"
            })

    print(f"[超级并行] 总共准备了 {len(all_processing_units)} 个处理单元，即将同时并行执行！")

    # 统计服务分布
    service_stats = {}
    for unit in all_processing_units:
        service_stats[unit["service"]] = service_stats.get(unit["service"], 0) + 1
    print(f"[超级并行] 服务分布: {service_stats}")

    def process_any_unit(processing_unit):
        """通用处理单元函数"""
        dimension = processing_unit["dimension"]
        service_name = processing_unit["service"]
        max_tokens = processing_unit["max_tokens"]
        unit_type = processing_unit["type"]

        return generate_questions_by_dimension(
            dimension=dimension,
            chunks=chunks,
            all_dimensions=dimensions,  # 传递所有维度用于上下文
            api_key=api_key,
            prompt_file=prompt_file,
            base_url=base_url,
            model=model,
            max_tokens=max_tokens,
            retry=retry,
            service_name=service_name
        )

    # 🚀 超级并行执行：所有单元同时处理
    print(f"\n[超级并行] 启动 {len(all_processing_units)} 个并行任务...")
    all_results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        futures = [executor.submit(process_any_unit, unit) for unit in all_processing_units]

        # 实时收集结果
        completed_count = 0
        for future in as_completed(futures):
            result = future.result()
            all_results.append(result)
            completed_count += 1

            # 实时进度报告
            dim_name = result.get("dimension", "未知")
            success = result.get("success", False)
            question_count = len(result.get("questions", []))

            # 识别处理单元类型
            unit_type = "未知"
            if "_分块" in dim_name:
                unit_type = "大维度分块"
            elif any(unit["dimension"]["name"] == dim_name for unit in all_processing_units if unit["type"] == "small"):
                unit_type = "小维度"
            elif any(unit["dimension"]["name"] == dim_name for unit in all_processing_units if unit["type"] == "medium"):
                unit_type = "中维度"

            progress = f"({completed_count}/{len(all_processing_units)})"
            print(f"[超级并行] {progress} {unit_type} '{dim_name}' 完成: {'✅成功' if success else '❌失败'}, 题目数: {question_count}")

    print(f"\n[超级并行] 🎉 所有 {len(all_processing_units)} 个处理单元已并行完成！")

    print(f"\n[处理完成] 总共处理了 {len(all_results)} 个处理单元")

    # 合并所有结果
    all_questions_by_dimension = []
    total_questions = 0
    successful_dimensions = 0
    current_question_number = 1  # 从第1题开始连续编号

    # 处理结果，合并分块的大维度
    merged_results = {}
    for result in all_results:
        if result.get("success", False) and result.get("questions"):
            dimension_name = result["dimension"]

            # 检查是否为分块
            if "_分块" in dimension_name:
                original_name = result.get("original_name") or dimension_name.split("_分块")[0]
                if original_name not in merged_results:
                    merged_results[original_name] = {
                        "dimension": original_name,
                        "questions": [],
                        "chunks": []
                    }
                merged_results[original_name]["questions"].extend(result["questions"])
                merged_results[original_name]["chunks"].append(result)
                print(f"[分块合并] 合并分块 '{dimension_name}' 到 '{original_name}', 题目数: {len(result['questions'])}")
            else:
                merged_results[dimension_name] = result

    # 按原始维度顺序排序
    original_dimension_names = [d["name"] for d in dimensions]
    sorted_merged_results = []

    for dim_name in original_dimension_names:
        if dim_name in merged_results:
            sorted_merged_results.append(merged_results[dim_name])
        else:
            # 查找可能的分块合并结果
            for merged_name, merged_result in merged_results.items():
                if merged_name.startswith(dim_name) or dim_name.startswith(merged_name):
                    sorted_merged_results.append(merged_result)
                    break

    for result in sorted_merged_results:
        if result.get("success", False) and result.get("questions"):
            questions = result["questions"].copy()  # 创建副本避免修改原数据
            original_count = len(questions)

            # 简化处理：只按题号排序，不进行复杂的过滤
            questions.sort(key=lambda x: x.get("sort_number", 0))
            print(f"[维度处理] 维度 '{result['dimension']}' 包含 {len(questions)} 个题目")

            if renumber_questions:
                # 为该维度的所有题目重新编号
                for question in questions:
                    original_number = question.get("sort_number")
                    question["sort_number"] = current_question_number
                    question["original_sort_number"] = original_number  # 保存原始题号
                    current_question_number += 1
                print(f"[重新编号] 维度 '{result['dimension']}' 的 {len(questions)} 个题目已重新编号")
            else:
                # 保持原始题号，只按原始题号排序
                questions.sort(key=lambda x: x.get("sort_number", 0))
                print(f"[保持原号] 维度 '{result['dimension']}' 的 {len(questions)} 个题目保持原始题号")

            dimension_questions = {
                "dimension": result["dimension"],
                "questions": questions
            }
            all_questions_by_dimension.append(dimension_questions)
            total_questions += len(questions)
            successful_dimensions += 1
        else:
            print(f"[警告] 维度 '{result.get('dimension')}' 处理失败或无题目")

    print(f"[并行处理] 完成统计: {successful_dimensions}/{len(dimensions)} 个维度成功, 总题目数: {total_questions}")

    # 多模型分配统计（使用之前计算的service_stats）
    if MULTI_MODEL_CONFIG["enabled"]:
        print(f"[多模型统计] 最终服务使用分布: {service_stats}")

    if renumber_questions:
        print(f"[重新编号] 题目编号已从 1 到 {current_question_number - 1} 连续排列")
    else:
        print(f"[保持原号] 题目保持原始编号，按维度顺序排列")

    return {
        "success": True,
        "question_data": all_questions_by_dimension,
        "total_questions": total_questions
    }

# ============================ 规则提取 ============================

def extract_rules_from_content(
    title: str,
    content_or_file: str,
    api_key: str = None,
    prompt_file: str = PROMPT_FILES["rule"],
    output_dir: Path = RESULT_DIR,
    base_url: str = None,
    model: str = None
) -> Dict[str, Any]:
    """从内容中提取规则"""
    # 获取 API 配置
    if not api_key or not base_url or not model:
        config = get_api_config("extract_rules")
        api_key = api_key or config["api_key"]
        base_url = base_url or config["base_url"]
        model = model or config["model"]

    content = ""
    potential_path = Path(content_or_file)

    if potential_path.exists():
        try:
            with open(potential_path, "r", encoding="utf-8") as f:
                if potential_path.suffix.lower() == ".json":
                    data = json.load(f)
                    content = data.get("content", "").strip()
                else:
                    content = f.read().strip()
            if not content:
                return {"title": title, "error": "内容为空", "vetos_data": []}
        except Exception as e:
            return {"title": title, "error": f"读取文件失败: {e}", "vetos_data": []}
    else:
        content = str(content_or_file).strip()

    if not content:
        return {"title": title, "error": "内容为空", "vetos_data": []}

    try:
        with open(prompt_file, "r", encoding="utf-8") as f:
            template = f.read()
    except Exception as e:
        return {"title": title, "error": f"无法读取提示词: {e}", "vetos_data": []}

    prompt = template.replace("{text}", content)

    # 获取 Speculative Decoding 配置
    spec_config = get_speculative_config("extract_rules")
    runner = AITaskRunner(
        api_key=api_key,
        base_url=base_url,
        model=model,
        enable_speculative=False,  # 强制禁用Speculative
        speculative_times=1
    )
    raw_response = runner.run_task(prompt, response_format="json")

    if isinstance(raw_response, str):
        cleaned = re.sub(r"```(?:json)?\n(.*?)```", r"\1", raw_response, flags=re.S).strip()
        cleaned = cleaned.replace("<result>", "").replace("</result>", "").strip()
        try:
            result = json.loads(cleaned)
        except Exception:
            result = {"raw_response": raw_response}
    else:
        result = raw_response

    vetos_data = result.get("vetos_data", []) if isinstance(result, dict) else []

    output_dir.mkdir(parents=True, exist_ok=True)
    safe_title = create_safe_filename(title)
    output_file = output_dir / f"{safe_title}_rules.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"[记录] 生成的规则 JSON 已保存: {output_file}")
    return {"title": title, "file": str(output_file), "result": result, "vetos_data": vetos_data}

# ============================ 主流程控制 ============================

def process_pipeline_dimension_based(
    title: str = DEFAULT_EXECUTION_CONFIG["title"],
    api_key: str = None,
    zhipu_api_key: str = None,
    deepseek_api_key: str = None,
    index_prompt_file: str = PROMPT_FILES["index"],
    question_prompt_file: str = PROMPT_FILES["question"],
    rule_prompt_file: str = PROMPT_FILES["rule"],
    mongo_uri: str = None,
    db_name: str = MONGODB_CONFIG["db_name"],
    collection_name: str = MONGODB_CONFIG["collection_name"],
    target_batch_id: Optional[int] = DEFAULT_EXECUTION_CONFIG["target_batch_id"],
    renumber_questions: bool = True,  # 新增：是否重新编号题目（从1开始连续排列）
    enable_content_filtering: bool = True,  # 新增：是否启用内容过滤（检测异常跳跃等）
    fast_mode: bool = True  # 新增：快速模式，优化性能参数
) -> Optional[Dict[str, Any]]:
    """
    执行基于维度分块的完整生成流程
    """
    print(f"\n{'='*60}\n开始处理标题: {title} (基于维度分块)\n{'='*60}")

    # 快速模式配置
    if fast_mode:
        print("[快速模式] 已启用性能优化")
        print(f"[快速模式] 并行度: {TASK_CONFIG['max_workers']} workers")
        print(f"[快速模式] 重试次数: {TASK_CONFIG['retry_count']}")
        print(f"[快速模式] 最大token: {TASK_CONFIG['max_tokens']}")

    # 显示 Speculative Decoding 状态
    log_speculative_status()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir_merged = MERGED_DIR / f"run_dimension_{timestamp}"
    run_dir_result = RESULT_DIR / f"run_dimension_{timestamp}"
    run_dir_merged.mkdir(parents=True, exist_ok=True)
    run_dir_result.mkdir(parents=True, exist_ok=True)

    # 1. 提取 MongoDB 分块
    print("\n[1/5] 正在从 MongoDB 提取分块...")
    chunks_file_data = extract_chunks_to_file(target_batch_id=target_batch_id)
    if not chunks_file_data:
        return None
    chunks_file = chunks_file_data["chunks_file"]
    chunks = chunks_file_data["chunks"]
    print(f" 提取完成: {chunks_file}")

    # 2. 查找标题、合并内容并识别维度
    print(f"\n[2/5] 正在查找标题 '{title}' 并识别维度...")
    index_result = find_content_index_with_dimensions(
        title=title,
        chunks_file=chunks_file,
        prompt_file=index_prompt_file,
        output_dir=run_dir_merged
    )
    if not index_result:
        return None

    dimensions = index_result.get("dimensions", [])
    merged_file_path = index_result.get("merged_content_file")

    print(f" 识别到 {len(dimensions)} 个维度")
    for dim in dimensions:
        print(f"   - {dim['name']}: 索引 {dim['start_index']}-{dim['end_index']}")

    # 3&4. 并发执行：提取规则 + 按维度生成结构化题目
    print(f"\n[3&4/5] 正在并发执行：提取规则 + 按维度生成题目...")

    def run_extract_rules():
        """并发执行：提取规则"""
        print(f"[并发任务1] 开始提取规则...")
        return extract_rules_from_content(
            title=title,
            content_or_file=merged_file_path,
            prompt_file=rule_prompt_file,
            output_dir=run_dir_result
        )

    def run_generate_questions():
        """并发执行：按维度生成结构化题目"""
        print(f"[并发任务2] 开始按维度生成题目...")
        print(f"[配置] 题目重新编号: {'启用' if renumber_questions else '禁用'}")
        print(f"[配置] 内容过滤: {'启用' if enable_content_filtering else '禁用'}")
        return generate_questions_by_dimensions_parallel(
            dimensions=dimensions,
            chunks=chunks,
            prompt_file=question_prompt_file,
            output_dir=run_dir_result,
            renumber_questions=renumber_questions
        )

    # 并发执行两个任务
    with ThreadPoolExecutor(max_workers=DEFAULT_EXECUTION_CONFIG["concurrent_tasks"]) as executor:
        rule_future = executor.submit(run_extract_rules)
        question_future = executor.submit(run_generate_questions)

        rule_result = rule_future.result()
        question_result = question_future.result()

        print(f"并发任务完成：规则提取和按维度题目生成都已完成")

    # 5. 保存最终结果 - 只保留用户需要的两个字段
    unified_result = {
        "question_data": question_result.get("question_data", []),
        "vetos_data": rule_result.get("vetos_data", [])
    }

    # 保存最终结果文件
    safe_title = create_safe_filename(title)
    final_result_file = run_dir_result / f"{safe_title}_final.json"
    with open(final_result_file, "w", encoding="utf-8") as f:
        json.dump(unified_result, f, ensure_ascii=False, indent=2)
    log_saved_file(final_result_file)

    print(f"\n[完成] 基于维度分块的流程完成！")
    print(f"处理了 {len(dimensions)} 个维度，生成 {question_result.get('total_questions', 0)} 个题目")
    print(f"结果文件: {final_result_file}")
    return unified_result

# ============================ 主函数 ============================

def main() -> None:
    """主函数"""
    start_time = time.time()

    print("="*80)
    print("基于维度分块的题目生成系统")
    print("="*80)

    # 使用配置文件中的默认值，如需修改可在这里覆盖
    result = process_pipeline_dimension_based()

    end_time = time.time()
    elapsed_seconds = end_time - start_time
    elapsed_str = f"{int(elapsed_seconds // 3600)}小时 {int((elapsed_seconds % 3600) // 60)}分钟 {int(elapsed_seconds % 60)}秒"

    if result:
        question_count = len([q for dim in result.get("question_data", []) for q in dim.get("questions", [])])
        vetos_count = len(result.get("vetos_data", []))
        print(f"\n{'='*60}")
        print("处理完成:")
        print(f"生成题目数: {question_count}")
        print(f"规则数量: {vetos_count}")
        print(f"处理时间: {elapsed_str}")
        print(f"{'='*60}")
    else:
        print("处理失败")

    print(f"\n程序总运行时间: {elapsed_str}")

if __name__ == "__main__":
    main()