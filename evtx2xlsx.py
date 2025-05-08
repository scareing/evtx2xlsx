import pandas as pd
import sys
import os
import gc
import traceback
import time
import multiprocessing
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
from Evtx.Evtx import Evtx
from Evtx.Views import evtx_file_xml_view
import xml.etree.ElementTree as ET

def process_chunk(chunk_data):
    """处理EVTX数据块，用于并行处理"""
    chunk_records = []
    matched_count = 0
    
    for xml, _ in chunk_data:
        try:
            root = ET.fromstring(xml)
            event_id_element = root.find('.//{http://schemas.microsoft.com/win/2004/08/events/event}EventID')
            event_id = event_id_element.text if event_id_element is not None else None

            # 仅筛选登录事件
            if event_id not in ["4624", "4625"]:
                continue

            matched_count += 1

            timestamp_element = root.find('.//{http://schemas.microsoft.com/win/2004/08/events/event}TimeCreated')
            timestamp = timestamp_element.attrib['SystemTime'] if timestamp_element is not None else None

            # 提取数据元素 - 使用字典优化查找
            data_elements = {}
            for data in root.findall('.//{http://schemas.microsoft.com/win/2004/08/events/event}Data'):
                if 'Name' in data.attrib:
                    data_elements[data.attrib['Name']] = data.text

            chunk_records.append({
                'EventID': event_id,
                'Timestamp': timestamp,
                'UserName': data_elements.get('TargetUserName'),
                'Domain': data_elements.get('TargetDomainName'),
                'IpAddress': data_elements.get('IpAddress'),
                'LogonType': data_elements.get('LogonType')
            })
        except Exception as e:
            # 简单记录错误但继续处理
            pass
            
    return chunk_records, matched_count

def divide_evtx_into_chunks(evtx_path, num_chunks=None):
    """将EVTX文件分割成多个块以便并行处理"""
    if num_chunks is None:
        num_chunks = max(1, multiprocessing.cpu_count() - 1)  # 留一个核心给系统
    
    chunks = [[] for _ in range(num_chunks)]
    chunk_idx = 0
    count = 0
    
    with Evtx(evtx_path) as log:
        for item in evtx_file_xml_view(log):
            chunks[chunk_idx].append(item)
            chunk_idx = (chunk_idx + 1) % num_chunks
            count += 1
            
            # 每处理1000条记录输出一次进度
            if count % 1000 == 0:
                print(f"正在准备数据块: 已读取 {count} 条记录...")
    
    return chunks

def save_data_to_excel(records, output_path):
    """保存数据到Excel文件，优化写入性能"""
    try:
        # 使用更高效的写入方式
        writer = pd.ExcelWriter(output_path, engine='openpyxl')
        df = pd.DataFrame(records)
        df.to_excel(writer, index=False, sheet_name='Login_Events')
        writer.close()
        return True
    except Exception as e:
        print(f"保存数据时出错: {e}")
        traceback.print_exc()
        return False

def parse_evtx_to_excel(evtx_path, output_excel_path, batch_size=5000, parallel=True):
    """
    将EVTX文件解析为Excel文件，支持并行处理和断点续传
    
    Args:
        evtx_path: EVTX文件路径
        output_excel_path: 输出Excel文件路径
        batch_size: 每次保存的记录数量
        parallel: 是否启用并行处理
    """
    records = []
    event_count = 0
    matched_events = 0
    last_save_time = time.time()
    start_time = time.time()
    temp_file = output_excel_path + ".temp.xlsx"
    
    # 检查是否有临时文件，有则加载已处理数据
    if os.path.exists(temp_file):
        try:
            print(f"找到临时文件: {temp_file}，加载已处理数据...")
            existing_data = pd.read_excel(temp_file)
            records = existing_data.to_dict('records')
            matched_events = len(records)
            print(f"已加载 {matched_events} 条记录，继续处理...")
        except Exception as e:
            print(f"加载临时文件失败: {e}")
            print("将创建新的数据文件...")

    print(f"打开EVTX文件: {evtx_path}")
    
    # 选择处理模式: 并行或顺序
    if parallel:
        print("使用并行处理模式...")
        try:
            # 获取可用CPU核心数
            cpu_cores = max(1, multiprocessing.cpu_count() - 1)  # 保留一个核心给系统
            print(f"将使用 {cpu_cores} 个CPU核心进行处理")
            
            # 分割数据
            print("分割数据为多个块...")
            chunks = divide_evtx_into_chunks(evtx_path, cpu_cores)
            total_chunks = len(chunks)
            print(f"数据已分割为 {total_chunks} 个块")
            
            # 并行处理各个块
            print("开始并行处理...")
            with ProcessPoolExecutor(max_workers=cpu_cores) as executor:
                futures = {executor.submit(process_chunk, chunk): i for i, chunk in enumerate(chunks)}
                
                completed = 0
                for future in as_completed(futures):
                    chunk_idx = futures[future]
                    try:
                        chunk_records, chunk_matched = future.result()
                        records.extend(chunk_records)
                        matched_events += chunk_matched
                        event_count += len(chunks[chunk_idx])
                        
                        completed += 1
                        progress = (completed / total_chunks) * 100
                        print(f"进度: {progress:.2f}% | 已完成 {completed}/{total_chunks} 个数据块 | 匹配: {matched_events} 登录事件")
                        
                        # 定期保存
                        if matched_events % batch_size == 0 or time.time() - last_save_time > 300:
                            print(f"临时保存数据到 {temp_file}...")
                            save_data_to_excel(records, temp_file)
                            last_save_time = time.time()
                            
                            # 释放内存
                            gc.collect()
                    
                    except Exception as e:
                        print(f"处理数据块 {chunk_idx} 时出错: {e}")
                        traceback.print_exc()
        
        except Exception as e:
            print(f"并行处理出错: {e}")
            traceback.print_exc()
            print("切换到顺序处理模式...")
            
            # 如果并行处理失败，回退到顺序处理
            return parse_evtx_to_excel(evtx_path, output_excel_path, batch_size, parallel=False)
    
    else:
        # 顺序处理模式
        print("使用顺序处理模式...")
        try:
            # 优化XML解析速度的缓存
            ns_map = {
                'e': 'http://schemas.microsoft.com/win/2004/08/events/event'
            }
            
            with Evtx(evtx_path) as log:
                # 获取文件总大小用于进度显示
                total_size = os.path.getsize(evtx_path)
                
                for xml, record in evtx_file_xml_view(log):
                    try:
                        event_count += 1
                        
                        # 更新进度显示
                        if event_count % 500 == 0:
                            try:
                                # 调用offset()方法获取当前位置
                                current_position = record.offset() if callable(record.offset) else record.offset
                            except:
                                # 如果获取失败，使用估算值
                                current_position = (event_count / (event_count + 1)) * total_size
                                
                            elapsed = time.time() - start_time
                            progress = (current_position / total_size) * 100 if total_size > 0 else 0
                            events_per_sec = event_count / elapsed if elapsed > 0 else 0
                            
                            print(f"进度: {progress:.2f}% | "
                                f"已处理: {event_count} 事件 | 匹配: {matched_events} 登录事件 | "
                                f"速度: {events_per_sec:.2f} 事件/秒")
                        
                        # 使用缓存的命名空间优化查找速度
                        root = ET.fromstring(xml)
                        event_id_element = root.find('.//e:EventID', ns_map)
                        event_id = event_id_element.text if event_id_element is not None else None

                        # 仅筛选登录事件
                        if event_id not in ["4624", "4625"]:
                            continue

                        matched_events += 1

                        timestamp_element = root.find('.//e:TimeCreated', ns_map)
                        timestamp = timestamp_element.attrib['SystemTime'] if timestamp_element is not None else None

                        # 提取数据元素 - 只查找一次所有Data元素并存入字典
                        data_elements = {}
                        for data in root.findall('.//e:Data', ns_map):
                            if 'Name' in data.attrib:
                                data_elements[data.attrib['Name']] = data.text

                        records.append({
                            'EventID': event_id,
                            'Timestamp': timestamp,
                            'UserName': data_elements.get('TargetUserName'),
                            'Domain': data_elements.get('TargetDomainName'),
                            'IpAddress': data_elements.get('IpAddress'),
                            'LogonType': data_elements.get('LogonType')
                        })

                        # 定期保存数据到临时文件
                        current_time = time.time()
                        if (matched_events % batch_size == 0) or (current_time - last_save_time > 300):  # 每batch_size条或5分钟保存一次
                            save_data_to_excel(records, temp_file)
                            last_save_time = current_time
                            print(f"临时保存数据到 {temp_file}，已处理 {matched_events} 条登录事件")
                            
                            # 释放内存
                            gc.collect()
                    
                    except Exception as e:
                        print(f"处理事件时出错: {e}")
                        if event_count % 1000 == 0:  # 避免过多错误信息
                            traceback.print_exc()
                        print("继续处理下一条事件...")
                        continue
        
        except Exception as e:
            print(f"处理EVTX文件时出错: {e}")
            traceback.print_exc()
    
    # 最终保存所有数据
    if records:
        print(f"完成处理，匹配到 {matched_events} 条登录事件")
        print(f"保存数据到 {output_excel_path}...")
        
        try:
            save_data_to_excel(records, output_excel_path)
            print(f"数据已保存到 {output_excel_path}")
            
            # 处理成功后删除临时文件
            if os.path.exists(temp_file):
                os.remove(temp_file)
                print(f"已删除临时文件 {temp_file}")
        except Exception as e:
            print(f"保存最终数据时出错: {e}")
            traceback.print_exc()
            print(f"请检查临时文件 {temp_file} 是否包含已处理数据")
    else:
        print("未找到匹配的登录事件")

    # 显示处理时间和性能统计
    total_time = time.time() - start_time
    print(f"总处理时间: {total_time:.2f} 秒")
    if event_count > 0:
        print(f"平均处理速度: {event_count/total_time:.2f} 事件/秒")
    if matched_events > 0:
        print(f"匹配事件比例: {(matched_events/event_count)*100:.2f}%")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("用法: python3 evtx2xlsx.py 输入文件.evtx 输出文件.xlsx [--sequential]")
        print("选项:")
        print("  --sequential  禁用并行处理，使用单线程顺序处理模式")
        sys.exit(1)

    evtx_path = sys.argv[1]
    output_excel_path = sys.argv[2]
    
    # 检查是否使用顺序处理模式
    use_parallel = "--sequential" not in sys.argv
    
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"处理模式: {'并行' if use_parallel else '顺序'}")
    
    parse_evtx_to_excel(evtx_path, output_excel_path, parallel=use_parallel)
    
    print(f"结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
