# -*- coding: utf-8 -*-
import sys
import os
import gc
import traceback
import time
import multiprocessing
import subprocess
import importlib.util
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

def check_and_install_packages(packages):
    """检查并安装所需的Python包"""
    for package, import_name in packages.items():
        if importlib.util.find_spec(import_name) is None:
            print(f"正在安装缺失的库: {package}...")
            try:
                subprocess.check_call([sys.executable, "-m", "pip", "install", package],
                                      stdout=subprocess.DEVNULL,
                                      stderr=subprocess.DEVNULL)
                print(f"库 {package} 安装成功。")
            except subprocess.CalledProcessError as e:
                print(f"错误: 无法自动安装库 {package}。请手动运行 'pip install {package}'。")
                print(f"详细错误: {e}")
                sys.exit(1)

# --- 依赖检查和自动安装 ---
required_packages = {"pandas": "pandas", "openpyxl": "openpyxl", "python-evtx": "Evtx", "lxml": "lxml"}
check_and_install_packages(required_packages)

# --- 成功安装依赖后，再导入 ---
import pandas as pd
from lxml import etree as ET
from Evtx.Evtx import Evtx
from Evtx.Views import evtx_file_xml_view

NS_MAP = {'e': 'http://schemas.microsoft.com/win/2004/08/events/event'}
LOGON_TYPE_DESC = {
    '2': 'Interactive (本地登录)', '3': 'Network (网络登录)', '4': 'Batch (批处理)', '5': 'Service (服务)',
    '7': 'Unlock (解锁)', '8': 'NetworkCleartext (网络明文)', '9': 'NewCredentials (新凭据)',
    '10': 'RemoteInteractive (远程桌面)', '11': 'CachedInteractive (缓存交互式)'
}

def process_chunk(chunk_of_xmls):
    """(多线程)处理EVTX数据块，输入为XML字符串列表"""
    chunk_records = []
    for xml in chunk_of_xmls:
        try:
            root = ET.fromstring(xml.encode('utf-8'))
            system_node = root.find('./e:System', namespaces=NS_MAP)
            if system_node is None: continue

            event_id_element = system_node.find('./e:EventID', namespaces=NS_MAP)
            event_id = event_id_element.text if event_id_element is not None else None

            if event_id not in ["4624", "4625"]:
                continue

            computer_element = system_node.find('./e:Computer', namespaces=NS_MAP)
            destination_computer = computer_element.text if computer_element is not None else 'N/A'
            
            timestamp_element = system_node.find('./e:TimeCreated', namespaces=NS_MAP)
            timestamp = timestamp_element.attrib.get('SystemTime') if timestamp_element is not None else None

            data_elements = {data.attrib['Name']: data.text for data in root.findall('.//e:Data', namespaces=NS_MAP) if 'Name' in data.attrib}

            chunk_records.append({
                'EventID': event_id,
                'Timestamp': timestamp,
                'Destination_Computer': destination_computer,
                'UserName': data_elements.get('TargetUserName', 'N/A'),
                'Domain': data_elements.get('TargetDomainName', 'N/A'),
                'IpAddress': data_elements.get('IpAddress', '-'),
                'Source_Computer': data_elements.get('WorkstationName', '-'),
                'LogonType': data_elements.get('LogonType')
            })
        except ET.XMLSyntaxError:
            continue
        except Exception:
            continue
    return chunk_records

def divide_evtx_into_chunks(evtx_path, num_chunks=None):
    """将EVTX文件分割成多个XML字符串块"""
    if num_chunks is None:
        num_chunks = max(1, multiprocessing.cpu_count() - 1)
    
    chunks = [[] for _ in range(num_chunks)]
    chunk_idx = 0
    count = 0
    
    print("正在准备数据块以便并行处理...")
    with Evtx(evtx_path) as log:
        for xml, record in evtx_file_xml_view(log):
            chunks[chunk_idx].append(xml)
            chunk_idx = (chunk_idx + 1) % num_chunks
            count += 1
            if count % 20000 == 0:
                print(f"  已读取 {count} 条记录...")
    print(f"数据块准备完成，共 {count} 条记录被分为 {len(chunks)} 块。")
    return chunks

def save_final_report(all_records_df, summary_dfs, output_path):
    """将所有数据和分析总结保存到同一个Excel文件的不同sheet中"""
    print(f"开始将结果写入Excel文件: {output_path}")
    try:
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            all_records_df.to_excel(writer, index=False, sheet_name='All_Login_Events')
            print("  - 已写入 'All_Login_Events' sheet.")
            
            for sheet_name, df in summary_dfs.items():
                if sheet_name == 'User_Login_Path_Summary':
                    df.columns = ['目标计算机', '用户名', '域', '源IP地址', '源计算机名', '成功登录次数', '失败登录次数']
                df.to_excel(writer, index=False, sheet_name=sheet_name)
                print(f"  - 已写入 '{sheet_name}' sheet.")
        print("Excel报告生成成功。")
        return True
    except Exception as e:
        print(f"保存最终Excel报告时出错: {e}")
        traceback.print_exc()
        return False

def generate_summary_and_analysis(df_records):
    """对登录日志进行总结和分析, 生成全新的详细文本报告"""
    if df_records.empty:
        return {}, "未找到任何登录事件，无法生成分析报告。"

    print("开始对提取的登录事件进行分析和总结...")
    df_records['Timestamp'] = pd.to_datetime(df_records['Timestamp'], utc=True, errors='coerce').dt.tz_localize(None)
    df_records['EventID'] = df_records['EventID'].astype(str)
    
    path_summary = df_records.groupby(
        ['Destination_Computer', 'UserName', 'Domain', 'IpAddress', 'Source_Computer']
    ).agg(
        Successful_Logins=('EventID', lambda x: (x == '4624').sum()),
        Failed_Logins=('EventID', lambda x: (x == '4625').sum())
    ).reset_index()
    path_summary = path_summary[(path_summary['Successful_Logins'] > 0) | (path_summary['Failed_Logins'] > 0)]
    path_summary = path_summary.sort_values(by=['UserName', 'Successful_Logins', 'Failed_Logins'], ascending=[True, False, False])

    user_summary = df_records.groupby(['UserName', 'Domain']).agg(
        Successful_Logins=('EventID', lambda x: (x == '4624').sum()),
        Failed_Logins=('EventID', lambda x: (x == '4625').sum())
    ).reset_index()
    user_summary['Total_Attempts'] = user_summary['Successful_Logins'] + user_summary['Failed_Logins']
    user_summary = user_summary.sort_values(by='Total_Attempts', ascending=False)

    ip_summary = df_records[df_records['IpAddress'].notna() & (df_records['IpAddress'] != '-')].groupby('IpAddress').agg(
        Successful_Logins=('EventID', lambda x: (x == '4624').sum()),
        Failed_Logins=('EventID', lambda x: (x == '4625').sum()),
        Affected_Users=('UserName', lambda x: ', '.join(x.unique()))
    ).reset_index()
    ip_summary['Total_Attempts'] = ip_summary['Successful_Logins'] + ip_summary['Failed_Logins']
    ip_summary = ip_summary.sort_values(by='Total_Attempts', ascending=False)
    
    df_records['LogonType_Desc'] = df_records['LogonType'].map(LOGON_TYPE_DESC).fillna('Unknown')
    logon_type_summary = df_records.groupby(['LogonType', 'LogonType_Desc']).agg(
        Count=('EventID', 'count')
    ).reset_index().sort_values(by='Count', ascending=False)
    
    # --- 全新的文本报告生成逻辑 ---
    summary_text = []
    summary_text.append("=" * 60)
    summary_text.append("          Windows 登录日志审计分析报告")
    summary_text.append("=" * 60)
    summary_text.append("\n--- 1. 总体情况概览 ---")
    summary_text.append(f"  - 分析时间范围: {df_records['Timestamp'].min(skipna=True).strftime('%Y-%m-%d %H:%M:%S')} to {df_records['Timestamp'].max(skipna=True).strftime('%Y-%m-%d %H:%M:%S')}")
    summary_text.append(f"  - 总共分析登录事件数: {len(df_records)} (成功: {(df_records['EventID'] == '4624').sum()}, 失败: {(df_records['EventID'] == '4625').sum()})")
    summary_text.append(f"  - 涉及独立用户账户数 (含机器账户): {df_records['UserName'].nunique()}")
    summary_text.append(f"  - 涉及源IP数: {df_records[df_records['IpAddress'] != '-']['IpAddress'].nunique()}")
    summary_text.append(f"  - 涉及目标计算机数: {df_records['Destination_Computer'].nunique()}")
    
    summary_text.append("\n\n--- 2. 详细用户登录活动报告 (已排除机器账户) ---")
    
    # 过滤掉机器账户 (以 $ 结尾)
    human_user_paths = path_summary[~path_summary['UserName'].str.endswith('$', na=False)]

    if human_user_paths.empty:
        summary_text.append("\n未发现任何人类用户的登录活动。")
    else:
        # 按用户名和域分组，为每个用户生成一份详细报告
        for (username, domain), user_activities in human_user_paths.groupby(['UserName', 'Domain']):
            summary_text.append("\n" + "=" * 60)
            summary_text.append(f"用户: {username}  (域: {domain})")
            summary_text.append("-" * 60)
            
            total_success = user_activities['Successful_Logins'].sum()
            total_failed = user_activities['Failed_Logins'].sum()
            summary_text.append(f"  [用户活动总计: 成功 {total_success} 次, 失败 {total_failed} 次]")

            # 在每个用户下，按目标计算机再次分组
            for dest_computer, dest_activities in user_activities.groupby('Destination_Computer'):
                summary_text.append(f"\n  -> 登录到目标计算机: {dest_computer}")
                
                # 遍历该用户到该目标计算机的所有登录路径
                for _, row in dest_activities.iterrows():
                    source_ip = row['IpAddress']
                    source_computer = row['Source_Computer']
                    success_count = row['Successful_Logins']
                    failed_count = row['Failed_Logins']
                    
                    report_line = f"    - 从 [源IP: {source_ip:<15} | 源计算机: {source_computer}]"
                    report_line += f"  >>  成功: {success_count} 次, 失败: {failed_count} 次"
                    summary_text.append(report_line)

    summary_text.append("\n\n" + "=" * 60)
    summary_text.append("报告结束。详细聚合数据请查看Excel文件。")
    
    print("分析总结完成。")

    summary_dfs = {
        'User_Login_Path_Summary': path_summary,
        'User_Summary': user_summary,
        'IP_Summary': ip_summary,
        'LogonType_Summary': logon_type_summary
    }
    return summary_dfs, "\n".join(summary_text)


def parse_evtx_to_excel(evtx_path, output_excel_path, batch_size=10000, parallel=True):
    records = []
    start_time = time.time()
    
    print(f"正在打开EVTX文件: {evtx_path}")
    
    if parallel:
        print("模式: 并行处理。")
        try:
            cpu_cores = max(1, multiprocessing.cpu_count() - 1)
            print(f"将使用 {cpu_cores} 个CPU核心进行处理。")
            
            chunks = divide_evtx_into_chunks(evtx_path, cpu_cores)
            total_chunks = len(chunks)
            processed_chunks = 0
            
            with ProcessPoolExecutor(max_workers=cpu_cores) as executor:
                futures = {executor.submit(process_chunk, chunk): i for i, chunk in enumerate(chunks)}
                for future in as_completed(futures):
                    processed_chunks += 1
                    try:
                        chunk_records = future.result()
                        records.extend(chunk_records)
                        progress = (processed_chunks / total_chunks) * 100
                        print(f"进度: {progress:.2f}% | 完成 {processed_chunks}/{total_chunks} 个数据块 | 当前匹配总数: {len(records)}")
                    except Exception as e:
                        print(f"处理一个数据块时发生意外错误: {e}")
            
        except Exception as e:
            print(f"并行处理过程中发生严重错误: {e}")
            traceback.print_exc()
            print("将自动切换到更稳定的顺序处理模式...")
            return parse_evtx_to_excel(evtx_path, output_excel_path, batch_size, parallel=False)
    
    if not parallel:
        print("模式: 顺序处理。")
        try:
            with Evtx(evtx_path) as log:
                total_event_count = 0
                for xml, record in evtx_file_xml_view(log):
                    total_event_count += 1
                    if total_event_count % 10000 == 0:
                        print(f"进度: 已处理 {total_event_count} 事件 | 当前匹配总数: {len(records)}")
                    
                    try:
                        root = ET.fromstring(xml.encode('utf-8'))
                        system_node = root.find('./e:System', namespaces=NS_MAP)
                        if system_node is None: continue

                        event_id_element = system_node.find('./e:EventID', namespaces=NS_MAP)
                        event_id = event_id_element.text if event_id_element is not None else None

                        if event_id not in ["4624", "4625"]: continue

                        computer_element = system_node.find('./e:Computer', namespaces=NS_MAP)
                        destination_computer = computer_element.text if computer_element is not None else 'N/A'

                        timestamp_element = system_node.find('./e:TimeCreated', namespaces=NS_MAP)
                        timestamp = timestamp_element.attrib.get('SystemTime') if timestamp_element is not None else None

                        data_elements = {data.attrib['Name']: data.text for data in root.findall('.//e:Data', namespaces=NS_MAP) if 'Name' in data.attrib}

                        records.append({
                            'EventID': event_id, 'Timestamp': timestamp, 'Destination_Computer': destination_computer,
                            'UserName': data_elements.get('TargetUserName', 'N/A'), 'Domain': data_elements.get('TargetDomainName', 'N/A'),
                            'IpAddress': data_elements.get('IpAddress', '-'), 'Source_Computer': data_elements.get('WorkstationName', '-'),
                            'LogonType': data_elements.get('LogonType')
                        })
                    except ET.XMLSyntaxError:
                        continue
        except Exception as e:
            print(f"顺序处理EVTX文件时发生严重错误: {e}")
            traceback.print_exc()
    
    if records:
        print(f"\n处理完成，共匹配到 {len(records)} 条登录事件。")
        df_records = pd.DataFrame(records)
        summary_dfs, summary_text = generate_summary_and_analysis(df_records)
        
        if save_final_report(df_records, summary_dfs, output_excel_path):
            summary_txt_path = os.path.splitext(output_excel_path)[0] + "_summary.txt"
            try:
                with open(summary_txt_path, 'w', encoding='utf-8') as f:
                    f.write(summary_text)
                print(f"分析总结报告已保存到: {summary_txt_path}")
            except Exception as e:
                print(f"保存文本总结报告时出错: {e}")
    else:
        print("未在日志文件中找到任何匹配的登录事件(ID 4624 或 4625)。")

    print(f"\n总处理时间: {time.time() - start_time:.2f} 秒")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("\n用法: python evtx2xlsx.py <输入文件.evtx> <输出文件.xlsx> [--sequential]")
        sys.exit(1)

    evtx_path = sys.argv[1]
    output_excel_path = sys.argv[2]
    use_parallel = "--sequential" not in sys.argv
    
    # 删除可能存在的旧的、格式不兼容的临时文件
    temp_file = output_excel_path + ".temp.xlsx"
    if os.path.exists(temp_file):
        print(f"警告: 发现旧的临时文件 {temp_file}，将予以删除以保证数据一致性。")
        try:
            os.remove(temp_file)
        except OSError as e:
            print(f"删除临时文件失败: {e}，请手动删除后重试。")
            sys.exit(1)
            
    print("="*50 + f"\n任务启动: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    parse_evtx_to_excel(evtx_path, output_excel_path, parallel=use_parallel)
    print("\n" + "="*50 + f"\n任务结束: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n" + "="*50)
