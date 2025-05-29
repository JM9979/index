import asyncio
import time
import logging

from app.dependencies import syclic_call_rpc
from app.dependencies import DBManager
from app.db.nft_collections import process_nft_collections
from app.db.nft_utxo_set import process_nft_utxo_set
from app.db.ft import process_ft_txo_set, process_ft_balance
from app.db.ft import process_ft_inputs, process_spent_ft_balances
from app.db.ft import process_ft_tokens
from app.db.transaction import process_transaction_record

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 定义并初始化全局变量
index_height = 862600
index_interval = 0
mempool = []
last_mempool = []

def schedule_task(task):
    """
    Schedule task.
    """
    async def wrapper():
        await DBManager.init_pool(db="TBC20721")

        # clear db
        clear_db_query = """
        SET FOREIGN_KEY_CHECKS = 0;
        TRUNCATE TABLE `ft_tokens`;
        TRUNCATE TABLE `ft_balance`;
        TRUNCATE TABLE `ft_txo_set`;
        TRUNCATE TABLE `nft_collections`;
        TRUNCATE TABLE `nft_utxo_set`;
        TRUNCATE TABLE `transactions`;
        TRUNCATE TABLE `address_transactions`;
        TRUNCATE TABLE `transaction_participants`;
        SET FOREIGN_KEY_CHECKS = 1;
        """
        await DBManager.execute_update(clear_db_query)

        while True:
            try:
                global index_interval
                await task()
                await asyncio.sleep(index_interval)
            except KeyboardInterrupt:
                logging.info("Interrupted by user")
                break
            
        await DBManager.close_pool()
    asyncio.run(wrapper())





async def analyze_transaction_data(decode_tx):
    """
    全面分析交易数据，返回交易类型和UTXO类型信息
    
    Args:
        decode_tx: 解码后的交易数据
        
    Returns:
        dict: 包含交易分析结果的字典
    """
    result = {
        'tx_type': 'P2PKH',  # 默认类型
        'utxo_types': []     # 每个输出的类型
    }
    
    # 分析每个输出，确定交易类型和每个UTXO的类型
    for i, output in enumerate(decode_tx['vout']):
        if 'scriptPubKey' not in output or 'asm' not in output['scriptPubKey']:
            result['utxo_types'].append('UNKNOWN')
            continue
            
        script_asm = output['scriptPubKey']['asm']
        utxo_type = 'NORMAL'
        
        # 判断UTXO类型
        if script_asm.startswith("9 OP_PICK OP_TOALTSTACK"):
            utxo_type = 'FT'
            if result['tx_type'] == 'P2PKH':  # 只有当前类型是默认值时才更新
                result['tx_type'] = 'TBC20'
        elif (script_asm.startswith("OP_RETURN") or 
              script_asm.startswith("0 OP_RETURN")):
            utxo_type = 'NFT_COLLECTION'
            if result['tx_type'] == 'P2PKH':
                result['tx_type'] = 'TBC721'
        elif (script_asm.startswith("1 OP_PICK 3 OP_SPLIT") or 
              script_asm.startswith("4 OP_PICK OP_BIN2NUM OP_TOALTSTACK 1 OP_PICK 3 OP_SPLIT")):
            utxo_type = 'NFT'
            if result['tx_type'] == 'P2PKH':
                result['tx_type'] = 'TBC721'
        elif script_asm.endswith("OP_CHECKMULTISIG"):
            utxo_type = 'MULTISIG'
            if result['tx_type'] == 'P2PKH':
                result['tx_type'] = 'P2MS'
                
        result['utxo_types'].append(utxo_type)
    
    return result


async def process_single_transaction(tx, block_height, timestamp):
    success_flags = {"transaction_record": False, "utxos": False}
    
    try:
        decode_tx = await syclic_call_rpc(method="getrawtransaction", params=[tx, 1])
        tx_analysis = await analyze_transaction_data(decode_tx)
        
        # 尝试处理交易记录
        try:
            await process_transaction_record(decode_tx, block_height, timestamp, tx_analysis['tx_type'])
            success_flags["transaction_record"] = True
        except Exception as e:
            logging.error("处理交易记录失败 %s: %s", tx, str(e))
        
        # 无论交易记录是否成功，都尝试处理UTXO
        try:
            await process_tx_utxos(decode_tx, timestamp, tx_analysis['utxo_types'])
            success_flags["utxos"] = True
        except Exception as e:
            logging.error("处理UTXO失败 %s: %s", tx, str(e))
        
        # 只有两者都成功才算成功
        return success_flags["transaction_record"] and success_flags["utxos"]
        
    except Exception as e:
        logging.error("处理交易失败 %s: %s", tx, str(e))
        return False


def is_in_blacklist(txid):
    """检查交易ID是否在黑名单中"""
    try:
        with open('black_list.txt', 'r', encoding='utf-8') as file:
            black_list = [line.strip() for line in file]
        return txid in black_list
    except FileNotFoundError:
        return False



def determine_tx_type(decode_tx):
    """
    确定交易类型
    
    Args:
        decode_tx: 解码后的交易数据
        
    Returns:
        str: 交易类型，如 "P2PKH"、"TBC20"、"TBC721" 等
    """
    tx_type = "P2PKH"  # 默认类型
    
    for output in decode_tx['vout']:
        script_asm = output["scriptPubKey"]["asm"]
        if script_asm.startswith("9 OP_PICK OP_TOALTSTACK"):
            tx_type = "TBC20"
            break
        elif (script_asm.startswith("OP_RETURN") or 
              script_asm.startswith("0 OP_RETURN") or 
              script_asm.startswith("1 OP_PICK")):
            tx_type = "TBC721"
            break
        elif script_asm.endswith("OP_CHECKMULTISIG"):
            tx_type = "P2MS"
            break
            
    return tx_type



async def process_tx_utxos(decode_tx, timestamp, utxo_types=None):
    """
    处理交易中的各种UTXO（FT/NFT等）
    
    Args:
        decode_tx: 解码后的交易数据
        timestamp: 时间戳
        utxo_types: 每个输出的类型列表，如果为None则实时判断
    """
    output_index = 0
    
    # 第一阶段：处理所有输出
    while output_index < len(decode_tx["vout"]):
        if output_index >= len(decode_tx["vout"]):
            break
        
        # 如果提供了UTXO类型，则使用它
        if utxo_types and output_index < len(utxo_types):
            utxo_type = utxo_types[output_index]
        else:
            # 否则实时判断UTXO类型
            script_asm = decode_tx["vout"][output_index]["scriptPubKey"]["asm"]
            if script_asm.startswith("0 OP_RETURN") or script_asm.startswith("OP_RETURN"):
                utxo_type = 'NFT_COLLECTION'
            elif script_asm.startswith("1 OP_PICK 3 OP_SPLIT") or script_asm.startswith("4 OP_PICK OP_BIN2NUM OP_TOALTSTACK 1 OP_PICK 3 OP_SPLIT"):
                utxo_type = 'NFT'
            elif script_asm.startswith("9 OP_PICK OP_TOALTSTACK"):
                utxo_type = 'FT'
            else:
                utxo_type = 'NORMAL'
        
        # 根据UTXO类型处理
        if utxo_type == 'NFT_COLLECTION':
            new_output_index, _, should_break = await process_nft_collections(decode_tx, output_index, timestamp)
            if should_break:
                break
            output_index = new_output_index
        elif utxo_type == 'NFT':
            new_output_index, _, should_break = await process_nft_utxo_set(decode_tx, output_index, timestamp)
            if should_break:
                break
            output_index = new_output_index
        elif utxo_type == 'FT':
            new_output_index, ft_contract_id, vout_combine_script, ft_balance, should_break = await process_ft_tokens(decode_tx, output_index, timestamp)
            if should_break:
                break
            output_index = new_output_index
            
            # 处理FT代币的UTXO记录（仅输出）
            should_break = await process_ft_txo_set(decode_tx, output_index, ft_contract_id, vout_combine_script, ft_balance)
            if should_break:
                break
            
            # 处理FT代币余额（仅输出增加）
            if ft_contract_id is not None and vout_combine_script is not None and ft_balance is not None:
                should_break = await process_ft_balance(ft_contract_id, vout_combine_script, ft_balance)
                if should_break:
                    break
            else:
                logging.warning("Invalid FT data: contract_id=%s, script=%s, balance=%s", 
                               ft_contract_id, vout_combine_script, ft_balance)
        else:
            output_index += 1
    
    # 第二阶段：统一处理所有输入
    try:
        spent_utxo_info_list = await process_ft_inputs(decode_tx)
        if spent_utxo_info_list:
            await process_spent_ft_balances(spent_utxo_info_list)
    except Exception as e:
        logging.error("Error processing FT inputs for transaction %s: %s", decode_tx["txid"], e)


async def check_block_height():
    """
    检查当前区块高度并确定是否追上最新区块
    
    Returns:
        tuple: (if_catch_lastest, block_count_res)
    """
    global index_interval
    global index_height
    
    block_count_res = await syclic_call_rpc(method="getblockcount", params=[])
    if_catch_lastest = False
    
    if block_count_res < index_height:
        if_catch_lastest = True
        index_interval = 2
    
    logging.info("扫描区块链... 当前索引高度: %s, 区块链高度: %s, 是否追上最新: %s", 
                 index_height, block_count_res, if_catch_lastest)
    
    return if_catch_lastest, block_count_res


async def get_mempool_and_timestamp(if_catch_lastest):
    """
    获取当前内存池和时间戳
    
    Args:
        if_catch_lastest: 是否已追上最新区块
        
    Returns:
        tuple: (current_mempool, timestamp)
    """
    global index_height
    
    if if_catch_lastest:
        current_mempool = await syclic_call_rpc(method="getrawmempool", params=[])
        timestamp = int(time.time())
    else:
        get_block_res = await syclic_call_rpc(method="getblockbyheight", params=[index_height, 1])
        current_mempool = get_block_res["tx"]
        timestamp = get_block_res["time"]
    
    return current_mempool, timestamp


def find_transactions(current_mempool):
    """
    找出新的交易
    
    Args:
        current_mempool: 当前内存池
        
    Returns:
        list: 新交易列表
    """
    global mempool
    global last_mempool
    
    nearly_mempool = mempool + last_mempool
    new_txs = [tx for tx in current_mempool if tx not in nearly_mempool]
    
    old_txs = [tx for tx in nearly_mempool if tx not in current_mempool]
    return new_txs, old_txs


async def process_new_transactions(new_txs, block_height, timestamp):
    """处理新交易"""
    try:
        for tx in new_txs:
            try:
                mempool.append(tx)
                await process_single_transaction(tx, block_height, timestamp)
            except Exception as e:
                logging.error("处理新交易失败 %s: %s", tx, str(e))
                continue
    except Exception as e:
        logging.error("处理新交易组失败: %s", str(e))

async def process_old_transactions(old_txs, block_height, timestamp):
    """处理旧交易"""
    try:
        for tx in old_txs:
            try:
                decode_tx = await syclic_call_rpc(method="getrawtransaction", params=[tx, 1])
                tx_analysis = await analyze_transaction_data(decode_tx)
                await process_transaction_record(decode_tx, block_height, timestamp, tx_analysis['tx_type'])
            except Exception as e:
                logging.error("处理旧交易失败 %s: %s", tx, str(e))
                continue
    except Exception as e:
        logging.error("处理旧交易组失败: %s", str(e))

async def process_transactions(new_txs, old_txs, if_catch_lastest, timestamp):
    """
    并发处理新交易和旧交易
    
    Args:
        new_txs: 新交易列表
        old_txs: 旧交易列表
        if_catch_lastest: 是否已追上最新区块
        timestamp: 时间戳
    """
    for tx in new_txs:
        mempool.append(tx)
        block_height = index_height if not if_catch_lastest else -1
        await process_single_transaction(tx, block_height, timestamp)
    
    # block_height = index_height if not if_catch_lastest else -1
    
    # # 并发执行新交易和旧交易的处理
    # await asyncio.gather(
    #     process_new_transactions(new_txs, block_height, timestamp),
    #     process_old_transactions(old_txs, block_height, timestamp)
    # )

def update_mempool_state(if_catch_lastest):
    """
    更新内存池状态
    
    Args:
        if_catch_lastest: 是否已追上最新区块
    """
    global index_height
    global mempool
    global last_mempool
    
    if not if_catch_lastest:
        index_height += 1
        last_mempool = mempool
        mempool = []


async def scan_chain_and_build_index():
    """
    扫描区块链并构建索引
    """
    try:
        # 检查区块高度并确定是否为最新区块
        if_catch_lastest, _ = await check_block_height()
        
        # 获取当前内存池和时间戳
        current_mempool, timestamp = await get_mempool_and_timestamp(if_catch_lastest)
        
        # 找出新交易
        new_txs, old_txs = find_transactions(current_mempool)
        
        # 处理新交易
        await process_transactions(new_txs, old_txs, if_catch_lastest, timestamp)
        
        # 更新内存池状态
        update_mempool_state(if_catch_lastest)
        
        return True
    except Exception as e:
        logging.error("扫描区块链出错: %s", str(e))
        return False


if __name__ == "__main__":
    async def task_wrapper():
        """
        Task wrapper.
        """
        await scan_chain_and_build_index()

    # 每 15 秒调用一次 task_wrapper
    schedule_task(task_wrapper)