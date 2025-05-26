import logging
from datetime import datetime, timezone
from app.dependencies import DBManager
from app.utils.utils import convert_p2ms_script_to_ms_address
from app.dependencies import syclic_call_rpc

async def process_transaction_record(decode_tx, block_height, timestamp, tx_type=None):
    """
    处理交易历史记录并更新相关表
    
    Args:
        decode_tx: 解码后的交易数据
        block_height: 区块高度
        timestamp: 时间戳
        tx_type: 交易类型，如果为None则自动确定
    """
    decode_txid = decode_tx["txid"]
    logging.info("处理交易历史: %s, 区块高度: %s, 时间戳: %s", decode_txid, block_height, timestamp)
    
    # 分析交易并提取数据
    balance_changes = {}
    total_spend = 0
    total_receive = 0
    senders = set()
    receivers = set()
    
    # 使用与 get_history 一致的交易类型检测逻辑
    if tx_type is None:
        tx_type = "P2PKH"  # 默认类型
        if_type_detected = False
    else:
        if_type_detected = True
    
    # 处理输出，获取接收方和总接收金额
    for output in decode_tx['vout']:
        value_get = round(float(output.get('value', 0)) * 1_000_000)
        total_receive += value_get
        
        if "scriptPubKey" in output and "asm" in output["scriptPubKey"]:
            script_asm = output["scriptPubKey"]["asm"]
            
            # 普通地址处理
            if output["scriptPubKey"].get("type") == "pubkeyhash" and "addresses" in output["scriptPubKey"]:
                for addr in output['scriptPubKey']['addresses']:
                    receivers.add(addr)
                    balance_changes[addr] = balance_changes.get(addr, 0) + value_get
            
            # TBC20 FT 处理（与 get_history 保持一致）
            elif script_asm.startswith("9 OP_PICK OP_TOALTSTACK"):
                if not if_type_detected:
                    if_type_detected = True
                    tx_type = "TBC20"
                # 处理 Pool 合约（与 get_history 保持一致）
                if script_asm.endswith("01 32436f6465"):
                    pool_contract_id = "Pool_" + script_asm[-53:-11]
                    receivers.add(pool_contract_id)
                    # Pool 合约不参与余额变化计算
            
            # TBC721 NFT 处理（与 get_history 保持一致）
            elif (script_asm.startswith("OP_RETURN") or 
                  script_asm.startswith("0 OP_RETURN") or 
                  script_asm.startswith("1 OP_PICK")) and not if_type_detected:
                if_type_detected = True
                tx_type = "TBC721"
            
            # P2MS 多重签名处理
            elif script_asm.endswith("OP_CHECKMULTISIG"):
                if not if_type_detected:
                    if_type_detected = True
                    tx_type = "P2MS"
                try:
                    ms_address = convert_p2ms_script_to_ms_address(script_asm)
                    receivers.add(ms_address)
                    balance_changes[ms_address] = balance_changes.get(ms_address, 0) + value_get
                except (ValueError, ImportError):
                    pass
    
    # 处理输入，获取发送方和总支出金额
    for vin in decode_tx['vin']:
        if "txid" not in vin:
            senders.add("coinbase")
            total_spend += 325  # coinbase 固定费用
        else:
            vin_txid = vin['txid']
            vin_vout = vin['vout']
            try:
                vin_decode = await syclic_call_rpc(method="getrawtransaction", params=[vin_txid, 1])
                value_spend = round(float(vin_decode['vout'][vin_vout].get('value', 0)) * 1_000_000)
                total_spend += value_spend
                
                if "scriptPubKey" in vin_decode['vout'][vin_vout] and "asm" in vin_decode['vout'][vin_vout]['scriptPubKey']:
                    vin_script_asm = vin_decode['vout'][vin_vout]['scriptPubKey']['asm']
                    
                    # 普通地址处理
                    if vin_decode['vout'][vin_vout]['scriptPubKey'].get("type") == "pubkeyhash" and "addresses" in vin_decode['vout'][vin_vout]['scriptPubKey']:
                        for addr in vin_decode['vout'][vin_vout]['scriptPubKey']['addresses']:
                            senders.add(addr)
                            balance_changes[addr] = balance_changes.get(addr, 0) - value_spend
                    
                    # P2MS 多重签名处理
                    elif vin_script_asm.endswith("OP_CHECKMULTISIG"):
                        try:
                            ms_address = convert_p2ms_script_to_ms_address(vin_script_asm)
                            senders.add(ms_address)
                            balance_changes[ms_address] = balance_changes.get(ms_address, 0) - value_spend
                        except (ValueError, ImportError):
                            pass
                    
                    # TBC20 Pool 合约处理（与 get_history 保持一致）
                    elif vin_script_asm.startswith("9 OP_PICK OP_TOALTSTACK"):
                        if vin_script_asm.endswith("01 32436f6465"):
                            pool_contract_id = "Pool_" + vin_script_asm[-53:-11]
                            senders.add(pool_contract_id)
                            # Pool 合约不参与余额变化计算
            except Exception as e:
                logging.error("处理交易输入 %s 时出错: %s", decode_txid, str(e))
    
    # 计算手续费
    fee = (total_spend - total_receive) / 1_000_000
    fee_str = f"{fee:f}".rstrip('0').rstrip('.')
    
    # 格式化时间
    utc_time = "unconfirmed" if block_height < 1 else datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    
    # 更新数据库
    await update_transaction_tables(decode_txid, fee_str, timestamp, utc_time, tx_type, block_height, balance_changes, senders, receivers)



async def update_transaction_tables(tx_hash, fee, timestamp, utc_time, tx_type, block_height, balance_changes, senders, receivers):
    """
    更新交易相关数据表
    
    Args:
        tx_hash: 交易哈希
        fee: 手续费
        timestamp: 时间戳
        utc_time: UTC时间
        tx_type: 交易类型
        block_height: 区块高度
        balance_changes: 余额变化字典
        senders: 发送方集合
        receivers: 接收方集合
    """
    # 1. 存储交易基本信息
    transactions_insert_query = """
    INSERT INTO transactions (tx_hash, fee, time_stamp, transaction_utc_time, tx_type, block_height)
    VALUES (%s, %s, %s, %s, %s, %s) AS new
    ON DUPLICATE KEY UPDATE
        fee = new.fee,
        time_stamp = new.time_stamp,
        transaction_utc_time = new.transaction_utc_time,
        tx_type = new.tx_type,
        block_height = new.block_height,
        updated_at = CURRENT_TIMESTAMP
    """
    await DBManager.execute_update(transactions_insert_query, (
        tx_hash, fee, timestamp, utc_time, tx_type, block_height
    ))
    
    # 2. 处理地址交易关系 - 为每个地址分别计算发送方/接收方逻辑（与 get_history 保持一致）
    final_senders = set()
    final_receivers = set()
    
    for address, balance_change in balance_changes.items():
        # 与 get_history 一致的发送方/接收方判断逻辑
        is_sender = False
        is_recipient = False
        
        if balance_change < 0:
            # 余额减少的地址作为发送方
            is_sender = True
            final_senders.add(address)
            # 其他接收方地址
            for receiver in receivers:
                if receiver != address:
                    final_receivers.add(receiver)
        elif balance_change >= 0:
            # 余额增加的地址作为接收方
            is_recipient = True
            final_receivers.add(address)
            # 其他发送方地址
            for sender in senders:
                if sender != address:
                    final_senders.add(sender)
        
        # 格式化余额变化
        balance_float = balance_change / 1_000_000
        formatted_balance = f"{balance_float:.8f}".rstrip('0').rstrip('.')
        if formatted_balance in ('', '+'):
            formatted_balance = "0"
        
        address_tx_insert_query = """
        INSERT INTO address_transactions (address, tx_hash, is_sender, is_recipient, balance_change)
        VALUES (%s, %s, %s, %s, %s) AS new
        ON DUPLICATE KEY UPDATE
            is_sender = new.is_sender,
            is_recipient = new.is_recipient,
            balance_change = new.balance_change,
            updated_at = CURRENT_TIMESTAMP
        """
        await DBManager.execute_update(address_tx_insert_query, (
            address, tx_hash, is_sender, is_recipient, formatted_balance
        ))
    
    # 处理没有余额变化但参与交易的地址（如 Pool 合约等）
    for sender in senders:
        if sender not in balance_changes:
            final_senders.add(sender)
            
            address_tx_insert_query = """
            INSERT INTO address_transactions (address, tx_hash, is_sender, is_recipient, balance_change)
            VALUES (%s, %s, %s, %s, %s) AS new
            ON DUPLICATE KEY UPDATE
                is_sender = new.is_sender,
                is_recipient = new.is_recipient,
                balance_change = new.balance_change,
                updated_at = CURRENT_TIMESTAMP
            """
            await DBManager.execute_update(address_tx_insert_query, (
                sender, tx_hash, True, False, "0"
            ))
    
    for receiver in receivers:
        if receiver not in balance_changes:
            final_receivers.add(receiver)
            
            address_tx_insert_query = """
            INSERT INTO address_transactions (address, tx_hash, is_sender, is_recipient, balance_change)
            VALUES (%s, %s, %s, %s, %s) AS new
            ON DUPLICATE KEY UPDATE
                is_sender = new.is_sender,
                is_recipient = new.is_recipient,
                balance_change = new.balance_change,
                updated_at = CURRENT_TIMESTAMP
            """
            await DBManager.execute_update(address_tx_insert_query, (
                receiver, tx_hash, False, True, "0"
            ))
    
    # 确保至少有一个发送方和接收方（与 get_history 逻辑保持一致）
    if len(final_senders) == 0 and len(balance_changes) > 0:
        # 如果没有发送方，取第一个有余额变化的地址作为发送方
        first_address = next(iter(balance_changes.keys()))
        final_senders.add(first_address)
    
    if len(final_receivers) == 0 and len(balance_changes) > 0:
        # 如果没有接收方，取第一个有余额变化的地址作为接收方
        first_address = next(iter(balance_changes.keys()))
        final_receivers.add(first_address)
    
    # 3. 处理交易参与方
    # 清除旧记录
    await DBManager.execute_update("DELETE FROM transaction_participants WHERE tx_hash = %s", (tx_hash,))
    
    # 插入发送方
    for sender in final_senders:
        await DBManager.execute_update(
            "INSERT INTO transaction_participants (tx_hash, address, role) VALUES (%s, %s, %s)",
            (tx_hash, sender, "sender")
        )
    
    # 插入接收方
    for receiver in final_receivers:
        await DBManager.execute_update(
            "INSERT INTO transaction_participants (tx_hash, address, role) VALUES (%s, %s, %s)",
            (tx_hash, receiver, "recipient")
        )
