import logging
from app.dependencies import DBManager


async def process_ft_tokens(decode_tx, output_index, timestamp):
    """处理同质化代币信息并更新ft_tokens表"""
    decode_txid = decode_tx["txid"]
    
    if not decode_tx["vout"][output_index]["scriptPubKey"]["asm"].startswith("9 OP_PICK OP_TOALTSTACK"):
        return output_index, None, None, None, False
    
    if len(decode_tx["vout"]) - output_index <= 1:
        logging.error("Error FT Protocal: %s", decode_txid)
        return output_index, None, None, None, True

    # 排除错误版本的 TBC20
    if decode_tx["vout"][output_index]["scriptPubKey"]["asm"][-32:-11] == "OP_CHECKSIG OP_RETURN":
        return output_index, None, None, None, True
    
    vout_combine_script = decode_tx["vout"][output_index]["scriptPubKey"]["hex"][-54:-12]
    ft_balance = 0
    ft_balance_tape = decode_tx["vout"][output_index + 1]["scriptPubKey"]["asm"][12:108]
    for i in range(0, len(ft_balance_tape), 16):
        segment = ft_balance_tape[i:i+16]
        segment = ''.join([segment[i:i+2] for i in range(0, len(segment), 2)][::-1])
        ft_balance += int(segment, 16)
    vout_utxo_balance = round(decode_tx["vout"][output_index]["value"] * 1_000_000)
    
    # 确定是否为首次铸造
    ft_origin_utxo = decode_tx["vout"][output_index]["scriptPubKey"]["asm"][2384:2456]
    # 处理新版本代币协议
    if ft_origin_utxo == "P OP_EQUAL OP_IF OP_FROMALTSTACK OP_DROP OP_TOALTSTACK OP_TOALTSTACK OP_":
        ft_origin_utxo = ft_origin_utxo = decode_tx["vout"][output_index]["scriptPubKey"]["asm"][2477:2549]
    # 处理 LP 输出
    if ft_origin_utxo == "UALVERIFY OP_ENDIF OP_DUP 2 OP_EQUAL OP_IF OP_DROP 2 OP_PICK 2 OP_PICK O":
        ft_origin_utxo = "LP"
    
    ft_tokens_query = """
    SELECT ft_contract_id FROM ft_tokens WHERE ft_origin_utxo = %s
    """
    ft_tokens_query_res = await DBManager.execute_query(ft_tokens_query, (ft_origin_utxo,))
    
    # 转移 FT
    if ft_tokens_query_res:
        logging.info("FT Transfer:            %s", decode_txid)
        ft_contract_id = ft_tokens_query_res[0][0]
    # 铸造 FT
    else:
        logging.info("FT Mint:                %s", decode_txid)

        ft_contract_id = decode_txid
        ft_code_script = decode_tx["vout"][output_index]["scriptPubKey"]["hex"]
        ft_tape_script = decode_tx["vout"][output_index + 1]["scriptPubKey"]["hex"]
        ft_supply = ft_balance
        ft_decimal = 0
        ft_name = ""
        ft_symbol = ""
        ft_description = ""
        ft_creator_combine_script = vout_combine_script
        ft_holders_count = 0
        ft_icon_url = ""
        ft_create_timestamp = timestamp
        ft_token_price = 0.0

        try:
            tape_parts = decode_tx["vout"][output_index + 1]["scriptPubKey"]["asm"].split(" ")
            ft_decimal = int(tape_parts[3])
            ft_tape_info = decode_tx["vout"][output_index + 1]["scriptPubKey"]["hex"][106:-12]
            ft_name_len = int(ft_tape_info[0:2], 16)
            ft_name = bytes.fromhex(ft_tape_info[2:2+ft_name_len*2]).decode('utf-8')
            ft_symbol_len = int(ft_tape_info[2+ft_name_len*2:4+ft_name_len*2], 16)
            ft_symbol = bytes.fromhex(ft_tape_info[4+ft_name_len*2:4+ft_name_len*2+ft_symbol_len*2]).decode('utf-8')
        except Exception as e:
            logging.error("Error parsing FT token info %s: %s", decode_txid, e)
            return output_index, None, None, None, True
        
        # 插入记录到 ft_tokens 表
        ft_token_insert_query = """
        INSERT INTO ft_tokens (ft_contract_id, ft_code_script, ft_tape_script, ft_supply, ft_decimal, ft_name, ft_symbol, 
                            ft_description, ft_origin_utxo, ft_creator_combine_script, ft_holders_count, ft_icon_url, ft_create_timestamp, ft_token_price)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) AS new
        ON DUPLICATE KEY UPDATE
            ft_code_script = new.ft_code_script,
            ft_supply = new.ft_supply,
            ft_decimal = new.ft_decimal,
            ft_name = new.ft_name,
            ft_symbol = new.ft_symbol,
            ft_description = new.ft_description,
            ft_origin_utxo = new.ft_origin_utxo,
            ft_creator_combine_script = new.ft_creator_combine_script,
            ft_holders_count = new.ft_holders_count,
            ft_icon_url = new.ft_icon_url,
            ft_create_timestamp = new.ft_create_timestamp,
            ft_token_price = new.ft_token_price
        """
        try:
            await DBManager.execute_update(ft_token_insert_query, (ft_contract_id, ft_code_script, ft_tape_script, ft_supply, ft_decimal, ft_name, ft_symbol, ft_description, ft_origin_utxo, ft_creator_combine_script, ft_holders_count, ft_icon_url, ft_create_timestamp, ft_token_price))
        except Exception as e:
            logging.error("Error inserting FT token %s: %s", decode_txid, e)
            return output_index, None, None, None, True
        
    return output_index + 2, ft_contract_id, vout_combine_script, ft_balance, False


async def process_ft_txo_set(decode_tx, output_index, ft_contract_id, vout_combine_script, ft_balance):
    """处理同质化代币UTXO并更新ft_txo_set表（仅处理当前输出）"""
    decode_txid = decode_tx["txid"]
    
    if ft_contract_id is None:
        return False
    
    vout_utxo_balance = round(decode_tx["vout"][output_index - 2]["value"] * 1_000_000)
    if_spend = 0
    
    # 插入记录到 ft_txo_set 表
    ft_utxo_set_insert_query = """
    INSERT INTO ft_txo_set (utxo_txid, utxo_vout, ft_holder_combine_script, ft_contract_id, utxo_balance, ft_balance, if_spend)
    VALUES (%s, %s, %s, %s, %s, %s, %s) AS new
    ON DUPLICATE KEY UPDATE
        ft_holder_combine_script = new.ft_holder_combine_script,
        ft_contract_id = new.ft_contract_id,
        utxo_balance = new.utxo_balance,
        ft_balance = new.ft_balance,
        if_spend = new.if_spend
    """
    try:
        await DBManager.execute_update(ft_utxo_set_insert_query, (decode_txid, output_index - 2, vout_combine_script, ft_contract_id, vout_utxo_balance, ft_balance, if_spend))
    except Exception as e:
        logging.error("Error inserting FT TXO set %s: %s", decode_txid, e)
        return True
    
    return False


async def process_ft_balance(ft_contract_id, vout_combine_script, ft_balance):
    """处理同质化代币余额并更新ft_balance表（仅处理输出余额增加）"""
    if ft_contract_id is None:
        return False
    
    # 如果 ft_balance 记录不存在，插入记录到 ft_balance 表
    ft_balance_query = """
    SELECT ft_balance FROM ft_balance WHERE ft_contract_id = %s and ft_holder_combine_script = %s
    """
    try:
        ft_balance_query_res = await DBManager.execute_query(ft_balance_query, (ft_contract_id, vout_combine_script))
        
        if not ft_balance_query_res:
            ft_balance_insert_query = """
            INSERT INTO ft_balance (ft_holder_combine_script, ft_contract_id, ft_balance)
            VALUES (%s, %s, %s)
            """
            await DBManager.execute_update(ft_balance_insert_query, (vout_combine_script, ft_contract_id, ft_balance))

            # ft_holders_count 增加
            ft_tokens_update = """
            UPDATE ft_tokens
            SET ft_holders_count = ft_holders_count + 1
            WHERE ft_contract_id = %s
            """
            await DBManager.execute_update(ft_tokens_update, (ft_contract_id,))
        else:
            ft_balance_update_query = """
            UPDATE ft_balance
            SET ft_balance = ft_balance + %s
            WHERE ft_holder_combine_script = %s and ft_contract_id = %s
            """
            await DBManager.execute_update(ft_balance_update_query, (ft_balance, vout_combine_script, ft_contract_id))
    except Exception as e:
        logging.error("Error updating FT balance %s: %s", ft_contract_id, e)
        return True
    
    return False


async def process_ft_inputs(decode_tx):
    """处理交易的所有FT输入，更新已花费的UTXO并返回已花费的UTXO信息"""
    spent_utxo_info_list = []
    
    # 更新已花费的 UTXO
    for vin in decode_tx["vin"]:
        if "scriptSig" in vin and vin["scriptSig"]["asm"].startswith("1 "):
            ft_txo_query = """
            SELECT ft_contract_id, ft_holder_combine_script, ft_balance
            FROM ft_txo_set
            WHERE utxo_txid = %s AND utxo_vout = %s
            """
            try:
                ft_txo_query_res = await DBManager.execute_query(ft_txo_query, (vin["txid"], vin["vout"]))
                if ft_txo_query_res and len(ft_txo_query_res) > 0 and len(ft_txo_query_res[0]) == 3:
                    # 更新 ft_txo_set
                    ft_utxo_update_query = """
                    UPDATE ft_txo_set
                    SET if_spend = 1
                    WHERE utxo_txid = %s AND utxo_vout = %s
                    """
                    await DBManager.execute_update(ft_utxo_update_query, (vin["txid"], vin["vout"]))
                    
                    # 添加到已花费UTXO列表
                    spent_utxo_info_list.append(ft_txo_query_res[0])
                elif ft_txo_query_res:
                    logging.warning("Invalid ft_txo_query_res format for %s: %s", vin["txid"], ft_txo_query_res)
            except Exception as e:
                logging.error("Error updating spent UTXO %s: %s", vin["txid"], e)
                return []
    
    return spent_utxo_info_list



async def process_spent_ft_balances(spent_utxo_info_list):
    """处理已花费的FT UTXO对应的余额更新"""
    if not spent_utxo_info_list or not isinstance(spent_utxo_info_list, list) or len(spent_utxo_info_list) == 0:
        return False
        
    for spent_utxo_info in spent_utxo_info_list:
        if not spent_utxo_info or len(spent_utxo_info) != 3:
            logging.warning("Invalid spent_utxo_info format: %s", spent_utxo_info)
            continue
            
        spent_ft_contract_id, spent_holder_script, spent_ft_balance = spent_utxo_info
        
        ft_balance_query = """
        SELECT ft_balance
        FROM ft_balance
        WHERE ft_contract_id = %s AND ft_holder_combine_script = %s
        """
        try:
            ft_balance_query_res = await DBManager.execute_query(ft_balance_query, (spent_ft_contract_id, spent_holder_script))
            
            if ft_balance_query_res:
                ft_balance_balance = ft_balance_query_res[0][0]
                # 如果 ft_balance.ft_balance 等于 ft_balance，删除记录并且 ft_tokens.ft_holders_count - 1
                if ft_balance_balance == spent_ft_balance:
                    ft_balance_delete_query = """
                    DELETE FROM ft_balance 
                    WHERE ft_holder_combine_script = %s 
                    AND ft_contract_id = %s 
                    """
                    await DBManager.execute_update(ft_balance_delete_query, (spent_holder_script, spent_ft_contract_id))
                    
                    ft_tokens_update = """
                    UPDATE ft_tokens
                    SET ft_holders_count = ft_holders_count - 1
                    WHERE ft_contract_id = %s
                    """
                    await DBManager.execute_update(ft_tokens_update, (spent_ft_contract_id,))
                elif ft_balance_balance > spent_ft_balance:
                    ft_balance_update_query = """
                    UPDATE ft_balance
                    SET ft_balance = ft_balance - %s
                    WHERE ft_holder_combine_script = %s 
                    AND ft_contract_id = %s 
                    """                            
                    await DBManager.execute_update(ft_balance_update_query, (spent_ft_balance, spent_holder_script, spent_ft_contract_id))
        except Exception as e:
            logging.error("Error updating spent FT balance %s: %s", spent_ft_contract_id, e)
            return True
    
    return False
