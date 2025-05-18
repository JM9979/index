import asyncio
import json
import time
import logging
import os
import base64
import tempfile
import argparse
from app.dependencies import call_node_rpc
from app.dependencies import DBManager
from app.utils import hex_to_json, convert_str_to_sha256
from app.s3 import S3Uploader
from datetime import datetime, timezone, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Parse command line arguments
parser = argparse.ArgumentParser(description='Build index for blockchain data')
parser.add_argument('--clear-db', action='store_true', help='Clear database before building index')

first_index_height = 862600
# 定义并初始化全局变量
index_height = 862600
index_interval = 0
mempool = []
last_mempool = []
s3_uploader = S3Uploader()  # 初始化S3上传器

def upload_base64_image_to_s3(image_data, object_name, content_type='image/jpeg'):
    """
    上传base64编码的图片到S3存储
    
    Args:
        image_data: base64编码的图片数据，格式如"data:image/jpeg;base64,/9j/4AAQSkZ..."
        object_name: S3中的对象名称，如"collections/xyz.jpg"
        content_type: 文件的内容类型，默认为'image/jpeg'
        
    Returns:
        tuple: (success, result)
            - success: 布尔值，表示上传是否成功
            - result: 成功时为图片URL，失败时为原始图片数据
    """
    if not image_data.startswith('data:image'):
        return False, image_data
    
    try:
        # 先检查S3上是否已经存在该对象
        if s3_uploader.check_object_exists(object_name):
            # 对象已存在，直接获取URL
            image_url = s3_uploader.get_public_url(object_name=object_name)
            logging.info("Image already exists in S3, reusing: %s", image_url)
            return True, image_url
            
        # 如果不存在，则进行上传
        # 解析Base64图片数据
        image_data_encoded = image_data.split(',')[1]
        image_bytes = base64.b64decode(image_data_encoded)
        
        # 创建临时文件保存图片
        with tempfile.NamedTemporaryFile(delete=False, suffix='.jpg') as temp_file:
            temp_file_path = temp_file.name
            temp_file.write(image_bytes)
        
        # 上传到S3
        success, _ = s3_uploader.upload_image(
            file_path=temp_file_path,
            object_name=object_name,
            content_type=content_type
        )
        
        # 删除临时文件
        os.unlink(temp_file_path)
        
        if success:
            # 获取公共URL
            image_url = s3_uploader.get_public_url(object_name=object_name)
            logging.info("Image uploaded to S3: %s", image_url)
            return True, image_url
        else:
            return False, image_data
            
    except Exception as e:
        logging.error("Error uploading image to S3: %s", str(e))
        return False, image_data

async def syclic_call_rpc(method, params):
    """
    Syclic call RPC.
    """
    retry_interval = 5
    while True:
        try:
            res = await call_node_rpc(method=method, params=params)
            return res
        except (ConnectionError, TimeoutError, ValueError) as e:
            logging.error("Error calling node RPC %s: %s. Retrying in %s seconds...", method, e, retry_interval)
            await asyncio.sleep(retry_interval)


async def process_nft_collections(conn, decode_tx, output_index, timestamp):
    """处理NFT集合信息并更新nft_collections表"""
    decode_txid = decode_tx["txid"]
    
    if not (decode_tx["vout"][output_index]["scriptPubKey"]["asm"].startswith("0 OP_RETURN") or 
            decode_tx["vout"][output_index]["scriptPubKey"]["asm"].startswith("OP_RETURN")):
        return output_index, None
        
    logging.info("TBC721 Collection:      %s", decode_txid)

    if len(decode_tx["vout"]) - output_index <= 1:
        logging.error("Error Collection Protocal: %s", decode_txid)
        return output_index + 1, None
    if decode_tx["vout"][output_index + 1]["scriptPubKey"]["type"] != "pubkeyhash":
        logging.error("Error Collection Protocal: %s", decode_txid)
        return output_index + 1, None

    # 准备集合插入数据
    collection_id = decode_txid
    collection_creator_address = decode_tx["vout"][output_index + 1]["scriptPubKey"]["addresses"][0]
    collection_creator_script_hash = convert_str_to_sha256(decode_tx["vout"][output_index]["scriptPubKey"]["hex"])
    collection_create_timestamp = timestamp
    
    if decode_tx["vout"][output_index]["scriptPubKey"]["asm"].startswith("0 OP_RETURN"):
        collection_tape_hex = decode_tx["vout"][output_index]["scriptPubKey"]["asm"][12:-11]
    else:
        collection_tape_hex = decode_tx["vout"][output_index]["scriptPubKey"]["asm"][10:-10]
    
    try:
        collection_tape_json = hex_to_json(collection_tape_hex)
    except ValueError:
        logging.error("Error decoding Collection tape %s", decode_txid)
        return output_index + 1, None
        
    collection_name = collection_tape_json.get("collectionName", "")
    collection_symbol = collection_tape_json.get("symbol", "")
    collection_attributes = collection_tape_json.get("attributes", "")
    collection_description = collection_tape_json.get("description", "")
    collection_supply = collection_tape_json.get("supply", 0)
    
    if collection_supply <= 0:
        logging.error("Wrong Collection supply input: %s", decode_txid)
        return output_index + 1, None
    
    # 处理集合图标 - 上传到S3
    collection_icon = collection_tape_json.get("file", "")
    if collection_icon and not collection_icon.startswith('http'):
        try:
            # 上传到S3
            object_name = f"collections/{collection_id}.jpg"
            success, collection_icon = upload_base64_image_to_s3(
                image_data=collection_icon,
                object_name=object_name
            )
        except Exception as e:
            logging.error("Error uploading collection icon to S3: %s", str(e))
            # 如果上传失败，保留原始数据

    # 插入记录到 nft_collections 表
    nft_collection_insert_query = """
    INSERT INTO nft_collections (collection_id, collection_name, collection_creator_address, collection_creator_script_hash, collection_symbol, collection_attributes, collection_description, collection_supply, collection_create_timestamp, collection_icon)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) AS new
    ON DUPLICATE KEY UPDATE
        collection_name = new.collection_name,
        collection_creator_address = new.collection_creator_address,
        collection_creator_script_hash = new.collection_creator_script_hash,
        collection_symbol = new.collection_symbol,
        collection_description = new.collection_description,
        collection_supply = new.collection_supply,
        collection_create_timestamp = new.collection_create_timestamp,
        collection_icon = new.collection_icon
    """
    
    try:
        await DBManager.execute_update_nocommit(conn, nft_collection_insert_query, (collection_id, collection_name, collection_creator_address, collection_creator_script_hash, collection_symbol, collection_attributes, collection_description, collection_supply, collection_create_timestamp, collection_icon))
        return output_index + collection_supply, collection_id
    except Exception as e:
        logging.error("Error inserting collection %s: %s", decode_txid, e)
        return output_index + 1, None


async def process_nft_utxo_set(conn, decode_tx, output_index, timestamp):
    """处理NFT信息并更新nft_utxo_set表"""
    decode_txid = decode_tx["txid"]
    
    if not (decode_tx["vout"][output_index]["scriptPubKey"]["asm"].startswith("1 OP_PICK 3 OP_SPLIT") or 
            decode_tx["vout"][output_index]["scriptPubKey"]["asm"].startswith("4 OP_PICK OP_BIN2NUM OP_TOALTSTACK 1 OP_PICK 3 OP_SPLIT")):
        return output_index, None
    
    nft_tape_json = {}
    
    if decode_tx["vout"][output_index]["scriptPubKey"]["asm"].startswith("1 OP_PICK 3 OP_SPLIT 20"):
        logging.info("TBC721 NFT:             %s", decode_txid)

        if len(decode_tx["vout"]) - output_index <= 2:
            logging.error("Error NFT Protocal: %s", decode_txid)
            return output_index + 1, None

        # 解码 tape json
        if decode_tx['vout'][output_index + 2]['scriptPubKey']['asm'].startswith("0 OP_RETURN"):
            nft_tape_hex = decode_tx['vout'][output_index + 2]['scriptPubKey']['asm'][12:-11]
        elif decode_tx['vout'][output_index + 2]['scriptPubKey']['asm'].startswith("OP_RETURN"):
            nft_tape_hex = decode_tx['vout'][output_index + 2]['scriptPubKey']['asm'][10:-10]
        else:
            logging.error("Error decoding nft scriptPubKey asm %s", decode_txid)
            return output_index + 1, None
        
        try:
            nft_tape_json = hex_to_json(nft_tape_hex)
        except ValueError:
            logging.error("Error decoding NFT tape %s", decode_txid)
            nft_tape_json = {}
            
        nft_offset = 3
            
    elif decode_tx["vout"][output_index]["scriptPubKey"]["asm"].startswith("1 OP_PICK 3 OP_SPLIT OP_NIP") or decode_tx["vout"][output_index]["scriptPubKey"]["asm"].startswith("4 OP_PICK OP_BIN2NUM OP_TOALTSTACK 1 OP_PICK 3 OP_SPLIT"):
        logging.info("Pool NFT:               %s", decode_txid)
        
        if len(decode_tx["vout"]) - output_index <= 1:
            logging.error("Error Pool NFT Protocal: %s", decode_txid)
            return output_index + 1, None
            
        nft_tape_hex = "POOLNFT"
        pool_tape_list = decode_tx["vout"][output_index + 1]["scriptPubKey"]["asm"].split(" ")
        token_pair_a_id = pool_tape_list[4]
        nft_tape_json = {"file": token_pair_a_id}
        nft_offset = 2
        
    else:
        logging.error("Error decoding nft scriptPubKey asm %s", decode_txid)
        return output_index + 1, None
    
    # 准备 NFT 插入数据
    nft_code_balance = round(decode_tx["vout"][output_index]["value"] * 1_000_000)
    nft_p2pkh_balance = round(decode_tx["vout"][output_index + 1]["value"] * 1_000_000)
    nft_holder_address = decode_tx["vout"][output_index + 1]["scriptPubKey"]["addresses"][0] if "addresses" in decode_tx["vout"][output_index + 1]["scriptPubKey"] else "LP"
    nft_holder_script_hash = convert_str_to_sha256(decode_tx["vout"][output_index + 1]["scriptPubKey"]["hex"])

    # 确定是否为首次铸造
    if len(decode_tx["vin"][0]["scriptSig"]["hex"]) > 500:
        logging.info("NFT Transfer:           %s", decode_txid)

        if nft_tape_hex == "POOLNFT":
            first_vin_txid = decode_tx["vin"][0]["txid"]
            nft_contract_id_query = """
            SELECT nft_contract_id
            FROM nft_utxo_set
            WHERE nft_utxo_id = %s
            """
            nft_contract_id_res = await DBManager.execute_query(nft_contract_id_query, (first_vin_txid,))
            if nft_contract_id_res:
                nft_contract_id = nft_contract_id_res[0][0]
            else:
                logging.error("Can not find which NFT the first input belong %s", decode_txid)
                return output_index + 1, None
        else:
            nft_file = nft_tape_json.get("file", "")
            if len(nft_file) != 72:
                logging.error("Error NFT Transfer Tape file: %s", decode_txid)
                return output_index + 1, None
            nft_contract_id = nft_file[:64]

        nft_update_query = """
        UPDATE nft_utxo_set
        SET nft_utxo_id = %s, nft_code_balance = %s, nft_p2pkh_balance = %s, nft_holder_address = %s, nft_holder_script_hash = %s, nft_last_transfer_timestamp = %s, nft_transfer_time_count = nft_transfer_time_count + 1
        WHERE nft_contract_id = %s
        """
        await DBManager.execute_update_nocommit(conn, nft_update_query, (decode_txid, nft_code_balance, nft_p2pkh_balance, nft_holder_address, nft_holder_script_hash, timestamp, nft_contract_id))
    else:
        logging.info("NFT Mint:               %s", decode_txid)
        
        collection_id = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
        collection_index = 0
        collection_name = "NOCOLLECTION"
        collection_icon = ""

        # 如果从集合铸造，获取 collection_id 和 collection_index
        for vin in decode_tx["vin"]:
            vin_txid, vin_vout = vin["txid"], vin["vout"]
            collection_query = """
            SELECT collection_id, collection_supply, collection_name, collection_icon FROM nft_collections WHERE collection_id = %s
            """
            collection_query_res = await DBManager.execute_query(collection_query, (vin_txid,))
            if collection_query_res:
                (record_collection_id, record_collection_supply, record_collection_name, record_collection_icon) = collection_query_res[0]
                if vin_vout <= record_collection_supply:
                    collection_id = record_collection_id
                    collection_index = vin_vout
                    collection_name = record_collection_name
                    collection_icon = record_collection_icon
                    break
        
        # 插入记录到 nft_utxo_set 表
        nft_contract_id = decode_txid
        nft_utxo_id = decode_txid
        nft_name = nft_tape_json.get("nftName", "")
        nft_symbol = nft_tape_json.get("symbol", "")
        nft_attributes = nft_tape_json.get("attributes", "")
        nft_description = nft_tape_json.get("description", "")
        nft_transfer_time_count = 0
        nft_create_timestamp = timestamp
        nft_last_transfer_timestamp = timestamp
        
        # 处理NFT图标 - 上传到S3
        nft_icon = nft_tape_json.get("file", "")
        if nft_icon and not nft_icon.startswith('http'):
            # 如果file是64+8长度的格式，保留原值
            if len(nft_icon) == 72:
                nft_icon = collection_icon
            # 否则尝试作为图片数据处理并上传到S3
            elif not nft_icon.startswith('http'):
                try:
                    # 上传到S3
                    object_name = f"nfts/{nft_contract_id}.jpg"
                    success, nft_icon = upload_base64_image_to_s3(
                        image_data=nft_icon,
                        object_name=object_name
                    )
                except Exception as e:
                    logging.error("Error uploading NFT icon to S3: %s", str(e))
                    # 如果上传失败，保留原始数据
        
        nft_utxo_set_insert_query = """
        INSERT INTO nft_utxo_set (nft_contract_id, collection_id, collection_index, collection_name, nft_utxo_id, nft_code_balance, nft_p2pkh_balance, nft_name, nft_symbol, nft_attributes, nft_description, nft_transfer_time_count, nft_holder_address, nft_holder_script_hash, nft_create_timestamp, nft_last_transfer_timestamp, nft_icon)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) AS new
        ON DUPLICATE KEY UPDATE
            collection_id = new.collection_id,
            collection_index = new.collection_index,
            collection_name = new.collection_name,
            nft_utxo_id = new.nft_utxo_id,
            nft_code_balance = new.nft_code_balance,
            nft_p2pkh_balance = new.nft_p2pkh_balance,
            nft_name = new.nft_name,
            nft_symbol = new.nft_symbol,
            nft_attributes = new.nft_attributes,
            nft_description = new.nft_description,
            nft_transfer_time_count = new.nft_transfer_time_count,
            nft_holder_address = new.nft_holder_address,
            nft_holder_script_hash = new.nft_holder_script_hash,
            nft_create_timestamp = new.nft_create_timestamp,
            nft_last_transfer_timestamp = new.nft_last_transfer_timestamp,
            nft_icon = new.nft_icon
        """
        await DBManager.execute_update_nocommit(conn, nft_utxo_set_insert_query, (nft_contract_id, collection_id, collection_index, collection_name, nft_utxo_id, nft_code_balance, nft_p2pkh_balance, nft_name, nft_symbol, nft_attributes, nft_description, nft_transfer_time_count, nft_holder_address, nft_holder_script_hash, nft_create_timestamp, nft_last_transfer_timestamp, nft_icon))
    
    return output_index + nft_offset, nft_contract_id


async def process_ft_tokens(conn, decode_tx, output_index, timestamp):
    """处理同质化代币信息并更新ft_tokens表"""
    decode_txid = decode_tx["txid"]
    
    if not decode_tx["vout"][output_index]["scriptPubKey"]["asm"].startswith("9 OP_PICK OP_TOALTSTACK"):
        return output_index + 1, None, None, None
    
    if len(decode_tx["vout"]) - output_index <= 1:
        logging.error("Error FT Protocal: %s", decode_txid)
        return output_index + 1, None, None, None

    # 排除错误版本的 TBC20
    if decode_tx["vout"][output_index]["scriptPubKey"]["asm"][-32:-11] == "OP_CHECKSIG OP_RETURN":
        return output_index + 1, None, None, None
    
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

        tape_parts = decode_tx["vout"][output_index + 1]["scriptPubKey"]["asm"].split(" ")
        ft_decimal = int(tape_parts[3])
        ft_tape_info = decode_tx["vout"][output_index + 1]["scriptPubKey"]["hex"][106:-12]
        ft_name_len = int(ft_tape_info[0:2], 16)
        ft_name = bytes.fromhex(ft_tape_info[2:2+ft_name_len*2]).decode('utf-8')
        ft_symbol_len = int(ft_tape_info[2+ft_name_len*2:4+ft_name_len*2], 16)
        ft_symbol = bytes.fromhex(ft_tape_info[4+ft_name_len*2:4+ft_name_len*2+ft_symbol_len*2]).decode('utf-8')
        
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
        await DBManager.execute_update_nocommit(conn, ft_token_insert_query, (ft_contract_id, ft_code_script, ft_tape_script, ft_supply, ft_decimal, ft_name, ft_symbol, ft_description, ft_origin_utxo, ft_creator_combine_script, ft_holders_count, ft_icon_url, ft_create_timestamp, ft_token_price))
        
    return output_index + 2, ft_contract_id, vout_combine_script, ft_balance


async def process_ft_txo_set(conn, decode_tx, output_index, ft_contract_id, vout_combine_script, ft_balance):
    """处理同质化代币UTXO并更新ft_txo_set表"""
    decode_txid = decode_tx["txid"]
    
    if ft_contract_id is None:
        return
    
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
    await DBManager.execute_update_nocommit(conn, ft_utxo_set_insert_query, (decode_txid, output_index - 2, vout_combine_script, ft_contract_id, vout_utxo_balance, ft_balance, if_spend))
    
    # 更新已花费的 UTXO
    for vin in decode_tx["vin"]:
        if "scriptSig" in vin and vin["scriptSig"]["asm"].startswith("1 "):
            ft_txo_query = """
            SELECT ft_contract_id, ft_holder_combine_script, ft_balance
            FROM ft_txo_set
            WHERE utxo_txid = %s AND utxo_vout = %s
            """
            ft_txo_query_res = await DBManager.execute_query(ft_txo_query, (vin["txid"], vin["vout"]))
            if ft_txo_query_res:
                # 更新 ft_txo_set
                ft_utxo_update_query = """
                UPDATE ft_txo_set
                SET if_spend = 1
                WHERE utxo_txid = %s AND utxo_vout = %s
                """
                await DBManager.execute_update_nocommit(conn, ft_utxo_update_query, (vin["txid"], vin["vout"]))
                
                # 返回已花费的UTXO信息，供ft_balance处理函数使用
                return ft_txo_query_res[0]
    
    return None


async def process_ft_balance(conn, ft_contract_id, vout_combine_script, ft_balance, spent_utxo_info=None):
    """处理同质化代币余额并更新ft_balance表"""
    if ft_contract_id is None:
        return
    
    # 如果 ft_balance 记录不存在，插入记录到 ft_balance 表
    ft_balance_query = """
    SELECT ft_balance FROM ft_balance WHERE ft_contract_id = %s and ft_holder_combine_script = %s
    """
    ft_balance_query_res = await DBManager.execute_query(ft_balance_query, (ft_contract_id, vout_combine_script))
    
    if not ft_balance_query_res:
        ft_balance_insert_query = """
        INSERT INTO ft_balance (ft_holder_combine_script, ft_contract_id, ft_balance)
        VALUES (%s, %s, %s)
        """
        await DBManager.execute_update_nocommit(conn, ft_balance_insert_query, (vout_combine_script, ft_contract_id, ft_balance))

        # ft_holders_count 增加
        ft_tokens_update = """
        UPDATE ft_tokens
        SET ft_holders_count = ft_holders_count + 1
        WHERE ft_contract_id = %s
        """
        await DBManager.execute_update_nocommit(conn, ft_tokens_update, (ft_contract_id,))
    else:
        ft_balance_update_query = """
        UPDATE ft_balance
        SET ft_balance = ft_balance + %s
        WHERE ft_holder_combine_script = %s and ft_contract_id = %s
        """
        await DBManager.execute_update_nocommit(conn, ft_balance_update_query, (ft_balance, vout_combine_script, ft_contract_id))
    
    # 处理已花费的UTXO对应的余额
    if spent_utxo_info:
        spent_ft_contract_id, spent_holder_script, spent_ft_balance = spent_utxo_info
        
        ft_balance_query = """
        SELECT ft_balance
        FROM ft_balance
        WHERE ft_contract_id = %s AND ft_holder_combine_script = %s
        """
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
                await DBManager.execute_update_nocommit(conn, ft_balance_delete_query, (spent_holder_script, spent_ft_contract_id))
                
                ft_tokens_update = """
                UPDATE ft_tokens
                SET ft_holders_count = ft_holders_count - 1
                WHERE ft_contract_id = %s
                """
                await DBManager.execute_update_nocommit(conn, ft_tokens_update, (spent_ft_contract_id,))
            elif ft_balance_balance > spent_ft_balance:
                ft_balance_update_query = """
                UPDATE ft_balance
                SET ft_balance = ft_balance - %s
                WHERE ft_holder_combine_script = %s 
                AND ft_contract_id = %s 
                """                            
                await DBManager.execute_update_nocommit(conn, ft_balance_update_query, (spent_ft_balance, spent_holder_script, spent_ft_contract_id))


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


async def process_single_transaction(conn, tx, block_height, timestamp):
    """
    处理单个交易，包括交易历史记录和代币操作
    
    Args:
        tx: 交易哈希
        block_height: 区块高度
        timestamp: 时间戳
        
    Returns:
        bool: 处理是否成功
    """
    try:
        # 解码原始交易
        decode_tx = await syclic_call_rpc(method="getrawtransaction", params=[tx, 1])
        decode_txid = decode_tx["txid"]

        # 检查黑名单
        if is_in_blacklist(decode_txid):
            return False
        
        # 分析交易数据，获取交易类型和UTXO类型
        tx_analysis = await analyze_transaction_data(decode_tx)
        
        # 处理交易历史记录
        await process_transaction_record(conn, decode_tx, block_height, timestamp, tx_analysis['tx_type'])

        # 处理代币相关UTXO
        await process_tx_utxos(conn, decode_tx, timestamp, tx_analysis['utxo_types'])
        
        return True
    except Exception as e:
        logging.error("处理交易 %s 时出错: %s", tx, str(e))
        return False


def is_in_blacklist(txid):
    """检查交易ID是否在黑名单中"""
    try:
        with open('black_list.txt', 'r', encoding='utf-8') as file:
            black_list = [line.strip() for line in file]
        return txid in black_list
    except FileNotFoundError:
        return False


async def process_transaction_record(conn, decode_tx, block_height, timestamp, tx_type=None):
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
    
    # 如果没有提供交易类型，则确定交易类型
    if tx_type is None:
        tx_type = determine_tx_type(decode_tx)
    
    # 处理输出，获取接收方和总接收金额
    for output in decode_tx['vout']:
        value_get = round(float(output.get('value', 0)) * 1_000_000)
        total_receive += value_get
        
        if "scriptPubKey" in output:
            if output["scriptPubKey"].get("type") == "pubkeyhash" and "addresses" in output["scriptPubKey"]:
                for addr in output['scriptPubKey']['addresses']:
                    receivers.add(addr)
                    balance_changes[addr] = balance_changes.get(addr, 0) + value_get
            elif output["scriptPubKey"]["asm"].endswith("OP_CHECKMULTISIG"):
                try:
                    from app.utils import convert_p2ms_script_to_ms_address
                    ms_address = convert_p2ms_script_to_ms_address(output["scriptPubKey"]["asm"])
                    receivers.add(ms_address)
                    balance_changes[ms_address] = balance_changes.get(ms_address, 0) + value_get
                except (ValueError, ImportError):
                    pass
    
    # 处理输入，获取发送方和总支出金额
    for vin in decode_tx['vin']:
        if "txid" not in vin:
            senders.add("coinbase")
            total_spend += 325
        else:
            vin_txid = vin['txid']
            vin_vout = vin['vout']
            try:
                vin_decode = await syclic_call_rpc(method="getrawtransaction", params=[vin_txid, 1])
                value_spend = round(float(vin_decode['vout'][vin_vout].get('value', 0)) * 1_000_000)
                total_spend += value_spend
                
                if "scriptPubKey" in vin_decode['vout'][vin_vout]:
                    if vin_decode['vout'][vin_vout]['scriptPubKey'].get("type") == "pubkeyhash" and "addresses" in vin_decode['vout'][vin_vout]['scriptPubKey']:
                        for addr in vin_decode['vout'][vin_vout]['scriptPubKey']['addresses']:
                            senders.add(addr)
                            balance_changes[addr] = balance_changes.get(addr, 0) - value_spend
                    elif vin_decode['vout'][vin_vout]['scriptPubKey']['asm'].endswith("OP_CHECKMULTISIG"):
                        try:
                            from app.utils import convert_p2ms_script_to_ms_address
                            ms_address = convert_p2ms_script_to_ms_address(vin_decode['vout'][vin_vout]['scriptPubKey']['asm'])
                            senders.add(ms_address)
                            balance_changes[ms_address] = balance_changes.get(ms_address, 0) - value_spend
                        except (ValueError, ImportError):
                            pass
            except Exception as e:
                logging.error("处理交易输入 %s 时出错: %s", decode_txid, str(e))
    
    # 计算手续费
    fee = (total_spend - total_receive) / 1_000_000
    fee_str = f"{fee:f}".rstrip('0').rstrip('.')
    
    # 格式化时间
    utc_time = "unconfirmed" if block_height < 1 else datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    
    # 更新数据库
    await update_transaction_tables(conn, decode_txid, fee_str, timestamp, utc_time, tx_type, block_height, balance_changes, senders, receivers)


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


async def update_transaction_tables(conn, tx_hash, fee, timestamp, utc_time, tx_type, block_height, balance_changes, senders, receivers):
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
    INSERT INTO transactions (tx_hash, fee, time_stamp, utc_time, tx_type, block_height)
    VALUES (%s, %s, %s, %s, %s, %s) AS new
    ON DUPLICATE KEY UPDATE
        fee = new.fee,
        time_stamp = new.time_stamp,
        utc_time = new.utc_time,
        tx_type = new.tx_type,
        block_height = new.block_height,
        updated_at = CURRENT_TIMESTAMP
    """
    await DBManager.execute_update_nocommit(conn, transactions_insert_query, (
        tx_hash, fee, timestamp, utc_time, tx_type, block_height
    ))
    
    # 2. 处理地址交易关系
    for address, balance_change in balance_changes.items():
        is_sender = address in senders
        is_recipient = address in receivers
        
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
        await DBManager.execute_update_nocommit(conn, address_tx_insert_query, (
            address, tx_hash, is_sender, is_recipient, formatted_balance
        ))
    
    # 3. 处理交易参与方
    # 清除旧记录
    await DBManager.execute_update_nocommit(conn, "DELETE FROM transaction_participants WHERE tx_hash = %s", (tx_hash,))
    
    # 插入发送方
    for sender in senders:
        await DBManager.execute_update_nocommit(conn,
            "INSERT INTO transaction_participants (tx_hash, address, role) VALUES (%s, %s, %s)",
            (tx_hash, sender, "sender")
        )
    
    # 插入接收方
    for receiver in receivers:
        await DBManager.execute_update_nocommit(conn,
            "INSERT INTO transaction_participants (tx_hash, address, role) VALUES (%s, %s, %s)",
            (tx_hash, receiver, "recipient")
        )


async def process_tx_utxos(conn, decode_tx, timestamp, utxo_types=None):
    """
    处理交易中的各种UTXO（FT/NFT等）
    
    Args:
        decode_tx: 解码后的交易数据
        timestamp: 时间戳
        utxo_types: 每个输出的类型列表，如果为None则实时判断
    """
    output_index = 0
    
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
            new_output_index, _ = await process_nft_collections(conn, decode_tx, output_index, timestamp)
            output_index = new_output_index
        elif utxo_type == 'NFT':
            new_output_index, _ = await process_nft_utxo_set(conn, decode_tx, output_index, timestamp)
            output_index = new_output_index
        elif utxo_type == 'FT':
            new_output_index, ft_contract_id, vout_combine_script, ft_balance = await process_ft_tokens(conn, decode_tx, output_index, timestamp)
            output_index = new_output_index
            
            # 处理FT代币的UTXO集合
            spent_utxo_info = await process_ft_txo_set(conn, decode_tx, output_index, ft_contract_id, vout_combine_script, ft_balance)
            
            # 处理FT代币余额
            await process_ft_balance(conn, ft_contract_id, vout_combine_script, ft_balance, spent_utxo_info)
        else:
            output_index += 1


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


def find_new_transactions(current_mempool):
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
    
    return new_txs


async def process_transactions(conn, new_txs, if_catch_lastest, timestamp):
    """
    处理新交易
    
    Args:
        new_txs: 新交易列表
        if_catch_lastest: 是否已追上最新区块
        timestamp: 时间戳
    """
    global mempool
    global index_height
    
    for tx in new_txs:
        mempool.append(tx)
        block_height = index_height if not if_catch_lastest else -1
        await process_single_transaction(conn, tx, block_height, timestamp)


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

# 该方法在处理完毕某一height的tx后调用；所以，如果当前没有追赶到最新状态，在保存时需要把height + 1
async def save_build_status(conn, height, tx_pool, last_tx_pool):
    """保存构建状态"""
    sql = """
    UPDATE t_index_build_status 
    SET value = %s
    WHERE name = %s
    """
    tx_pool_json = json.dumps(tx_pool)
    last_tx_pool_json = json.dumps(last_tx_pool)
    await DBManager.execute_update_nocommit(conn, sql, (height, "index_height"))
    await DBManager.execute_update_nocommit(conn, sql, (tx_pool_json, "mempool"))
    await DBManager.execute_update_nocommit(conn, sql, (last_tx_pool_json, "last_mempool"))

    return

async def load_build_status():
    """加载构建状态"""
    sql = """
    SELECT value FROM t_index_build_status WHERE name = %s
    """
    index_height_res = await DBManager.execute_query(sql, ("index_height",))
    if int(index_height_res[0][0]) < first_index_height:
        raise Exception(f"数据库存储的构建状态异常, 数据库中索引高度为 {index_height_res[0][0]}")
    mempool_res = await DBManager.execute_query(sql, ("mempool",))
    last_mempool_res = await DBManager.execute_query(sql, ("last_mempool",))
    return int(index_height_res[0][0]), json.loads(mempool_res[0][0]), json.loads(last_mempool_res[0][0])

async def reset_build_status(conn):
    """重置构建状态"""
    sql = """
    UPDATE t_index_build_status 
    SET value = %s
    WHERE name = %s
    """
    await DBManager.execute_update_nocommit(conn, sql, (json.dumps([]), "mempool"))
    await DBManager.execute_update_nocommit(conn, sql, (json.dumps([]), "last_mempool"))
    await DBManager.execute_update_nocommit(conn, sql, (first_index_height, "index_height"))

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
        new_txs = find_new_transactions(current_mempool)
        async with DBManager._pool.acquire() as conn:
            await conn.begin()
            try:
                # 处理新交易
                await process_transactions(conn, new_txs, if_catch_lastest, timestamp)
                
                # 更新内存池状态
                update_mempool_state(if_catch_lastest)

                await save_build_status(conn, index_height, mempool, last_mempool)
            except Exception as e:
                await conn.rollback()
                raise e
            else:
                await conn.commit()
        
        return True
    except Exception as e:
        logging.error("P0 扫描区块链出错: %s", str(e))
        return False

async def clear_db():
    """
    清空数据库
    """
    clear_db_query = """
    SET FOREIGN_KEY_CHECKS = 0;
    TRUNCATE TABLE `ft_tokens`;
    TRUNCATE TABLE `ft_balance`;
    TRUNCATE TABLE `ft_txo_set`;
    TRUNCATE TABLE `nft_collections`;
    TRUNCATE TABLE `nft_utxo_set`;
    SET FOREIGN_KEY_CHECKS = 1;
    """
    async with DBManager._pool.acquire() as conn:
        await DBManager.execute_update_nocommit(conn, clear_db_query)
        await reset_build_status(conn) # 重置构建状态
        await conn.commit()

def schedule_task(task, is_clear_db=False):
    """
    Schedule task.
    """
    async def wrapper():
        await DBManager.init_pool(db="TBC20721")

        if is_clear_db:
            await clear_db()
            logging.info("清空数据库成功，开始从头构建索引")
        else:
            global index_height
            global mempool
            global last_mempool
            index_height, mempool, last_mempool = await load_build_status() # 加载构建状态
            logging.info("加载构建状态成功, 从索引高度为 %s 开始构建, mempool 长度为 %s, last_mempool 长度为 %s", index_height, len(mempool), len(last_mempool))

        while True:
            try:
                global index_interval
                await task()
                await asyncio.sleep(index_interval)
            except KeyboardInterrupt:
                logging.info("Interrupted by user")
                break
            except Exception as e:
                logging.error("P0 Uncatched Error: %s", str(e))
                await asyncio.sleep(10)
            
        await DBManager.close_pool()
    asyncio.run(wrapper())



async def clean_transactions_keep_minimum_per_user():
    """清理策略：确保每个用户至少保留30条最新交易记录"""
    try:
        # 查询所有地址
        address_query = "SELECT DISTINCT address FROM address_transactions"
        addresses = await DBManager.execute_query(address_query, ())

        # 收集每个地址需要保留的交易
        preserved_txs = set()

        for (address,) in addresses:
            # 获取该地址的最新30条交易
            recent_txs_query = """
            SELECT at.tx_hash FROM address_transactions at
            JOIN transactions t ON at.tx_hash = t.tx_hash
            WHERE at.address = %s
            ORDER BY t.time_stamp DESC LIMIT 30
            """
            recent_txs = await DBManager.execute_query(recent_txs_query, (address,))

            # 将这些交易加入保留列表
            for (tx_hash,) in recent_txs:
                preserved_txs.add(tx_hash)

        # 查询所有交易
        all_txs_query = "SELECT tx_hash FROM transactions"
        all_txs = await DBManager.execute_query(all_txs_query, ())

        # 找出可以删除的交易(不在任何用户的保留列表中)
        txs_to_delete = []
        for (tx_hash,) in all_txs:
            if tx_hash not in preserved_txs:
                txs_to_delete.append(tx_hash)

        # 删除这些交易的所有相关记录
        if txs_to_delete:
            placeholders = ','.join(['%s'] * len(txs_to_delete))

            # 删除相关的address_transactions记录
            addr_tx_del_query = f"DELETE FROM address_transactions WHERE tx_hash IN ({placeholders})"
            await DBManager.execute_update(addr_tx_del_query, tuple(txs_to_delete))

            # 删除相关的transaction_participants记录
            participants_del_query = f"DELETE FROM transaction_participants WHERE tx_hash IN ({placeholders})"
            await DBManager.execute_update(participants_del_query, tuple(txs_to_delete))

            # 删除transactions记录
            tx_del_query = f"DELETE FROM transactions WHERE tx_hash IN ({placeholders})"
            await DBManager.execute_update(tx_del_query, tuple(txs_to_delete))

        logging.info(f"已清理{len(txs_to_delete)}条交易记录，确保每个用户至少保留30条最新交易")
        current_time = datetime.now(timezone(timedelta(hours=8)))
        current_date = current_time.date()
        global last_cleanup_date
        last_cleanup_date = current_date
        logging.info(f"清理任务完成，上次清理日期为 {last_cleanup_date}")
    except Exception as e:
        logging.error("清理任务执行出错: %s", str(e))
    return

async def task_wrapper():
    """
    Task wrapper.
    """
    # 使用全局变量存储上次清理日期
    global last_cleanup_date
    
    # 获取当前北京时间（UTC+8）
    current_time = datetime.now(timezone(timedelta(hours=8)))
    current_date = current_time.date()
    
    # 检查是否到达凌晨3点且与上次清理不在同一天
    if (current_time.hour == 3 and current_time.minute < 15 and 
        (last_cleanup_date is None or current_date != last_cleanup_date)):
        logging.info("执行定时清理任务，时间: %s", current_time)
        # 用一个新的线程来完成清理任务
        asyncio.create_task(clean_transactions_keep_minimum_per_user())
    
    # 正常的区块链扫描任务
    await scan_chain_and_build_index()

if __name__ == "__main__":
    # Parse command line arguments
    args = parser.parse_args()
    
    # 定义全局变量
    last_cleanup_date = None
    
    # 每 15 秒调用一次 task_wrapper，传入命令行参数
    schedule_task(task_wrapper, is_clear_db=args.clear_db)
