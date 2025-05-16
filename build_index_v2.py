import asyncio
import time
import logging
import os
import base64
import tempfile
from app.dependencies import call_node_rpc
from app.dependencies import DBManager
from app.utils import hex_to_json, convert_str_to_sha256
from app.s3 import S3Uploader

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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


async def process_nft_collections(decode_tx, output_index, timestamp):
    """处理NFT集合信息并更新nft_collections表"""
    decode_txid = decode_tx["txid"]
    
    if not (decode_tx["vout"][output_index]["scriptPubKey"]["asm"].startswith("0 OP_RETURN") or 
            decode_tx["vout"][output_index]["scriptPubKey"]["asm"].startswith("OP_RETURN")):
        return output_index, None
        
    logging.info("TBC721 Collection:      %s", decode_txid)

    if len(decode_tx["vout"]) - output_index <= 1:
        logging.error("Error Collection Protocal: %s", decode_txid)
        return output_index, None
    if decode_tx["vout"][output_index + 1]["scriptPubKey"]["type"] != "pubkeyhash":
        logging.error("Error Collection Protocal: %s", decode_txid)
        return output_index, None

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
        return output_index, None
        
    collection_name = collection_tape_json.get("collectionName", "")
    collection_symbol = collection_tape_json.get("symbol", "")
    collection_attributes = collection_tape_json.get("attributes", "")
    collection_description = collection_tape_json.get("description", "")
    collection_supply = collection_tape_json.get("supply", 0)
    
    if collection_supply <= 0:
        logging.error("Wrong Collection supply input: %s", decode_txid)
        return output_index, None
    
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
        await DBManager.execute_update(nft_collection_insert_query, (collection_id, collection_name, collection_creator_address, collection_creator_script_hash, collection_symbol, collection_attributes, collection_description, collection_supply, collection_create_timestamp, collection_icon))
        return output_index + collection_supply, collection_id
    except Exception as e:
        logging.error("Error inserting collection %s: %s", decode_txid, e)
        return output_index, None


async def process_nft_utxo_set(decode_tx, output_index, timestamp):
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
            return output_index, None

        # 解码 tape json
        if decode_tx['vout'][output_index + 2]['scriptPubKey']['asm'].startswith("0 OP_RETURN"):
            nft_tape_hex = decode_tx['vout'][output_index + 2]['scriptPubKey']['asm'][12:-11]
        elif decode_tx['vout'][output_index + 2]['scriptPubKey']['asm'].startswith("OP_RETURN"):
            nft_tape_hex = decode_tx['vout'][output_index + 2]['scriptPubKey']['asm'][10:-10]
        else:
            logging.error("Error decoding nft scriptPubKey asm %s", decode_txid)
            return output_index, None
        
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
            return output_index, None
            
        nft_tape_hex = "POOLNFT"
        pool_tape_list = decode_tx["vout"][output_index + 1]["scriptPubKey"]["asm"].split(" ")
        token_pair_a_id = pool_tape_list[4]
        nft_tape_json = {"file": token_pair_a_id}
        nft_offset = 2
        
    else:
        logging.error("Error decoding nft scriptPubKey asm %s", decode_txid)
        return output_index, None
    
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
                return output_index, None
        else:
            nft_file = nft_tape_json.get("file", "")
            if len(nft_file) != 72:
                logging.error("Error NFT Transfer Tape file: %s", decode_txid)
                return output_index, None
            nft_contract_id = nft_file[:64]

        nft_update_query = """
        UPDATE nft_utxo_set
        SET nft_utxo_id = %s, nft_code_balance = %s, nft_p2pkh_balance = %s, nft_holder_address = %s, nft_holder_script_hash = %s, nft_last_transfer_timestamp = %s, nft_transfer_time_count = nft_transfer_time_count + 1
        WHERE nft_contract_id = %s
        """
        await DBManager.execute_update(nft_update_query, (decode_txid, nft_code_balance, nft_p2pkh_balance, nft_holder_address, nft_holder_script_hash, timestamp, nft_contract_id))
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
        await DBManager.execute_update(nft_utxo_set_insert_query, (nft_contract_id, collection_id, collection_index, collection_name, nft_utxo_id, nft_code_balance, nft_p2pkh_balance, nft_name, nft_symbol, nft_attributes, nft_description, nft_transfer_time_count, nft_holder_address, nft_holder_script_hash, nft_create_timestamp, nft_last_transfer_timestamp, nft_icon))
    
    return output_index + nft_offset, nft_contract_id


async def process_ft_tokens(decode_tx, output_index, timestamp):
    """处理同质化代币信息并更新ft_tokens表"""
    decode_txid = decode_tx["txid"]
    
    if not decode_tx["vout"][output_index]["scriptPubKey"]["asm"].startswith("9 OP_PICK OP_TOALTSTACK"):
        return output_index, None, None, None
    
    if len(decode_tx["vout"]) - output_index <= 1:
        logging.error("Error FT Protocal: %s", decode_txid)
        return output_index, None, None, None

    # 排除错误版本的 TBC20
    if decode_tx["vout"][output_index]["scriptPubKey"]["asm"][-32:-11] == "OP_CHECKSIG OP_RETURN":
        return output_index, None, None, None
    
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
        await DBManager.execute_update(ft_token_insert_query, (ft_contract_id, ft_code_script, ft_tape_script, ft_supply, ft_decimal, ft_name, ft_symbol, ft_description, ft_origin_utxo, ft_creator_combine_script, ft_holders_count, ft_icon_url, ft_create_timestamp, ft_token_price))
        
    return output_index + 2, ft_contract_id, vout_combine_script, ft_balance


async def process_ft_txo_set(decode_tx, output_index, ft_contract_id, vout_combine_script, ft_balance):
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
    await DBManager.execute_update(ft_utxo_set_insert_query, (decode_txid, output_index - 2, vout_combine_script, ft_contract_id, vout_utxo_balance, ft_balance, if_spend))
    
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
                await DBManager.execute_update(ft_utxo_update_query, (vin["txid"], vin["vout"]))
                
                # 返回已花费的UTXO信息，供ft_balance处理函数使用
                return ft_txo_query_res[0]
    
    return None


async def process_ft_balance(ft_contract_id, vout_combine_script, ft_balance, spent_utxo_info=None):
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


async def scan_chain_and_build_index():
    """
    扫描区块链并构建索引
    """
    # 初始化变量
    global index_interval
    global index_height
    global mempool
    global last_mempool
    if_catch_lastest = False

    # 如果当前高度更高或相等，index_height加一并标记
    block_count_res = await syclic_call_rpc(method="getblockcount", params=[])
    if block_count_res < index_height:
        if_catch_lastest = True
        index_interval = 2

    logging.info("Scanning chain and building index... index_height: %s, block_count_res: %s, if_catch_lastest: %s", index_height, block_count_res, if_catch_lastest)

    # 更新内存池并寻找新交易
    if if_catch_lastest:
        current_mempool = await syclic_call_rpc(method="getrawmempool", params=[])
        timestamp = int(time.time())
    else:
        get_block_res = await syclic_call_rpc(method="getblockbyheight", params=[index_height, 1])
        current_mempool = get_block_res["tx"]
        timestamp = get_block_res["time"]

    nearly_mempool = mempool + last_mempool
    new_txs = [tx for tx in current_mempool if tx not in nearly_mempool]

    # 为新交易构建索引
    for tx in new_txs:
        mempool.append(tx)

        # 解码原始交易
        decode_tx = await syclic_call_rpc(method="getrawtransaction", params=[tx, 1])
        decode_txid = decode_tx["txid"]

        # 从文件读取黑名单
        try:
            with open('black_list.txt', 'r', encoding='utf-8') as file:
                black_list = [line.strip() for line in file]
        except FileNotFoundError:
            black_list = []
        if decode_txid in black_list:
            continue

        # 为新UTXO和FT/Collection信息构建索引
        output_index = 0
        while output_index < len(decode_tx["vout"]):
            # 处理各种类型的UTXO
            if decode_tx["vout"][output_index]["scriptPubKey"]["asm"].startswith("0 OP_RETURN") or decode_tx["vout"][output_index]["scriptPubKey"]["asm"].startswith("OP_RETURN"):
                # 处理NFT集合
                new_output_index, _ = await process_nft_collections(decode_tx, output_index, timestamp)
                output_index = new_output_index
            elif decode_tx["vout"][output_index]["scriptPubKey"]["asm"].startswith("1 OP_PICK 3 OP_SPLIT") or decode_tx["vout"][output_index]["scriptPubKey"]["asm"].startswith("4 OP_PICK OP_BIN2NUM OP_TOALTSTACK 1 OP_PICK 3 OP_SPLIT"):
                # 处理NFT
                new_output_index, _ = await process_nft_utxo_set(decode_tx, output_index, timestamp)
                output_index = new_output_index
            elif decode_tx["vout"][output_index]["scriptPubKey"]["asm"].startswith("9 OP_PICK OP_TOALTSTACK"):
                # 处理FT代币
                new_output_index, ft_contract_id, vout_combine_script, ft_balance = await process_ft_tokens(decode_tx, output_index, timestamp)
                output_index = new_output_index
                
                # 处理FT代币的UTXO集合
                spent_utxo_info = await process_ft_txo_set(decode_tx, output_index, ft_contract_id, vout_combine_script, ft_balance)
                
                # 处理FT代币余额
                await process_ft_balance(ft_contract_id, vout_combine_script, ft_balance, spent_utxo_info)
            else:
                output_index += 1
                continue

    # 如果当前高度更高，在建立索引后清除内存池
    if not if_catch_lastest:
        index_height += 1
        last_mempool = mempool
        mempool = []
        
    return


if __name__ == "__main__":
    async def task_wrapper():
        """
        Task wrapper.
        """
        await scan_chain_and_build_index()

    # 每 15 秒调用一次 task_wrapper
    schedule_task(task_wrapper)
