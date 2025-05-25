import logging
from app.dependencies import DBManager
from app.utils import convert_str_to_sha256, hex_to_json
from app.s3 import upload_base64_image_to_s3



async def process_nft_utxo_set(decode_tx, output_index, timestamp):
    """处理NFT信息并更新nft_utxo_set表"""
    decode_txid = decode_tx["txid"]
    
    if not (decode_tx["vout"][output_index]["scriptPubKey"]["asm"].startswith("1 OP_PICK 3 OP_SPLIT") or 
            decode_tx["vout"][output_index]["scriptPubKey"]["asm"].startswith("4 OP_PICK OP_BIN2NUM OP_TOALTSTACK 1 OP_PICK 3 OP_SPLIT")):
        return output_index, None, False
    
    nft_tape_json = {}
    
    if decode_tx["vout"][output_index]["scriptPubKey"]["asm"].startswith("1 OP_PICK 3 OP_SPLIT 20"):
        logging.info("TBC721 NFT:             %s", decode_txid)

        if len(decode_tx["vout"]) - output_index <= 2:
            logging.error("Error NFT Protocal: %s", decode_txid)
            return output_index, None, True

        # 解码 tape json
        if decode_tx['vout'][output_index + 2]['scriptPubKey']['asm'].startswith("0 OP_RETURN"):
            nft_tape_hex = decode_tx['vout'][output_index + 2]['scriptPubKey']['asm'][12:-11]
        elif decode_tx['vout'][output_index + 2]['scriptPubKey']['asm'].startswith("OP_RETURN"):
            nft_tape_hex = decode_tx['vout'][output_index + 2]['scriptPubKey']['asm'][10:-10]
        else:
            logging.error("Error decoding nft scriptPubKey asm %s", decode_txid)
            return output_index, None, True
        
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
            return output_index, None, True
            
        nft_tape_hex = "POOLNFT"
        pool_tape_list = decode_tx["vout"][output_index + 1]["scriptPubKey"]["asm"].split(" ")
        token_pair_a_id = pool_tape_list[4]
        nft_tape_json = {"file": token_pair_a_id}
        nft_offset = 2
        
    else:
        logging.error("Error decoding nft scriptPubKey asm %s", decode_txid)
        return output_index, None, True
    
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
                return output_index, None, True
        else:
            nft_file = nft_tape_json.get("file", "")
            if len(nft_file) != 72:
                logging.error("Error NFT Transfer Tape file: %s", decode_txid)
                return output_index, None, True
            nft_contract_id = nft_file[:64]

        nft_update_query = """
        UPDATE nft_utxo_set
        SET nft_utxo_id = %s, nft_code_balance = %s, nft_p2pkh_balance = %s, nft_holder_address = %s, nft_holder_script_hash = %s, nft_last_transfer_timestamp = %s, nft_transfer_time_count = nft_transfer_time_count + 1
        WHERE nft_contract_id = %s
        """
        try:
            await DBManager.execute_update(nft_update_query, (decode_txid, nft_code_balance, nft_p2pkh_balance, nft_holder_address, nft_holder_script_hash, timestamp, nft_contract_id))
        except Exception as e:
            logging.error("Error updating NFT transfer %s: %s", decode_txid, e)
            return output_index, None, True
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
        try:
            await DBManager.execute_update(nft_utxo_set_insert_query, (nft_contract_id, collection_id, collection_index, collection_name, nft_utxo_id, nft_code_balance, nft_p2pkh_balance, nft_name, nft_symbol, nft_attributes, nft_description, nft_transfer_time_count, nft_holder_address, nft_holder_script_hash, nft_create_timestamp, nft_last_transfer_timestamp, nft_icon))
        except Exception as e:
            logging.error("Error inserting NFT %s: %s", decode_txid, e)
            return output_index, None, True
    
    return output_index + nft_offset, nft_contract_id, False
