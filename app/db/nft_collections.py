import logging
from app.dependencies import DBManager
from app.utils.utils import convert_str_to_sha256, hex_to_json
from app.s3 import upload_base64_image_to_s3
from app.utils.field_truncate import FieldTruncate

async def process_nft_collections(decode_tx, output_index, timestamp):
    """处理NFT集合信息并更新nft_collections表"""
    decode_txid = decode_tx["txid"]
    
    if not (decode_tx["vout"][output_index]["scriptPubKey"]["asm"].startswith("0 OP_RETURN") or 
            decode_tx["vout"][output_index]["scriptPubKey"]["asm"].startswith("OP_RETURN")):
        return output_index, None, False
        
    logging.info("TBC721 Collection:      %s", decode_txid)

    if len(decode_tx["vout"]) - output_index <= 1:
        logging.error("Error Collection Protocal: %s", decode_txid)
        return output_index, None, True
    if decode_tx["vout"][output_index + 1]["scriptPubKey"]["type"] != "pubkeyhash":
        logging.error("Error Collection Protocal: %s", decode_txid)
        return output_index, None, True

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
        return output_index, None, True
        
    collection_name = collection_tape_json.get("collectionName", "")
    collection_symbol = collection_tape_json.get("symbol", "")
    collection_attributes = collection_tape_json.get("attributes", "")
    collection_description = collection_tape_json.get("description", "")
    collection_supply = collection_tape_json.get("supply", 0)
    
    if collection_supply <= 0:
        logging.error("Wrong Collection supply input: %s", decode_txid)
        return output_index, None, True
    
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

    # 准备集合插入数据
    collection_data = {
        'collection_id': collection_id,
        'collection_name': collection_name,
        'collection_creator_address': collection_creator_address,
        'collection_creator_script_hash': collection_creator_script_hash,
        'collection_symbol': collection_symbol,
        'collection_attributes': collection_attributes,
        'collection_description': collection_description,
        'collection_supply': collection_supply,
        'collection_create_timestamp': collection_create_timestamp,
        'collection_icon': collection_icon
    }
    
    # 截断字段
    collection_data = FieldTruncate.truncate_fields(collection_data)
    
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
        await DBManager.execute_update(nft_collection_insert_query, tuple(collection_data.values()))
        return output_index + collection_supply, collection_id, False
    except Exception as e:
        logging.error("Error inserting collection %s: %s", decode_txid, e)
        return output_index, None, True
