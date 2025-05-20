import asyncio
import json
import time
import logging
from app.dependencies import call_node_rpc
from app.dependencies import DBManager
from app.utils import hex_to_json, convert_str_to_sha256

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 定义并初始化全局变量
index_height = 862600
index_interval = 0
mempool = []
last_mempool = []

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


async def scan_chain_and_build_index():
    """
    This function will scan the chain and build the index.
    """
    # init variables
    global index_interval
    global index_height
    global mempool
    global last_mempool
    if_catch_lastest = False

    # if current height is higher or equal, index_height plus one and tag.
    block_count_res = await syclic_call_rpc(method="getblockcount", params=[])
    if block_count_res < index_height:
        if_catch_lastest = True
        index_interval = 2

    logging.info("Scanning chain and building index... index_height: %s, block_count_res: %s, if_catch_lastest: %s", index_height, block_count_res, if_catch_lastest)

    # update mempool and find new transactions.
    if if_catch_lastest:
        current_mempool = await syclic_call_rpc(method="getrawmempool", params=[])
        timestamp = int(time.time())
    else:
        get_block_res = await syclic_call_rpc(method="getblockbyheight", params=[index_height, 1])
        current_mempool = get_block_res["tx"]
        timestamp = get_block_res["time"]

    nearly_mempool = mempool + last_mempool
    new_txs = [tx for tx in current_mempool if tx not in nearly_mempool]

    async with DBManager._pool.acquire() as conn:
        await conn.begin()
        try:
            # build index for new transactions.
            for tx in new_txs:
                mempool.append(tx)

                # decode raw transaction
                decode_tx = await syclic_call_rpc(method="getrawtransaction", params=[tx, 1])
                decode_txid = decode_tx["txid"]

                # Read black list from file
                try:
                    with open('black_list.txt', 'r', encoding='utf-8') as file:
                        black_list = [line.strip() for line in file]
                except FileNotFoundError:
                    black_list = []
                if decode_txid in black_list:
                    continue

                # build index for new utxo and FT/Collection info.
                output_index = 0
                while output_index < len(decode_tx["vout"]):

                    # TBC721 Collection utxo.
                    if decode_tx["vout"][output_index]["scriptPubKey"]["asm"].startswith("0 OP_RETURN") or decode_tx["vout"][output_index]["scriptPubKey"]["asm"].startswith("OP_RETURN"):
                        logging.info("TBC721 Collection:      %s", decode_txid)

                        if len(decode_tx["vout"]) - output_index <= 1:
                            logging.error("Error Collection Protocal: %s", decode_txid)
                            break
                        if decode_tx["vout"][output_index + 1]["scriptPubKey"]["type"] != "pubkeyhash":
                            logging.error("Error Collection Protocal: %s", decode_txid)
                            break

                        # prepare collection insert data
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
                            break
                        collection_name = collection_tape_json.get("collectionName", "")[:64]
                        collection_symbol = collection_tape_json.get("symbol", "")[:64]
                        collection_attributes = collection_tape_json.get("attributes", "")
                        collection_description = collection_tape_json.get("description", "")
                        collection_supply = collection_tape_json.get("supply", 0)
                        if collection_supply <= 0:
                            logging.error("Wrong Collection supply input: %s", decode_txid)
                            break
                        collection_icon = collection_tape_json.get("file", "")

                        # insert record into table nft_collections
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
                        except Exception as e:
                            logging.error("Error inserting collection %s: %s", decode_txid, e)
                            break
                        output_index += collection_supply

                    # TBC721 NFT utxo.
                    elif decode_tx["vout"][output_index]["scriptPubKey"]["asm"].startswith("1 OP_PICK 3 OP_SPLIT") or decode_tx["vout"][output_index]["scriptPubKey"]["asm"].startswith("4 OP_PICK OP_BIN2NUM OP_TOALTSTACK 1 OP_PICK 3 OP_SPLIT"):
                        if decode_tx["vout"][output_index]["scriptPubKey"]["asm"].startswith("1 OP_PICK 3 OP_SPLIT 20"):
                            logging.info("TBC721 NFT:             %s", decode_txid)

                            if len(decode_tx["vout"]) - output_index <= 2:
                                logging.error("Error NFT Protocal: %s", decode_txid)
                                break

                            # decode tape json
                            if decode_tx['vout'][output_index + 2]['scriptPubKey']['asm'].startswith("0 OP_RETURN"):
                                nft_tape_hex = decode_tx['vout'][output_index + 2]['scriptPubKey']['asm'][12:-11]
                            elif decode_tx['vout'][output_index + 2]['scriptPubKey']['asm'].startswith("OP_RETURN"):
                                nft_tape_hex = decode_tx['vout'][output_index + 2]['scriptPubKey']['asm'][10:-10]
                            else:
                                logging.error("Error decoding nft scriptPubKey asm %s", decode_txid)
                                break
                            try:
                                nft_tape_json = hex_to_json(nft_tape_hex)
                            except ValueError:
                                logging.error("Error decoding NFT tape %s", decode_txid)
                                nft_tape_json = {}
                        elif decode_tx["vout"][output_index]["scriptPubKey"]["asm"].startswith("1 OP_PICK 3 OP_SPLIT OP_NIP") or decode_tx["vout"][output_index]["scriptPubKey"]["asm"].startswith("4 OP_PICK OP_BIN2NUM OP_TOALTSTACK 1 OP_PICK 3 OP_SPLIT"):
                            logging.info("Pool NFT:               %s", decode_txid)
                            if len(decode_tx["vout"]) - output_index <= 1:
                                logging.error("Error Pool NFT Protocal: %s", decode_txid)
                                break
                            nft_tape_hex = "POOLNFT"
                            pool_tape_list = decode_tx["vout"][output_index + 1]["scriptPubKey"]["asm"].split(" ")
                            token_pair_a_id = pool_tape_list[4]
                            nft_tape_json = {"file": token_pair_a_id}
                        else:
                            logging.error("Error decoding nft scriptPubKey asm %s", decode_txid)
                            break
                        
                        # prepare nft insert data
                        nft_code_balance = round(decode_tx["vout"][output_index]["value"] * 1_000_000)
                        nft_p2pkh_balance = round(decode_tx["vout"][output_index + 1]["value"] * 1_000_000)
                        nft_holder_address = decode_tx["vout"][output_index + 1]["scriptPubKey"]["addresses"][0] if "addresses" in decode_tx["vout"][output_index + 1]["scriptPubKey"] else "LP"
                        nft_holder_script_hash = convert_str_to_sha256(decode_tx["vout"][output_index + 1]["scriptPubKey"]["hex"])

                        # determine if it is the first mint.
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
                                    break
                            else:
                                nft_file = nft_tape_json.get("file", "")
                                if len(nft_file) != 72:
                                    logging.error("Error NFT Transfer Tape file: %s", decode_txid)
                                    break
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

                            # if mint from collection, get collection_id and collection_index
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
                            
                            # insert record into table nft_utxo_set
                            nft_contract_id = decode_txid
                            nft_utxo_id = decode_txid
                            nft_name = nft_tape_json.get("nftName", "")[:64]
                            nft_symbol = nft_tape_json.get("symbol", "")[:64]
                            nft_attributes = nft_tape_json.get("attributes", "")
                            nft_description = nft_tape_json.get("description", "")
                            nft_transfer_time_count = 0
                            nft_create_timestamp = timestamp
                            nft_last_transfer_timestamp = timestamp
                            nft_icon = nft_tape_json.get("file", "")
                            if len(nft_icon) == 72:
                                nft_icon = collection_icon
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
                            await DBManager.execute_update_nocommit(conn, nft_utxo_set_insert_query, (nft_contract_id, collection_id, collection_index , collection_name, nft_utxo_id, nft_code_balance, nft_p2pkh_balance, nft_name, nft_symbol, nft_attributes, nft_description, nft_transfer_time_count, nft_holder_address, nft_holder_script_hash, nft_create_timestamp, nft_last_transfer_timestamp, nft_icon))
                        if decode_tx["vout"][output_index]["scriptPubKey"]["asm"].startswith("1 OP_PICK 3 OP_SPLIT 20"):
                            output_index += 3
                        elif decode_tx["vout"][output_index]["scriptPubKey"]["asm"].startswith("1 OP_PICK 3 OP_SPLIT OP_NIP") or decode_tx["vout"][output_index]["scriptPubKey"]["asm"].startswith("4 OP_PICK OP_BIN2NUM OP_TOALTSTACK 1 OP_PICK 3 OP_SPLIT"):
                            output_index += 2
                        else:
                            break

                    # TBC20 FT utxo.
                    elif decode_tx["vout"][output_index]["scriptPubKey"]["asm"].startswith("9 OP_PICK OP_TOALTSTACK"):

                        if len(decode_tx["vout"]) - output_index <= 1:
                            logging.error("Error FT Protocal: %s", decode_txid)
                            break

                        # exclude wrong version TBC20
                        if decode_tx["vout"][output_index]["scriptPubKey"]["asm"][-32:-11] == "OP_CHECKSIG OP_RETURN":
                            break
                        
                        vout_combine_script = decode_tx["vout"][output_index]["scriptPubKey"]["hex"][-54:-12]
                        ft_balance = 0
                        ft_balance_tape = decode_tx["vout"][output_index + 1]["scriptPubKey"]["asm"][12:108]
                        for i in range(0, len(ft_balance_tape), 16):
                            segment = ft_balance_tape[i:i+16]
                            segment = ''.join([segment[i:i+2] for i in range(0, len(segment), 2)][::-1])
                            ft_balance += int(segment, 16)
                        vout_utxo_balance = round(decode_tx["vout"][output_index]["value"] * 1_000_000)
                        if_spend = 0
                        # Determine if it is the first mint.
                        ft_origin_utxo = decode_tx["vout"][output_index]["scriptPubKey"]["asm"][2384:2456]
                        # Handle new verison token protocol.
                        if ft_origin_utxo == "P OP_EQUAL OP_IF OP_FROMALTSTACK OP_DROP OP_TOALTSTACK OP_TOALTSTACK OP_":
                            ft_origin_utxo = ft_origin_utxo = decode_tx["vout"][output_index]["scriptPubKey"]["asm"][2477:2549]
                        # Handle with LP output.
                        if ft_origin_utxo == "UALVERIFY OP_ENDIF OP_DUP 2 OP_EQUAL OP_IF OP_DROP 2 OP_PICK 2 OP_PICK O":
                            ft_origin_utxo = "LP"
                        ft_tokens_query = """
                        SELECT ft_contract_id FROM ft_tokens WHERE ft_origin_utxo = %s
                        """
                        ft_tokens_query_res = await DBManager.execute_query(ft_tokens_query, (ft_origin_utxo,))
                        # Transfer FT
                        if ft_tokens_query_res:
                            logging.info("FT Transfer:            %s", decode_txid)
                            ft_contract_id = ft_tokens_query_res[0][0]
                        # Mint FT
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
                            ft_name = bytes.fromhex(ft_tape_info[2:2+ft_name_len*2]).decode('utf-8')[:64]
                            ft_symbol_len = int(ft_tape_info[2+ft_name_len*2:4+ft_name_len*2], 16)
                            ft_symbol = bytes.fromhex(ft_tape_info[4+ft_name_len*2:4+ft_name_len*2+ft_symbol_len*2]).decode('utf-8')[:64]
                            
                            # insert record into table ft_tokens
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
                            
                        # insert record into table ft_txo_set
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
                        await DBManager.execute_update_nocommit(conn, ft_utxo_set_insert_query, (decode_txid, output_index, vout_combine_script, ft_contract_id, vout_utxo_balance, ft_balance, if_spend))

                        # if ft_balance record not exist, insert record into table ft_balance
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

                            # ft_holders_count increase
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

                        output_index += 2

                    else:
                        output_index += 1
                        continue

                # update index for utxo.
                for vin in decode_tx["vin"]:
                    if "scriptSig" in vin and vin["scriptSig"]["asm"].startswith("1 "):
                        ft_txo_query = """
                        SELECT ft_contract_id, ft_holder_combine_script, ft_balance
                        FROM ft_txo_set
                        WHERE utxo_txid = %s AND utxo_vout = %s
                        """
                        ft_txo_query_res = await DBManager.execute_query(ft_txo_query, (vin["txid"], vin["vout"]))
                        if ft_txo_query_res:
                            # update ft_txo_set
                            ft_utxo_update_query = """
                            UPDATE ft_txo_set
                            SET if_spend = 1
                            WHERE utxo_txid = %s AND utxo_vout = %s
                            """
                            await DBManager.execute_update_nocommit(conn, ft_utxo_update_query, (vin["txid"], vin["vout"]))

                            # update ft_balance
                            ft_contract_id, ft_holder_combine_script, ft_balance = ft_txo_query_res[0]

                            ft_balance_query = """
                            SELECT ft_balance
                            FROM ft_balance
                            WHERE ft_contract_id = %s AND ft_holder_combine_script = %s
                            """
                            ft_balance_query_res = await DBManager.execute_query(ft_balance_query, (ft_contract_id, ft_holder_combine_script))
                            if ft_balance_query_res:
                                ft_balance_balance = ft_balance_query_res[0][0]
                                # if ft_balance.ft_balance equals to ft_balance, delete record and ft_tokens.ft_holders_count - 1
                                if ft_balance_balance == ft_balance:
                                    ft_balance_delete_query = """
                                    DELETE FROM ft_balance 
                                    WHERE ft_holder_combine_script = %s 
                                    AND ft_contract_id = %s 
                                    """
                                    await DBManager.execute_update_nocommit(conn, ft_balance_delete_query, (ft_holder_combine_script, ft_contract_id))
                                    ft_tokens_update = """
                                    UPDATE ft_tokens
                                    SET ft_holders_count = ft_holders_count - 1
                                    WHERE ft_contract_id = %s
                                    """
                                    await DBManager.execute_update_nocommit(conn, ft_tokens_update, (ft_contract_id,))
                                elif ft_balance_balance > ft_balance:
                                    ft_balance_update_query = """
                                    UPDATE ft_balance
                                    SET ft_balance = ft_balance - %s
                                    WHERE ft_holder_combine_script = %s 
                                    AND ft_contract_id = %s 
                                    """                            
                                    await DBManager.execute_update_nocommit(conn, ft_balance_update_query, (ft_balance, ft_holder_combine_script, ft_contract_id))
        except Exception as e:
            # await conn.rollback()
            logging.error("构建高度: %s 发生异常, 错误信息: %s", index_height, str(e), exc_info=True)
            exit(1)
        finally:
            try:
                if not if_catch_lastest:
                    await save_build_status(conn, index_height + 1, [], mempool)
                else:
                    await save_build_status(conn, index_height, mempool, last_mempool)
            except Exception as e:
                logging.error("保存构建状态发生异常, 错误信息: %s", str(e), exc_info=True)
                exit(1)
            await conn.commit()

        # if current height is higher, clear mempool after building index.
        if not if_catch_lastest:
            index_height += 1
            last_mempool = mempool
            mempool = []

    return

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
    if int(index_height_res[0][0]) < 862600:
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
    await DBManager.execute_update_nocommit(conn, sql, (862600, "index_height"))

if __name__ == "__main__":
    async def task_wrapper():
        """
        Task wrapper.
        """
        await scan_chain_and_build_index()

    # 每 15 秒调用一次 task_wrapper
    schedule_task(task_wrapper, is_clear_db=True)
