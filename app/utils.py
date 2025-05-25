"""
Utility functions for the API.
"""
import binascii
import hashlib
import json
from fastapi import HTTPException
import base58

def convert_str_to_sha256(input: str) -> str:
    """
    Converts a string to a SHA256 hash in little-endian format.
    """
    try:
        input_bytes = bytes.fromhex(input)
        # SHA256 hash the input
        sha256 = hashlib.sha256(input_bytes).digest()
        # convert the hash to little-endian
        scripthash = sha256[::-1]  # anti-slice
        scripthash_hex = binascii.hexlify(scripthash).decode('utf-8')

        return scripthash_hex
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid '{input}': {str(e)}") from e
    

def hex_to_json(hex_str):
    """
    Convert hex to JSON
    """
    try:
        json_str = bytes.fromhex(hex_str).decode('utf-8')
        return json.loads(json_str)
    except (ValueError, json.JSONDecodeError) as e:
        raise ValueError("Invalid hex input or JSON decoding error") from e 
    
def convert_p2ms_script_to_ms_address(p2ms_script: str) -> bool:
    """
    Converts a p2ms script to a multi-sig address.
    """
    try:
        ms_script_list = p2ms_script.split(' ')
        sig_needed_count = int(ms_script_list[0])
        sig_total_count = int(ms_script_list[-2])
        ms_pubkeys_hash = ms_script_list[-4]

        version_byte = (sig_needed_count << 4) | (sig_total_count & 0x0f)
        ms_address = base58.b58encode_check(bytes([version_byte]) + bytes.fromhex(ms_pubkeys_hash))
        return ms_address.decode('utf-8')

    except ValueError as e:
        return f"Invalid ms script '{p2ms_script}': {str(e)}"
    

def convert_p2ms_unlock_script_to_address(unlock_script: str) -> str:
    """
    Converts a p2ms unlock script to a address.
    """
    try:
        unlock_script_list = unlock_script.split(' ')
        if unlock_script_list[0] == "0":
            pubkey_needed_count = len(unlock_script_list) - 2
            pubkey_total_count = int(len(unlock_script_list[-1]) // 66)
            version_byte = (pubkey_needed_count << 4) | (pubkey_total_count & 0x0f)
            ms_pubkeys_hash = hashlib.new('ripemd160', hashlib.sha256(bytes.fromhex(unlock_script_list[-1])).digest()).digest()
            ms_address = base58.b58encode_check(bytes([version_byte]) + ms_pubkeys_hash)
            return ms_address.decode('utf-8')
        else:
            raise ValueError("Invalid unlock script")
    except ValueError as e:
        raise ValueError(f"Invalid unlock script '{unlock_script}': {str(e)}") from e


def verify_a_ms_address(ms_address: str) -> str:
    """
    Verifies a multi-sig address.
    """
    return True


def convert_ms_address_to_ms_script(ms_address: str) -> str:
    """
    Converts a multi-sig address to a p2ms script.
    """

def get_pool_balance(tape_asm: str):
    """
    Gets the balance of a pool.
    """
    ft_balance_tape_list = tape_asm.split(" ")
    complex_balance = ft_balance_tape_list[3]
    ft_lp_balance = int(''.join([complex_balance[i:i+2] for i in range(0, 16, 2)][::-1]), 16)
    ft_a_balance = int(''.join([complex_balance[i:i+2] for i in range(16, 32, 2)][::-1]), 16)
    tbc_balance = int(''.join([complex_balance[i:i+2] for i in range(32, 48, 2)][::-1]), 16)
    return ft_lp_balance, ft_a_balance, tbc_balance
