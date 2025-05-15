"""
Utility functions for the API.
"""
import binascii
import hashlib
import json
from fastapi import HTTPException

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