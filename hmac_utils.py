"""
HMAC Utilities for Provably Fair RNG
"""
import hashlib
import hmac
import secrets
from typing import List

class HMACRNG:
    def __init__(self):
        self.server_seeds = {}
        
    def generate_server_seed(self) -> str:
        """Generate cryptographically secure server seed"""
        return secrets.token_hex(32)
    
    def get_commitment(self, server_seed: str) -> str:
        """Get commitment hash for server seed"""
        return hashlib.sha256(server_seed.encode()).hexdigest()
    
    def generate_digits_hmac(self, server_seed: str, round_id: str, client_seed: str = "") -> List[int]:
        """
        Generate 6 digits using HMAC-SHA256 with rejection sampling to avoid bias
        """
        message = f"{round_id}{client_seed}".encode()
        key = server_seed.encode()
        
        digits = []
        counter = 0
        
        while len(digits) < 6:
            # Generate HMAC with counter to get more bytes if needed
            hmac_msg = message + counter.to_bytes(4, 'big')
            mac = hmac.new(key, hmac_msg, hashlib.sha256).digest()
            
            # Process each byte with rejection sampling
            for byte in mac:
                if len(digits) >= 6:
                    break
                    
                # Rejection sampling: only accept bytes 0-249 for uniform distribution
                if byte < 250:
                    digit = byte % 10
                    digits.append(digit)
            
            counter += 1
        
        return digits
    
    def verify_round(self, server_seed: str, round_id: str, expected_digits: List[int], client_seed: str = "") -> bool:
        """Verify round results"""
        computed_digits = self.generate_digits_hmac(server_seed, round_id, client_seed)
        return computed_digits == expected_digits

def bytes_to_digits_unbiased(byte_array: bytes, num_digits: int = 6) -> List[int]:
    """
    Convert bytes to digits using rejection sampling to avoid modulo bias
    """
    digits = []
    index = 0
    extended_data = byte_array
    
    while len(digits) < num_digits:
        if index >= len(extended_data):
            extended_data = hashlib.sha256(extended_data).digest()
            index = 0
            
        byte_val = extended_data[index]
        index += 1
        
        # Rejection sampling: only accept bytes 0-249
        if byte_val < 250:
            digit = byte_val % 10
            digits.append(digit)
    
    return digits
