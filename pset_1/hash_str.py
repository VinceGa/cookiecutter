from typing import Union
from hashlib import sha256
import os

def get_csci_salt() -> bytes:   #pragma no cover
    """Returns the appropriate salt for CSCI E-29
    :return: bytes representation of the CSCI salt
    """

    # Hint: use os.environment and bytes.fromhex
    SALT = os.environ["CSCI_SALT"]
    return bytes.fromhex(SALT)



def hash_str(some_val: Union[str, bytes], salt: Union[str, bytes] = "") -> bytes:
    """Converts strings to hash digest

    See: https://en.wikipedia.org/wiki/Salt_(cryptography)

    :param some_val: thing to hash, can be str or bytes
    :param salt: Add randomness to the hashing, can be str or bytes
    :return: sha256 hash digest of some_val with salt, type bytes
    """
    h=sha256()

    # lambda function to encode strings.
    str_encode = lambda x: x.encode() if isinstance(x, str) else x
    
    h.update(str_encode(salt))
    h.update(str_encode(some_val))
    # return digest of the data
    return h.digest()


def get_user_id(username: str) -> str:  #pragma: no cover

    """Returns hashed username using the previous provided salt
    :return: bytes representation of the hashed username.
    """    
    salt = get_csci_salt()
    return hash_str(username.lower(), salt=salt).hex()[:8]
