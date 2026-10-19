"""115 网盘上传协议的加密封装（app/providers/pan115_cipher.py）。

覆盖两块能力：
    1. ``make_upload_payload``  —— 把上传参数打包成 uplb.115.com 上传接口
       所需的加密表单（``k_ec`` token + AES-CBC 加密的 ``data``）；
    2. ``decode_upload_response`` —— 解密上传接口的响应（AES-CBC + LZ4）。

.. note::
    协议逆向参考社区实现（p115client / p115cipher），此处为本项目的独立
    重写：AES-128-CBC 与 LZ4 block 解压均为纯 Python 实现，**不引入任何
    第三方依赖**。载荷只有数百字节、响应只有数十 KB，纯 Python 的性能
    完全够用，换来的是用户部署零额外依赖。

    所谓 "ECDH" 只是 115 服务端的历史命名：实际使用协议内置的固定
    AES key/IV（客户端与服务端硬编码一致），并不做真正的椭圆曲线
    密钥交换。社区实现的 ``generate_ecdh_pair``（NIST224p 真 ECDH）
    属于另一条链路，本项目不走该路径。
"""

import json
from base64 import b64encode
from binascii import b2a_hex, crc32
from hashlib import md5, sha1
from time import time
from typing import Any, Dict, List
from urllib.parse import urlencode

__all__ = [
    "make_upload_payload",
    "decode_upload_response",
    "aes128_cbc_encrypt",
    "aes128_cbc_decrypt",
    "lz4_block_decompress",
    "lz4_decompress",
]

# === 协议常量（服务端硬编码一致，逆向自社区实现） ===
CRC_SALT = b"^j>WD3Kr?J2gLFjD4W2y@"
MD5_SALT = b"Qclm8MGWUv59TnrR0XPg"
AES_PUBKEY = (
    b"\x1d\x03\x0e\x80\xa1x\xdc\xee\xce\xcd\xa3w\xde\x12\x8d\x8e\xd9\xdd\xcfU\xaea\xedF\xea\x12\x1a\x1c\xfc\x81"
)
AES_KEY = b"\xfb\x1a\x19\xd6R\xf5\xaa\xf7\xbce\x1d\x0fi\xbfB/"
AES_IV = b"i\xbfB/I\x96\x05P\xa0\xadD\xec4F\xcbL"

# AES-128 轮常量与 S-box（FIPS-197 标准公开数据）
_RCON = (0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80, 0x1B, 0x36)
_SBOX = bytes((
    0x63, 0x7C, 0x77, 0x7B, 0xF2, 0x6B, 0x6F, 0xC5, 0x30, 0x01, 0x67, 0x2B, 0xFE, 0xD7, 0xAB, 0x76,
    0xCA, 0x82, 0xC9, 0x7D, 0xFA, 0x59, 0x47, 0xF0, 0xAD, 0xD4, 0xA2, 0xAF, 0x9C, 0xA4, 0x72, 0xC0,
    0xB7, 0xFD, 0x93, 0x26, 0x36, 0x3F, 0xF7, 0xCC, 0x34, 0xA5, 0xE5, 0xF1, 0x71, 0xD8, 0x31, 0x15,
    0x04, 0xC7, 0x23, 0xC3, 0x18, 0x96, 0x05, 0x9A, 0x07, 0x12, 0x80, 0xE2, 0xEB, 0x27, 0xB2, 0x75,
    0x09, 0x83, 0x2C, 0x1A, 0x1B, 0x6E, 0x5A, 0xA0, 0x52, 0x3B, 0xD6, 0xB3, 0x29, 0xE3, 0x2F, 0x84,
    0x53, 0xD1, 0x00, 0xED, 0x20, 0xFC, 0xB1, 0x5B, 0x6A, 0xCB, 0xBE, 0x39, 0x4A, 0x4C, 0x58, 0xCF,
    0xD0, 0xEF, 0xAA, 0xFB, 0x43, 0x4D, 0x33, 0x85, 0x45, 0xF9, 0x02, 0x7F, 0x50, 0x3C, 0x9F, 0xA8,
    0x51, 0xA3, 0x40, 0x8F, 0x92, 0x9D, 0x38, 0xF5, 0xBC, 0xB6, 0xDA, 0x21, 0x10, 0xFF, 0xF3, 0xD2,
    0xCD, 0x0C, 0x13, 0xEC, 0x5F, 0x97, 0x44, 0x17, 0xC4, 0xA7, 0x7E, 0x3D, 0x64, 0x5D, 0x19, 0x73,
    0x60, 0x81, 0x4F, 0xDC, 0x22, 0x2A, 0x90, 0x88, 0x46, 0xEE, 0xB8, 0x14, 0xDE, 0x5E, 0x0B, 0xDB,
    0xE0, 0x32, 0x3A, 0x0A, 0x49, 0x06, 0x24, 0x5C, 0xC2, 0xD3, 0xAC, 0x62, 0x91, 0x95, 0xE4, 0x79,
    0xE7, 0xC8, 0x37, 0x6D, 0x8D, 0xD5, 0x4E, 0xA9, 0x6C, 0x56, 0xF4, 0xEA, 0x65, 0x7A, 0xAE, 0x08,
    0xBA, 0x78, 0x25, 0x2E, 0x1C, 0xA6, 0xB4, 0xC6, 0xE8, 0xDD, 0x74, 0x1F, 0x4B, 0xBD, 0x8B, 0x8A,
    0x70, 0x3E, 0xB5, 0x66, 0x48, 0x03, 0xF6, 0x0E, 0x61, 0x35, 0x57, 0xB9, 0x86, 0xC1, 0x1D, 0x9E,
    0xE1, 0xF8, 0x98, 0x11, 0x69, 0xD9, 0x8E, 0x94, 0x9B, 0x1E, 0x87, 0xE9, 0xCE, 0x55, 0x28, 0xDF,
    0x8C, 0xA1, 0x89, 0x0D, 0xBF, 0xE6, 0x42, 0x68, 0x41, 0x99, 0x2D, 0x0F, 0xB0, 0x54, 0xBB, 0x16,
))
_INV_SBOX = bytes((_SBOX.index(i) for i in range(256)))


# === AES-128 纯 Python 实现（FIPS-197） ===

def _xtime(a: int) -> int:
    a <<= 1
    if a & 0x100:
        a = (a ^ 0x1B) & 0xFF
    return a


def _gmul(a: int, b: int) -> int:
    """GF(2^8) 乘法（俄罗斯农民乘法）。"""
    p = 0
    for _ in range(8):
        if b & 1:
            p ^= a
        hi = a & 0x80
        a = (a << 1) & 0xFF
        if hi:
            a ^= 0x1B
        b >>= 1
    return p


def _expand_key(key: bytes) -> List[List[int]]:
    """AES-128 密钥扩展，返回 44 个 32 位词（每词 4 字节）。"""
    if len(key) != 16:
        raise ValueError("AES-128 key 必须是 16 字节")
    words = [list(key[4 * i:4 * i + 4]) for i in range(4)]
    for i in range(4, 44):
        temp = list(words[i - 1])
        if i % 4 == 0:
            temp = temp[1:] + temp[:1]                      # RotWord
            temp = [_SBOX[b] for b in temp]                 # SubWord
            temp[0] ^= _RCON[i // 4 - 1]                    # Rcon
        words.append([words[i - 4][j] ^ temp[j] for j in range(4)])
    return words


def _add_round_key(state: List[int], words: List[List[int]], rnd: int) -> None:
    for c in range(4):
        word = words[4 * rnd + c]
        for r in range(4):
            state[4 * c + r] ^= word[r]


def _sub_bytes(state: List[int]) -> None:
    for i in range(16):
        state[i] = _SBOX[state[i]]


def _inv_sub_bytes(state: List[int]) -> None:
    for i in range(16):
        state[i] = _INV_SBOX[state[i]]


def _shift_rows(state: List[int]) -> None:
    for r in range(1, 4):
        row = [state[r + 4 * c] for c in range(4)]
        row = row[r:] + row[:r]
        for c in range(4):
            state[r + 4 * c] = row[c]


def _inv_shift_rows(state: List[int]) -> None:
    for r in range(1, 4):
        row = [state[r + 4 * c] for c in range(4)]
        row = row[-r:] + row[:-r]
        for c in range(4):
            state[r + 4 * c] = row[c]


def _mix_columns(state: List[int]) -> None:
    for c in range(4):
        i = 4 * c
        a0, a1, a2, a3 = state[i], state[i + 1], state[i + 2], state[i + 3]
        state[i] = _xtime(a0) ^ (_xtime(a1) ^ a1) ^ a2 ^ a3
        state[i + 1] = a0 ^ _xtime(a1) ^ (_xtime(a2) ^ a2) ^ a3
        state[i + 2] = a0 ^ a1 ^ _xtime(a2) ^ (_xtime(a3) ^ a3)
        state[i + 3] = (_xtime(a0) ^ a0) ^ a1 ^ a2 ^ _xtime(a3)


def _inv_mix_columns(state: List[int]) -> None:
    for c in range(4):
        i = 4 * c
        a0, a1, a2, a3 = state[i], state[i + 1], state[i + 2], state[i + 3]
        state[i] = _gmul(a0, 14) ^ _gmul(a1, 11) ^ _gmul(a2, 13) ^ _gmul(a3, 9)
        state[i + 1] = _gmul(a0, 9) ^ _gmul(a1, 14) ^ _gmul(a2, 11) ^ _gmul(a3, 13)
        state[i + 2] = _gmul(a0, 13) ^ _gmul(a1, 9) ^ _gmul(a2, 14) ^ _gmul(a3, 11)
        state[i + 3] = _gmul(a0, 11) ^ _gmul(a1, 13) ^ _gmul(a2, 9) ^ _gmul(a3, 14)


def _aes_encrypt_block(block: bytes, words: List[List[int]]) -> bytes:
    state = list(block)
    _add_round_key(state, words, 0)
    for rnd in range(1, 10):
        _sub_bytes(state)
        _shift_rows(state)
        _mix_columns(state)
        _add_round_key(state, words, rnd)
    _sub_bytes(state)
    _shift_rows(state)
    _add_round_key(state, words, 10)
    return bytes(state)


def _aes_decrypt_block(block: bytes, words: List[List[int]]) -> bytes:
    state = list(block)
    _add_round_key(state, words, 10)
    for rnd in range(9, 0, -1):
        _inv_shift_rows(state)
        _inv_sub_bytes(state)
        _add_round_key(state, words, rnd)
        _inv_mix_columns(state)
    _inv_shift_rows(state)
    _inv_sub_bytes(state)
    _add_round_key(state, words, 0)
    return bytes(state)


def _pkcs7_pad(data: bytes) -> bytes:
    # 注意：整块数据（16 的倍数）不追加填充块，与 115 协议行为一致
    pad_size = -len(data) & 15
    return data + bytes((pad_size,)) * pad_size


def _pkcs7_unpad(data: bytes) -> bytes:
    if not data:
        return data
    pad_size = data[-1]
    if 0 < pad_size < 16 and all(b == pad_size for b in data[-pad_size:]):
        return data[:-pad_size]
    return data


def aes128_cbc_encrypt(data: bytes, key: bytes = AES_KEY, iv: bytes = AES_IV) -> bytes:
    padded = _pkcs7_pad(bytes(data))
    words = _expand_key(bytes(key))
    out = bytearray()
    prev = list(iv)
    for i in range(0, len(padded), 16):
        chunk = bytes(a ^ b for a, b in zip(padded[i:i + 16], prev))
        enc = _aes_encrypt_block(chunk, words)
        out += enc
        prev = list(enc)
    return bytes(out)


def aes128_cbc_decrypt(data: bytes, key: bytes = AES_KEY, iv: bytes = AES_IV) -> bytes:
    raw = bytes(data)
    # 与 p115cipher 参考实现一致：uplb 响应密文尾部可能有少量协议杂音
    # （整体非 16 对齐，约 12 字节），截断到 16 的倍数再解密，尾部直接忽略。
    raw = raw[:len(raw) & -16]
    if not raw:
        raise ValueError("AES-CBC 密文为空")
    words = _expand_key(bytes(key))
    out = bytearray()
    prev = list(iv)
    for i in range(0, len(raw), 16):
        chunk = raw[i:i + 16]
        dec = _aes_decrypt_block(chunk, words)
        out += bytes(a ^ b for a, b in zip(dec, prev))
        prev = list(chunk)
    return _pkcs7_unpad(bytes(out))


# === LZ4 block 解压（LZ4 block format，纯 Python） ===

def lz4_block_decompress(src: bytes) -> bytes:
    """解压单个 LZ4 block（无长度前缀）。"""
    raw = bytes(src)
    out = bytearray()
    pos = 0
    length = len(raw)
    while pos < length:
        token = raw[pos]
        pos += 1
        lit_len = token >> 4
        if lit_len == 15:
            while True:
                if pos >= length:
                    raise ValueError("LZ4 字面量长度损坏")
                extra = raw[pos]
                pos += 1
                lit_len += extra
                if extra != 255:
                    break
        out += raw[pos:pos + lit_len]
        pos += lit_len
        if pos >= length:
            break
        if pos + 2 > length:
            raise ValueError("LZ4 匹配偏移量损坏")
        offset = raw[pos] | (raw[pos + 1] << 8)
        pos += 2
        match_len = token & 0x0F
        if match_len == 15:
            while True:
                if pos >= length:
                    raise ValueError("LZ4 匹配长度损坏")
                extra = raw[pos]
                pos += 1
                match_len += extra
                if extra != 255:
                    break
        match_len += 4
        start = len(out) - offset
        if start < 0:
            raise ValueError("LZ4 匹配偏移量越界")
        # 逐字节拷贝，天然支持重叠匹配（offset < match_len）
        for _ in range(match_len):
            out.append(out[start])
            start += 1
    return bytes(out)


def lz4_decompress(src: bytes) -> bytes:
    """解压 115 响应使用的分块 LZ4 流（每块带 2 字节小端长度前缀）。"""
    raw = bytes(src)
    out = bytearray()
    pos = 0
    length = len(raw)
    while pos + 2 <= length:
        block_len = raw[pos] | (raw[pos + 1] << 8)
        pos += 2
        if not block_len:
            break
        if pos + block_len > length:
            raise ValueError("LZ4 分块长度越界")
        out += lz4_block_decompress(raw[pos:pos + block_len])
        pos += block_len
    return bytes(out)


# === 上传接口载荷编解码 ===

def ecdh_encode_token(timestamp: int) -> bytes:
    """生成上传请求的 k_ec token（时间戳 + 协议公钥 + CRC32 校验）。"""
    token = bytearray()
    token += AES_PUBKEY[:15]
    token += b"\x00s\x00\x00\x00"
    token += int(timestamp).to_bytes(4, "little")
    token += AES_PUBKEY[15:]
    token += b"\x00\x01\x00\x00\x00"
    token += crc32(CRC_SALT + bytes(token)).to_bytes(4, "little")
    return b64encode(bytes(token))


def make_upload_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """构建 uplb 上传接口的请求参数。

    :param payload: 需包含 userid / userkey / fileid(SHA1) / filename /
        filesize / target / path；sign_key / sign_val 二次验证时提供。
    :return: {"params": {"k_ec": ...}, "data": <AES-CBC 密文>}，
        HTTP 请求时以表单形式提交（key=params/data 序列化后的值）。
        注意：最终密文只包含真值字段（appid=0/sign_key=""/path="" 等
        空值会被剔除，与真实客户端行为一致）。
    """
    merged = dict(payload)
    merged.setdefault("sign_key", "")
    merged.setdefault("sign_val", "")
    merged.setdefault("appid", 0)
    merged.setdefault("topupload", "true")
    merged.setdefault("appversion", "36.2.28")
    merged["t"] = int(time())

    sig_sha1 = sha1(str(merged["userkey"]).encode("ascii"))
    sig_sha1.update(b2a_hex(sha1(
        "{userid}{fileid}{target}0".format_map(merged).encode("ascii")
    ).digest()))
    sig_sha1.update(b"000000")
    merged["sig"] = sig_sha1.hexdigest().upper()

    token_md5 = md5(MD5_SALT)
    token_md5.update(
        "{fileid}{filesize}{sign_key}{sign_val}{userid}{t}".format_map(merged).encode("ascii")
    )
    token_md5.update(b2a_hex(md5(b"%d" % int(merged["userid"])).digest()))
    token_md5.update(str(merged["appversion"]).encode("ascii"))
    merged["token"] = token_md5.hexdigest()

    # 与 p115cipher.make_upload_payload 完全一致：只保留真值字段。
    # 注意 appid=0 / sign_key="" / sign_val="" / path="" 都会被剔除
    # （真实客户端从不发送这些空值，多发字段可能触发服务端拒绝）。
    encoded = urlencode(
        sorted((k, v) for k, v in merged.items() if v)
    ).encode("latin-1")
    return {
        "params": {"k_ec": ecdh_encode_token(merged["t"]).decode("ascii")},
        "data": aes128_cbc_encrypt(encoded),
    }


def decode_upload_response(content: bytes) -> Dict[str, Any]:
    """解密 uplb 上传接口的加密响应（AES-CBC + 分块 LZ4 + JSON）。"""
    plain = aes128_cbc_decrypt(bytes(content))
    plain = lz4_decompress(plain)
    try:
        payload = json.loads(plain.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise ValueError("115 上传响应解密失败") from exc
    return payload if isinstance(payload, dict) else {}
