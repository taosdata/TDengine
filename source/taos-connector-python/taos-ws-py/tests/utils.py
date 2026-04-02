import base64
import hashlib
import hmac
import os
import random
import string
import struct
import subprocess
import time
import toml


TEST_TD_ENTERPRISE = os.getenv("TEST_TD_ENTERPRISE") is not None


def get_connector_info():
    cargo_toml = toml.load(os.path.join(os.path.dirname(__file__), "../Cargo.toml"))
    version = cargo_toml["package"]["version"]

    git_commit_id = os.getenv("GIT_COMMIT_ID")
    if not git_commit_id:
        git_commit_id = (
            subprocess.check_output(
                ["git", "rev-parse", "--short=7", "HEAD"],
                cwd=os.path.dirname(os.path.abspath(__file__)),
            )
            .decode()
            .strip()
        )
    if not git_commit_id:
        git_commit_id = "ncid000"

    return f"python-ws-v{version}-{git_commit_id}"


LOWERCASE = string.ascii_lowercase
UPPERCASE = string.ascii_uppercase
DIGITS = string.digits
ALL_CHARS = LOWERCASE + UPPERCASE + DIGITS


def generate_totp_seed(length: int) -> str:
    assert length >= 3
    buf = [
        random.choice(LOWERCASE),
        random.choice(UPPERCASE),
        random.choice(DIGITS),
    ]
    buf += [random.choice(ALL_CHARS) for _ in range(3, length)]
    random.shuffle(buf)
    return "".join(buf)


def generate_totp_secret(seed: bytes) -> bytes:
    return hmac.new(b"", seed, hashlib.sha256).digest()


def generate_totp_code(secret: bytes) -> str:
    now = int(time.time())
    counter = now // 30
    code = hotp(secret, counter, 6)
    return f"{code:06d}"


def hotp(secret: bytes, counter: int, digits: int) -> int:
    counter_bytes = struct.pack(">Q", counter)
    mac = hmac.new(secret, counter_bytes, hashlib.sha1).digest()
    offset = mac[-1] & 0x0F
    code = struct.unpack(">I", mac[offset : offset + 4])[0] & 0x7FFFFFFF
    return code % (10 ** min(digits, 8))


def totp_secret_encode(secret: bytes) -> str:
    return base64.b32encode(secret).decode("utf-8").rstrip("=")


def totp_secret_decode(secret: str) -> bytes:
    missing_padding = len(secret) % 8
    if missing_padding:
        secret += "=" * (8 - missing_padding)
    return base64.b32decode(secret, casefold=True)


def test_username() -> str:
    return os.getenv("TDENGINE_TEST_USERNAME", "root")


def test_password() -> str:
    return os.getenv("TDENGINE_TEST_PASSWORD", "taosdata")
