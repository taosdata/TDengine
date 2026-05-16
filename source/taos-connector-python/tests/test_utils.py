from utils import *


def test_generate_totp_seed():
    seed = generate_totp_seed(16)
    assert len(seed) == 16
    assert any(c.islower() for c in seed)
    assert any(c.isupper() for c in seed)
    assert any(c.isdigit() for c in seed)


def test_generate_totp_code():
    secret = generate_totp_secret(b"12345678901234567890")
    code = generate_totp_code(secret)
    assert len(code) == 6
    assert code.isdigit()


def test_hotp():
    counter = 1765854733 // 30
    digits = 6
    assert hotp(generate_totp_secret(b"12345678901234567890"), counter, digits) == 383089
    assert hotp(generate_totp_secret(b"abcdefghijklmnopqrstuvwxyz"), counter, digits) == 269095
    assert hotp(generate_totp_secret(b"!@#$%^&*()_+-=[]{}|;':,.<>/?`~"), counter, digits) == 203356


def test_generate_totp_secret():
    secret1 = generate_totp_secret(b"12345678901234567890")
    totp_secret1 = totp_secret_encode(secret1)
    assert totp_secret1 == "VR62SA7EK3RP7MRTH7QXSIVZXXS57OY2SRUMGLKDJPREZ62OHFEQ"
    assert totp_secret_decode(totp_secret1) == secret1

    secret2 = generate_totp_secret(b"abcdefghijklmnopqrstuvwxyz")
    totp_secret2 = totp_secret_encode(secret2)
    assert totp_secret2 == "OMYPD744HIZB2KZPAUNLEWUFBNRQBILEPWD2FGPUDYCZMFTCRFXQ"
    assert totp_secret_decode(totp_secret2) == secret2

    secret3 = generate_totp_secret(b"!@#$%^&*()_+-=[]{}|;':,.<>/?`~")
    totp_secret3 = totp_secret_encode(secret3)
    assert totp_secret3 == "FURKOZ6REIGLQHP5OMKLZZFUQNOCRDJPCOSDP5ESH2VR3IU7NAYA"
    assert totp_secret_decode(totp_secret3) == secret3
