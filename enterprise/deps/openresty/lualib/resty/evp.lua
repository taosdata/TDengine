-- Copyright (C) by Daniel Hiltgen (daniel.hiltgen@docker.com)


local ffi = require "ffi"
local _C = ffi.C
local _M = { _VERSION = "0.0.2" }


local CONST = {
    SHA256_DIGEST = "SHA256",
    SHA512_DIGEST = "SHA512",
}
_M.CONST = CONST


-- Reference: https://wiki.openssl.org/index.php/EVP_Signing_and_Verifying
ffi.cdef[[
// Error handling
unsigned long ERR_get_error(void);
const char * ERR_reason_error_string(unsigned long e);

// Basic IO
typedef struct bio_st BIO;
typedef struct bio_method_st BIO_METHOD;
BIO_METHOD *BIO_s_mem(void);
BIO * BIO_new(BIO_METHOD *type);
int	BIO_puts(BIO *bp,const char *buf);
void BIO_vfree(BIO *a);
int    BIO_write(BIO *b, const void *buf, int len);

// RSA
typedef struct rsa_st RSA;
int RSA_size(const RSA *rsa);
void RSA_free(RSA *rsa);
typedef int pem_password_cb(char *buf, int size, int rwflag, void *userdata);
RSA * PEM_read_bio_RSAPrivateKey(BIO *bp, RSA **rsa, pem_password_cb *cb,
								void *u);
RSA * PEM_read_bio_RSAPublicKey(BIO *bp, RSA **rsa, pem_password_cb *cb,
                                void *u);

// EVP PKEY
typedef struct evp_pkey_st EVP_PKEY;
typedef struct engine_st ENGINE;
EVP_PKEY *EVP_PKEY_new(void);
int EVP_PKEY_set1_RSA(EVP_PKEY *pkey,RSA *key);
EVP_PKEY *EVP_PKEY_new_mac_key(int type, ENGINE *e,
                               const unsigned char *key, int keylen);
void EVP_PKEY_free(EVP_PKEY *key);
int i2d_RSA(RSA *a, unsigned char **out);

// PUBKEY
EVP_PKEY *PEM_read_bio_PUBKEY(BIO *bp, EVP_PKEY **x,
                              pem_password_cb *cb, void *u);

// X509
typedef struct x509_st X509;
X509 *PEM_read_bio_X509(BIO *bp, X509 **x, pem_password_cb *cb, void *u);
EVP_PKEY *      X509_get_pubkey(X509 *x);
void X509_free(X509 *a);
void EVP_PKEY_free(EVP_PKEY *key);
int i2d_X509(X509 *a, unsigned char **out);
X509 *d2i_X509_bio(BIO *bp, X509 **x);

// X509 store
typedef struct x509_store_st X509_STORE;
typedef struct X509_crl_st X509_CRL;
X509_STORE *X509_STORE_new(void );
int X509_STORE_add_cert(X509_STORE *ctx, X509 *x);
    // Use this if we want to load the certs directly from a variables
int X509_STORE_add_crl(X509_STORE *ctx, X509_CRL *x);
int     X509_STORE_load_locations (X509_STORE *ctx,
                const char *file, const char *dir);
void X509_STORE_free(X509_STORE *v);

// X509 store context
typedef struct x509_store_ctx_st X509_STORE_CTX;
X509_STORE_CTX *X509_STORE_CTX_new(void);
int X509_STORE_CTX_init(X509_STORE_CTX *ctx, X509_STORE *store,
                         X509 *x509, void *chain);
int             X509_verify_cert(X509_STORE_CTX *ctx);
void X509_STORE_CTX_cleanup(X509_STORE_CTX *ctx);
int    X509_STORE_CTX_get_error(X509_STORE_CTX *ctx);
const char *X509_verify_cert_error_string(long n);
void X509_STORE_CTX_free(X509_STORE_CTX *ctx);

// EVP Sign/Verify
typedef struct env_md_ctx_st EVP_MD_CTX;
typedef struct env_md_st EVP_MD;
typedef struct evp_pkey_ctx_st EVP_PKEY_CTX;
const EVP_MD *EVP_get_digestbyname(const char *name);
EVP_MD_CTX *EVP_MD_CTX_create(void);
void    EVP_MD_CTX_destroy(EVP_MD_CTX *ctx);
int     EVP_DigestInit_ex(EVP_MD_CTX *ctx, const EVP_MD *type, ENGINE *impl);
int     EVP_DigestSignInit(EVP_MD_CTX *ctx, EVP_PKEY_CTX **pctx,
                        const EVP_MD *type, ENGINE *e, EVP_PKEY *pkey);
int     EVP_DigestUpdate(EVP_MD_CTX *ctx,const void *d,
                         size_t cnt);
int     EVP_DigestSignFinal(EVP_MD_CTX *ctx,
                        unsigned char *sigret, size_t *siglen);

int     EVP_DigestVerifyInit(EVP_MD_CTX *ctx, EVP_PKEY_CTX **pctx,
                        const EVP_MD *type, ENGINE *e, EVP_PKEY *pkey);
int     EVP_DigestVerifyFinal(EVP_MD_CTX *ctx,
                        unsigned char *sig, size_t siglen);

// Fingerprints
int X509_digest(const X509 *data,const EVP_MD *type,
                unsigned char *md, unsigned int *len);

]]


local function _err(ret)
    local code = _C.ERR_get_error()
    if code == 0 then
        return ret, "Zero error code (null arguments?)"
    end
    return ret, ffi.string(_C.ERR_reason_error_string(code))
end


local RSASigner = {}
_M.RSASigner = RSASigner

--- Create a new RSASigner
-- @param pem_private_key A private key string in PEM format
-- @returns RSASigner, err_string
function RSASigner.new(self, pem_private_key)
    local bio = _C.BIO_new(_C.BIO_s_mem())
    ffi.gc(bio, _C.BIO_vfree)
    if _C.BIO_puts(bio, pem_private_key) < 0 then
        return _err()
    end

    -- TODO might want to support password protected private keys...
    local rsa = _C.PEM_read_bio_RSAPrivateKey(bio, nil, nil, nil)
    ffi.gc(rsa, _C.RSA_free)

    local evp_pkey = _C.EVP_PKEY_new()
    if not evp_pkey then
        return _err()
    end
    ffi.gc(evp_pkey, _C.EVP_PKEY_free)
    if _C.EVP_PKEY_set1_RSA(evp_pkey, rsa) ~= 1 then
        return _err()
    end
    self.evp_pkey = evp_pkey
    return self, nil
end


--- Sign a message
-- @param message The message to sign
-- @param digest_name The digest format to use (e.g., "SHA256")
-- @returns signature, error_string
function RSASigner.sign(self, message, digest_name)
    local buf = ffi.new("unsigned char[?]", 1024)
    local len = ffi.new("size_t[1]", 1024)

    local ctx = _C.EVP_MD_CTX_create()
    if not ctx then
        return _err()
    end
    ffi.gc(ctx, _C.EVP_MD_CTX_destroy)

    local md = _C.EVP_get_digestbyname(digest_name)
    if not md then
        return _err()
    end

    if _C.EVP_DigestInit_ex(ctx, md, nil) ~= 1 then
        return _err()
    end
    local ret = _C.EVP_DigestSignInit(ctx, nil, md, nil, self.evp_pkey)
    if  ret ~= 1 then
        return _err()
    end
    if _C.EVP_DigestUpdate(ctx, message, #message) ~= 1 then
         return _err()
    end
    if _C.EVP_DigestSignFinal(ctx, buf, len) ~= 1 then
        return _err()
    end
    return ffi.string(buf, len[0]), nil
end



local RSAVerifier = {}
_M.RSAVerifier = RSAVerifier


--- Create a new RSAVerifier
-- @param key_source An instance of Cert or PublicKey used for verification
-- @returns RSAVerifier, error_string
function RSAVerifier.new(self, key_source)
    if not key_source then
        return nil, "You must pass in an key_source for a public key"
    end
    local evp_public_key = key_source.public_key
    self.evp_pkey = evp_public_key
    return self, nil
end

--- Verify a message is properly signed
-- @param message The original message
-- @param the signature to verify
-- @param digest_name The digest type that was used to sign
-- @returns bool, error_string
function RSAVerifier.verify(self, message, sig, digest_name)
    local md = _C.EVP_get_digestbyname(digest_name)
    if not md then
        return _err(false)
    end

    local ctx = _C.EVP_MD_CTX_create()
    if not ctx then
        return _err(false)
    end
    ffi.gc(ctx, _C.EVP_MD_CTX_destroy)

    if _C.EVP_DigestInit_ex(ctx, md, nil) ~= 1 then
        return _err(false)
    end

    local ret = _C.EVP_DigestVerifyInit(ctx, nil, md, nil, self.evp_pkey)
    if ret ~= 1 then
        return _err(false)
    end
    if _C.EVP_DigestUpdate(ctx, message, #message) ~= 1 then
        return _err(false)
    end
    local sig_bin = ffi.new("unsigned char[?]", #sig)
    ffi.copy(sig_bin, sig, #sig)
    if _C.EVP_DigestVerifyFinal(ctx, sig_bin, #sig) == 1 then
        return true, nil
    else
        return false, "Verification failed"
    end
end


local Cert = {}
_M.Cert = Cert


--- Create a new Certificate object
-- @param payload A PEM or DER format X509 certificate
-- @returns Cert, error_string
function Cert.new(self, payload)
    if not payload then
        return nil, "Must pass a PEM or binary DER cert"
    end
    local bio = _C.BIO_new(_C.BIO_s_mem())
    ffi.gc(bio, _C.BIO_vfree)
    local x509
    if payload:find('-----BEGIN') then
        if _C.BIO_puts(bio, payload) < 0 then
            return _err()
        end
        x509 = _C.PEM_read_bio_X509(bio, nil, nil, nil)
    else
        if _C.BIO_write(bio, payload, #payload) < 0 then
            return _err()
        end
        x509 = _C.d2i_X509_bio(bio, nil)
    end
    if not x509 then
        return _err()
    end
    ffi.gc(x509, _C.X509_free)
    self.x509 = x509
    local public_key, err = self:get_public_key()
    if not public_key then
        return nil, err
    end

    ffi.gc(public_key, _C.EVP_PKEY_free)
    
    self.public_key = public_key
    return self, nil
end


--- Retrieve the DER format of the certificate
-- @returns Binary DER format
function Cert.get_der(self)
    local bufp = ffi.new("unsigned char *[1]")
    local len = _C.i2d_X509(self.x509, bufp)
    if len < 0 then
        return _err()
    end
    local der = ffi.string(bufp[0], len)
    return der, nil
end

--- Retrieve the cert fingerprint
-- @param digest_name the Type of digest to use (e.g., "SHA256")
-- @returns fingerprint_string
function Cert.get_fingerprint(self, digest_name)
    local md = _C.EVP_get_digestbyname(digest_name)
    if not md then
        return _err()
    end
    local buf = ffi.new("unsigned char[?]", 32)
    local len = ffi.new("unsigned int[1]", 32)
    if _C.X509_digest(self.x509, md, buf, len) ~= 1 then
        return _err()
    end
    local raw = ffi.string(buf, len[0])
    local t = {}
    raw:gsub('.', function (c) table.insert(t, string.format('%02X', string.byte(c))) end)
    return table.concat(t, ":"), nil
end

--- Retrieve the public key from the CERT
-- @returns An OpenSSL EVP PKEY object representing the public key
function Cert.get_public_key(self)
    local evp_pkey = _C.X509_get_pubkey(self.x509)
    if not evp_pkey then
        return _err()
    end

    return evp_pkey, nil
end

--- Verify the Certificate is trusted
-- @param trusted_cert_file File path to a list of PEM encoded trusted certificates
-- @return bool, error_string
function Cert.verify_trust(self, trusted_cert_file)
    local store = _C.X509_STORE_new()
    if not store then
        return _err(false)
    end
    ffi.gc(store, _C.X509_STORE_free)
    if _C.X509_STORE_load_locations(store, trusted_cert_file, nil) ~=1 then
        return _err(false)
    end

    local ctx = _C.X509_STORE_CTX_new()
    if not store then
        return _err(false)
    end
    ffi.gc(ctx, _C.X509_STORE_CTX_free)
    if _C.X509_STORE_CTX_init(ctx, store, self.x509, nil) ~= 1 then
        return _err(false)
    end

    if _C.X509_verify_cert(ctx) ~= 1 then
        local code = _C.X509_STORE_CTX_get_error(ctx)
        local msg = ffi.string(_C.X509_verify_cert_error_string(code))
        _C.X509_STORE_CTX_cleanup(ctx)
        return false, msg
    end
    _C.X509_STORE_CTX_cleanup(ctx)
    return true, nil

end

local PublicKey = {}
_M.PublicKey = PublicKey

--- Create a new PublicKey object
--
-- If a PEM fornatted key is provided, the key must start with
--
-- ----- BEGIN PUBLIC KEY -----
--
-- @param payload A PEM or DER format public key file 
-- @return PublicKey, error_string
function PublicKey.new(self, payload)
    if not payload then
        return nil, "Must pass a PEM or binary DER public key"
    end
    local bio = _C.BIO_new(_C.BIO_s_mem())
    ffi.gc(bio, _C.BIO_vfree)
    local pkey
    if payload:find('-----BEGIN') then
        if _C.BIO_puts(bio, payload) < 0 then
            return _err()
        end
        pkey = _C.PEM_read_bio_PUBKEY(bio, nil, nil, nil)
    else
        if _C.BIO_write(bio, payload, #payload) < 0 then
            return _err()
        end
        pkey = _C.d2i_PUBKEY_bio(bio, nil)
    end
    if not pkey then
        return _err()
    end
    ffi.gc(pkey, _C.EVP_PKEY_free)
    self.public_key = pkey
    return self, nil
end
    

return _M
