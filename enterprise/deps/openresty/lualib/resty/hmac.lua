
local str_util = require "resty.string"
local ffi = require "ffi"
local ffi_new = ffi.new
local ffi_str = ffi.string
local ffi_gc = ffi.gc
local C = ffi.C
local setmetatable = setmetatable
local error = error


local _M = { _VERSION = '0.01' }

local mt = { __index = _M }


ffi.cdef[[
typedef struct engine_st ENGINE;
typedef struct evp_pkey_ctx_st EVP_PKEY_CTX;
typedef struct env_md_ctx_st EVP_MD_CTX;
typedef struct env_md_st EVP_MD;

struct env_md_ctx_st
    {
    const EVP_MD *digest;
    ENGINE *engine;
    unsigned long flags;
    void *md_data;
    EVP_PKEY_CTX *pctx;
    int (*update)(EVP_MD_CTX *ctx,const void *data,size_t count);
    };

struct env_md_st
    {
    int type;
    int pkey_type;
    int md_size;
    unsigned long flags;
    int (*init)(EVP_MD_CTX *ctx);
    int (*update)(EVP_MD_CTX *ctx,const void *data,size_t count);
    int (*final)(EVP_MD_CTX *ctx,unsigned char *md);
    int (*copy)(EVP_MD_CTX *to,const EVP_MD_CTX *from);
    int (*cleanup)(EVP_MD_CTX *ctx);

    int (*sign)(int type, const unsigned char *m, unsigned int m_length, unsigned char *sigret, unsigned int *siglen, void *key);
    int (*verify)(int type, const unsigned char *m, unsigned int m_length, const unsigned char *sigbuf, unsigned int siglen, void *key);
    int required_pkey_type[5];
    int block_size;
    int ctx_size;
    int (*md_ctrl)(EVP_MD_CTX *ctx, int cmd, int p1, void *p2);
    };

typedef struct hmac_ctx_st
    {
    const EVP_MD *md;
    EVP_MD_CTX md_ctx;
    EVP_MD_CTX i_ctx;
    EVP_MD_CTX o_ctx;
    unsigned int key_length;
    unsigned char key[128];
    } HMAC_CTX;

void HMAC_CTX_init(HMAC_CTX *ctx);
void HMAC_CTX_cleanup(HMAC_CTX *ctx);

int HMAC_Init_ex(HMAC_CTX *ctx, const void *key, int len,const EVP_MD *md, ENGINE *impl);
int HMAC_Update(HMAC_CTX *ctx, const unsigned char *data, size_t len);
int HMAC_Final(HMAC_CTX *ctx, unsigned char *md, unsigned int *len);

const EVP_MD *EVP_md5(void);
const EVP_MD *EVP_sha1(void);
const EVP_MD *EVP_sha256(void);
const EVP_MD *EVP_sha512(void);
]]

local buf = ffi_new("unsigned char[64]")
local res_len = ffi_new("unsigned int[1]")
local ctx_ptr_type = ffi.typeof("HMAC_CTX[1]")
local hashes = {
    MD5 = C.EVP_md5(),
    SHA1 = C.EVP_sha1(),
    SHA256 = C.EVP_sha256(),
    SHA512 = C.EVP_sha512()
}


_M.ALGOS = hashes


function _M.new(self, key, hash_algo)
    local ctx = ffi_new(ctx_ptr_type)

    C.HMAC_CTX_init(ctx)

    local _hash_algo = hash_algo or hashes.md5

    if C.HMAC_Init_ex(ctx, key, #key, _hash_algo, nil) == 0 then
        return nil
    end

    ffi_gc(ctx, C.HMAC_CTX_cleanup)

    return setmetatable({ _ctx = ctx }, mt)
end


function _M.update(self, s)
    return C.HMAC_Update(self._ctx, s, #s) == 1
end


function _M.final(self, s, hex_output)

    if s ~= nil then
        if C.HMAC_Update(self._ctx, s, #s) == 0 then
            return nil
        end
    end

    if C.HMAC_Final(self._ctx, buf, res_len) == 1 then
        if hex_output == true then
            return str_util.to_hex(ffi_str(buf, res_len[0]))
        end
        return ffi_str(buf, res_len[0])
    end

    return nil
end


function _M.reset(self)
    return C.HMAC_Init_ex(self._ctx, nil, 0, nil, nil) == 1
end

return _M
