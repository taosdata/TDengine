local cjson = require "cjson.safe"

local aes = require "resty.aes"
local evp = require "resty.evp"
local hmac = require "resty.hmac"
local resty_random = require "resty.random"

local _M = {_VERSION="0.1.11"}
local mt = {__index=_M}

local string_match= string.match
local string_rep = string.rep
local string_format = string.format
local string_sub = string.sub
local string_byte = string.byte
local string_char = string.char
local table_concat = table.concat
local ngx_encode_base64 = ngx.encode_base64
local ngx_decode_base64 = ngx.decode_base64
local cjson_encode = cjson.encode
local cjson_decode = cjson.decode
local tostring = tostring

-- define string constants to avoid string garbage collection
local str_const = {
  invalid_jwt= "invalid jwt string",
  regex_join_msg = "%s.%s",
  regex_join_delim = "([^%s]+)",
  regex_split_dot = "%.",
  regex_jwt_join_str = "%s.%s.%s",
  raw_underscore  = "raw_",
  dash = "-",
  empty = "",
  dotdot = "..",
  table  = "table",
  plus = "+",
  equal = "=",
  underscore = "_",
  slash = "/",
  header = "header",
  typ = "typ",
  JWT = "JWT",
  JWE = "JWE",
  payload = "payload",
  signature = "signature",
  encrypted_key = "encrypted_key",
  alg = "alg",
  enc = "enc",
  kid = "kid",
  exp = "exp",
  nbf = "nbf",
  iss = "iss",
  full_obj = "__jwt",
  AES = "AES",
  cbc = "cbc",
  x5c = "x5c",
  x5u = 'x5u',
  HS256 = "HS256",
  HS512 = "HS512",
  RS256 = "RS256",
  RS512 = "RS512",
  A128CBC_HS256 = "A128CBC-HS256",
  A256CBC_HS512 = "A256CBC-HS512",
  DIR = "dir",
  reason = "reason",
  verified = "verified",
  number = "number",
  string = "string",
  funct = "function",
  boolean = "boolean",
  table = "table",
  valid = "valid",
  valid_issuers = "valid_issuers",
  lifetime_grace_period = "lifetime_grace_period",
  require_nbf_claim = "require_nbf_claim",
  require_exp_claim = "require_exp_claim",
  internal_error = "internal error",
  everything_awesome = "everything is awesome~ :p"
}

-- @function split string
local function split_string(str, delim, maxNb)
  local result = {}
  local sep = string_format(str_const.regex_join_delim, delim)
  for m in str:gmatch(sep) do
    result[#result+1]=m
  end
  return result
end

-- @function is nil or boolean
-- @return true if param is nil or true or false; false otherwise
local function is_nil_or_boolean(arg_value)
    if arg_value == nil then
        return true
    end

    if type(arg_value) ~= str_const.boolean then
        return false
    end

    return true
end

--@function get the row part
--@param part_name
--@param jwt_obj
local function get_raw_part(part_name, jwt_obj)
  local raw_part = jwt_obj[str_const.raw_underscore .. part_name]
  if raw_part == nil then
    local part = jwt_obj[part_name]
    if part == nil then
      error({reason="missing part " .. part_name})
    end
    raw_part = _M:jwt_encode(part)
  end
  return raw_part
end


--@function decrypt payload
--@param secret_key to decrypt the payload
--@param encrypted payload
--@param encryption algorithm
--@param iv which was generated while encrypting the payload
--@return decrypted payloaf
local function decrypt_payload(secret_key, encrypted_payload, enc, iv_in )
  local decrypted_payload
  if enc == str_const.A128CBC_HS256 then
    local aes_128_cbc_with_iv = assert(aes:new(secret_key, str_const.AES, aes.cipher(128,str_const.cbc), {iv=iv_in} ))
    decrypted_payload=  aes_128_cbc_with_iv:decrypt(encrypted_payload)
  elseif enc == str_const.A256CBC_HS512 then
    local aes_256_cbc_with_iv = assert(aes:new(secret_key, str_const.AES, aes.cipher(256,str_const.cbc), {iv=iv_in} ))
    decrypted_payload=  aes_256_cbc_with_iv:decrypt(encrypted_payload)

  else
    return nil, "unsupported enc: " .. enc
  end
  if not  decrypted_payload then
    return nil, "invalid secret key"
  end
  return decrypted_payload
end

-- @function : encrypt payload using given secret
-- @param secret key to encrypt
-- @param algortim to use for encryption
-- @message  : data to be encrypted. It could be lua table or string
local function encrypt_payload(secret_key, message, enc )

  if enc == str_const.A128CBC_HS256 then
    local iv_rand =  resty_random.bytes(16,true)
    local aes_128_cbc_with_iv = assert(aes:new(secret_key, str_const.AES, aes.cipher(128,str_const.cbc), {iv=iv_rand} ))
    local encrypted = aes_128_cbc_with_iv:encrypt(message)
    return encrypted, iv_rand

  elseif enc == str_const.A256CBC_HS512 then
    local iv_rand =  resty_random.bytes(16,true)
    local aes_256_cbc_with_iv = assert(aes:new(secret_key, str_const.AES, aes.cipher(256,str_const.cbc), {iv=iv_rand} ))
    local encrypted = aes_256_cbc_with_iv:encrypt(message)
    return encrypted, iv_rand

  else
    return nil, nil , "unsupported enc: " .. enc
  end
end

--@function hmac_digest : generate hmac digest based on key for input message
--@param mac_key
--@param input message
--@return hmac digest
local function hmac_digest(enc, mac_key, message)
  if enc == str_const.A128CBC_HS256 then
    return hmac:new(mac_key, hmac.ALGOS.SHA256):final(message)
  elseif enc == str_const.A256CBC_HS512 then
    return hmac:new(mac_key, hmac.ALGOS.SHA512):final(message)
  else
    error({reason="unsupported enc: " .. enc})
  end
end

--@function dervice keys: it generates key if null based on encryption algorithm
--@param encryption type
--@param secret key
--@return secret key, mac key and encryption key
local function derive_keys(enc, secret_key)
  local mac_key_len, enc_key_len = 16, 16

  if enc == str_const.A128CBC_HS256 then
    mac_key_len, enc_key_len = 16, 16
  elseif enc == str_const.A256CBC_HS512 then
    mac_key_len, enc_key_len = 32, 32
  end

  local secret_key_len = mac_key_len + enc_key_len

  if not secret_key then
    secret_key =  resty_random.bytes(secret_key_len, true)
  end

  if #secret_key ~= secret_key_len then
    error({reason="The pre-shared content key must be ".. secret_key_len})
  end

  local mac_key = string_sub(secret_key, 1, mac_key_len)
  local enc_key = string_sub(secret_key, enc_key_len + 1)
  return secret_key, mac_key, enc_key
end

--@function parse_jwe
--@param pre-shared key
--@encoded-header
local function parse_jwe(preshared_key, encoded_header, encoded_encrypted_key, encoded_iv, encoded_cipher_text, encoded_auth_tag)


  local header = _M:jwt_decode(encoded_header, true)
  if not header then
    error({reason="invalid header: " .. encoded_header})
  end

  local key, mac_key, enc_key = derive_keys(header.enc, preshared_key)

  -- use preshared key if given otherwise decrypt the encoded key
  if not preshared_key then
    local encrypted_key = _M:jwt_decode(encoded_encrypted_key)
    if header.alg == str_const.DIR then
      error({reason="preshared key must not ne null"})
    else  -- implement algorithm to decrypt the key
      error({reason="invalid algorithm: " .. header.alg})
    end
  end

  local cipher_text = _M:jwt_decode(encoded_cipher_text)
  local iv =  _M:jwt_decode(encoded_iv)

  local basic_jwe = {
    internal = {
      encoded_header = encoded_header,
      cipher_text = cipher_text,
      key = key,
      iv = iv
    },
    header=header,
    signature=_M:jwt_decode(encoded_auth_tag)
  }

  local json_payload, err = decrypt_payload(enc_key, cipher_text, header.enc, iv)
  if not json_payload then
    basic_jwe.reason = err

  else
    basic_jwe.payload = cjson_decode(json_payload)
    basic_jwe.internal.json_payload=json_payload
  end
  return basic_jwe
end

-- @function parse_jwt
-- @param encoded header
-- @param encoded
-- @param signature
-- @return jwt table
local function parse_jwt(encoded_header, encoded_payload, signature)
  local header = _M:jwt_decode(encoded_header, true)
  if not header then
    error({reason="invalid header: " .. encoded_header})
  end

  local payload = _M:jwt_decode(encoded_payload, true)
  if not payload then
    error({reason="invalid payload: " .. encoded_payload})
  end

  local basic_jwt = {
    raw_header=encoded_header,
    raw_payload=encoded_payload,
    header=header,
    payload=payload,
    signature=signature
  }
  return basic_jwt

end

-- @function parse token - this can be JWE or JWT token
-- @param token string
-- @return jwt/jwe tables
local function parse(secret, token_str)
  local tokens = split_string(token_str, str_const.regex_split_dot)
  local num_tokens = #tokens
  if num_tokens == 3 then
    return  parse_jwt(tokens[1], tokens[2], tokens[3])
  elseif num_tokens == 4  then
    return parse_jwe(secret, tokens[1], "", tokens[2], tokens[3],  tokens[4])
  elseif num_tokens == 5 then
    return parse_jwe(secret, tokens[1], tokens[2], tokens[3],  tokens[4], tokens[5])
  else
    error({reason=str_const.invalid_jwt})
  end
end


--@function jwt encode : it converts into base64 encoded string. if input is a table, it convets into
-- json before converting to base64 string
--@param payloaf
--@return base64 encoded payloaf
function _M.jwt_encode(self, ori)
  if type(ori) == str_const.table then
    ori = cjson_encode(ori)
  end
  return ngx.encode_base64(ori):gsub(str_const.plus, str_const.dash):gsub(str_const.slash, str_const.underscore):gsub(str_const.equal, str_const.empty)
end



--@function jwt decode : decode bas64 encoded string
function _M.jwt_decode(self, b64_str, json_decode)
  b64_str = b64_str:gsub(str_const.dash, str_const.plus):gsub(str_const.underscore, str_const.slash)

  local reminder = #b64_str % 4
  if reminder > 0 then
    b64_str = b64_str .. string_rep(str_const.equal, 4 - reminder)
  end
  local data = ngx_decode_base64(b64_str)
  if not data then
    return nil
  end
  if json_decode then
    data = cjson_decode(data)
  end
  return data
end

--- Initialize the trusted certs
-- During RS256 verify, we'll make sure the
-- cert was signed by one of these
function _M.set_trusted_certs_file(self, filename)
  self.trusted_certs_file = filename
end
_M.trusted_certs_file = nil

--- Set a whitelist of allowed algorithms
-- E.g., jwt:set_alg_whitelist({RS256=1,HS256=1})
--
-- @param algorithms - A table with keys for the supported algorithms
--                     If the table is non-nil, during
--                     verify, the alg must be in the table
function _M.set_alg_whitelist(self, algorithms)
  self.alg_whitelist = algorithms
end

_M.alg_whitelist = nil


--- Returns the list of default validations that will be
--- applied upon the verification of a jwt.
function _M.get_default_validation_options(self, jwt_obj)
  return {
    [str_const.require_exp_claim]=jwt_obj[exp] ~= nil,
    [str_const.require_nbf_claim]=jwt_obj[nbf] ~= nil
  }
end

--- Set a function used to retrieve the content of x5u urls
--
-- @param retriever_function - A pointer to a function. This function should be
--                             defined to accept three string parameters. First one
--                             will be the value of the 'x5u' attribute. Second
--                             one will be the value of the 'iss' attribute, would
--                             it be defined in the jwt. Third one will be the value
--                             of the 'iss' attribute, would it be defined in the jwt.
--                             This function should return the matching certificate.
function _M.set_x5u_content_retriever(self, retriever_function)
  if type(retriever_function) ~= str_const.funct then
    error("'retriever_function' is expected to be a function", 0)
  end
  self.x5u_content_retriever = retriever_function
end

_M.x5u_content_retriever = nil

-- https://tools.ietf.org/html/rfc7516#appendix-B.3
-- TODO: do it in lua way
local function binlen(s)
  if type(s) ~= 'string' then return end

  local len = 8 * #s

  return string_char(len / 0x0100000000000000 % 0x100)
      .. string_char(len / 0x0001000000000000 % 0x100)
      .. string_char(len / 0x0000010000000000 % 0x100)
      .. string_char(len / 0x0000000100000000 % 0x100)
      .. string_char(len / 0x0000000001000000 % 0x100)
      .. string_char(len / 0x0000000000010000 % 0x100)
      .. string_char(len / 0x0000000000000100 % 0x100)
      .. string_char(len / 0x0000000000000001 % 0x100)
end

--@function sign jwe payload
--@param secret key : if used pre-shared or RSA key
--@param  jwe payload
--@return jwe token
local function sign_jwe(secret_key, jwt_obj)
  local header = jwt_obj.header
  local enc = header.enc

  local key, mac_key, enc_key = derive_keys(enc, secret_key)
  local json_payload = cjson_encode(jwt_obj.payload)
  local cipher_text, iv, err = encrypt_payload(enc_key, json_payload, enc)
  if err then
    error({reason="error while encrypting payload. Error: " .. err})
  end
  local alg = header.alg

  if alg ~= str_const.DIR then
    error({reason="unsupported alg: " .. tostring(alg)})
  end
  -- remove type
  if header.typ then
    header.typ = nil
  end
  local encoded_header = _M:jwt_encode(header)

  local encoded_header_length = binlen(encoded_header)
  local mac_input = table_concat({encoded_header , iv, cipher_text , encoded_header_length})
  local mac = hmac_digest(enc, mac_key, mac_input)
  -- TODO: implement logic for creating enc key and mac key and then encrypt key
  local encrypted_key
  if alg ==  str_const.DIR then
    encrypted_key = ""
  else
    error({reason="unsupported alg: " .. alg})
  end
  local auth_tag = string_sub(mac, 1, #mac/2)
  local jwe_table = {encoded_header, _M:jwt_encode(encrypted_key), _M:jwt_encode(iv),
    _M:jwt_encode(cipher_text),   _M:jwt_encode(auth_tag)}
  return table_concat(jwe_table, ".", 1, 5)
end

--@function get_secret_str  : returns the secret if it is a string, or the result of a function
--@param either the string secret or a function that takes a string parameter and returns a string or nil
--@param  jwt payload
--@return the secret as a string or as a function
local function get_secret_str(secret_or_function, jwt_obj)
  if type(secret_or_function) == str_const.funct then
    -- Only use with hmac algorithms
    local alg = jwt_obj[str_const.header][str_const.alg]
    if alg ~= str_const.HS256 and alg ~= str_const.HS512 then
      error({reason="secret function can only be used with hmac alg: " .. alg})
    end

    -- Pull out the kid value from the header
    local kid_val = jwt_obj[str_const.header][str_const.kid]
    if kid_val == nil then
      error({reason="secret function specified without kid in header"})
    end

    -- Call the function
    return secret_or_function(kid_val) or error({reason="function returned nil for kid: " .. kid_val})
  elseif type(secret_or_function) == str_const.string then
    -- Just return the string
    return secret_or_function
  else
    -- Throw an error
    error({reason="invalid secret type (must be string or function)"})
  end
end

--@function sign  : create a jwt/jwe signature from jwt_object
--@param secret key
--@param jwt/jwe payload
function _M.sign(self, secret_key, jwt_obj)
  -- header typ check
  local typ = jwt_obj[str_const.header][str_const.typ]
  -- Optional header typ check [See http://tools.ietf.org/html/draft-ietf-oauth-json-web-token-25#section-5.1]
  if typ ~= nil then
    if typ ~= str_const.JWT and typ ~= str_const.JWE then
      error({reason="invalid typ: " .. typ})
    end
  end

  if typ == str_const.JWE or jwt_obj.header.enc then
    return sign_jwe(secret_key, jwt_obj)
  end
  -- header alg check
  local raw_header = get_raw_part(str_const.header, jwt_obj)
  local raw_payload = get_raw_part(str_const.payload, jwt_obj)
  local message = string_format(str_const.regex_join_msg, raw_header , raw_payload)

  local alg = jwt_obj[str_const.header][str_const.alg]
  local signature = ""
  if alg == str_const.HS256 then
    local secret_str = get_secret_str(secret_key, jwt_obj)
    signature = hmac:new(secret_str, hmac.ALGOS.SHA256):final(message)
  elseif alg == str_const.HS512 then
    local secret_str = get_secret_str(secret_key, jwt_obj)
    signature = hmac:new(secret_str, hmac.ALGOS.SHA512):final(message)
  elseif alg == str_const.RS256 then
    local signer, err = evp.RSASigner:new(secret_key)
    if not signer then
      error({reason="signer error: " .. err})
    end
    signature = signer:sign(message, evp.CONST.SHA256_DIGEST)
  else
    error({reason="unsupported alg: " .. alg})
  end
  -- return full jwt string
  return string_format(str_const.regex_join_msg, message , _M:jwt_encode(signature))

end

--@function load jwt
--@param jwt string token
--@param secret
function _M.load_jwt(self, jwt_str, secret)
  local success, ret = pcall(parse, secret, jwt_str)
  if not success then
    return {
      valid=false,
      verified=false,
      reason=ret[str_const.reason] or str_const.invalid_jwt
    }
  end

  local jwt_obj = ret
  jwt_obj[str_const.verified] = false
  jwt_obj[str_const.valid] = true
  return jwt_obj
end

--@function verify jwe object
--@param secret
--@param jwt object
--@return jwt object with reason whether verified or not
local function verify_jwe_obj(secret, jwt_obj)
  local key, mac_key, enc_key = derive_keys(jwt_obj.header.enc, jwt_obj.internal.key)
  local encoded_header = jwt_obj.internal.encoded_header

  local encoded_header_length = binlen(encoded_header)
  local mac_input = table_concat({encoded_header , jwt_obj.internal.iv, jwt_obj.internal.cipher_text , encoded_header_length})
  local mac = hmac_digest(jwt_obj.header.enc, mac_key,  mac_input)
  local auth_tag = string_sub(mac, 1, #mac/2)

  if auth_tag ~= jwt_obj.signature then
    jwt_obj[str_const.reason] = "signature mismatch: " ..
    tostring(jwt_obj[str_const.signature])

  end
  jwt_obj.internal = nil
  jwt_obj.signature = nil

  if not jwt_obj[str_const.reason] then
    jwt_obj[str_const.verified] = true
    jwt_obj[str_const.reason] = str_const.everything_awesome
  end

  return jwt_obj
end

--@function extract certificate
--@param jwt object
--@return decoded certificate
local function extract_certificate(jwt_obj, x5u_content_retriever)
  local x5c = jwt_obj[str_const.header][str_const.x5c]
  if x5c ~= nil and x5c[1] ~= nil then
    -- TODO Might want to add support for intermediaries that we
    -- don't have in our trusted chain (items 2... if present)
    local cert_str = ngx_decode_base64(x5c[1])
    if not cert_str then
      jwt_obj[str_const.reason] = "Malformed x5c header"
    end

    return cert_str
  end

  local x5u = jwt_obj[str_const.header][str_const.x5u]
  if x5u ~= nil then
    -- TODO Ensure the url starts with https://
    -- cf. https://tools.ietf.org/html/rfc7517#section-4.6

    if x5u_content_retriever == nil then
      jwt_obj[str_const.reason] = "No function has been provided to retrieve the content pointed at by the 'x5u'."
      return nil
    end

    -- TODO Maybe validate the url against an optional list whitelisted url prefixes?
    -- cf. https://news.ycombinator.com/item?id=9302394

    local iss = jwt_obj[str_const.payload][str_const.iss]
    local kid = jwt_obj[str_const.header][str_const.kid]
    local success, ret = pcall(x5u_content_retriever, x5u, iss, kid)

    if not success then
      jwt_obj[str_const.reason] = "An error occured while invoking the x5u_content_retriever function."
      return nil
    end

    return ret
  end

  -- TODO When both x5c and x5u are defined, the implementation should
  -- ensure their content match
  -- cf. https://tools.ietf.org/html/rfc7517#section-4.6

  jwt_obj[str_const.reason] = "Unsupported RS256 key model"
  return nil
  -- TODO - Implement jwk and kid based models...
end

local function get_claim_spec_from_legacy_options(self, options)
  local claim_spec = { }
  local jwt_validators = require "resty.jwt-validators"

  if options[str_const.valid_issuers] ~= nil then
    claim_spec[str_const.iss] = jwt_validators.equals_any_of(options[str_const.valid_issuers])
  end

  if options[str_const.lifetime_grace_period] ~= nil then
    jwt_validators.set_system_leeway(options[str_const.lifetime_grace_period] or 0)

    -- If we have a leeway set, then either an NBF or an EXP should also exist requireds are added below
    if options[str_const.require_nbf_claim] ~= true and options[str_const.require_exp_claim] ~= true then
      claim_spec[str_const.full_obj] = jwt_validators.require_one_of({ str_const.nbf, str_const.exp })
    end
  end

  if not is_nil_or_boolean(options[str_const.require_nbf_claim]) then
    error(string.format("'%s' validation option is expected to be a boolean.", str_const.require_nbf_claim), 0)
  end

  if not is_nil_or_boolean(options[str_const.require_exp_claim]) then
    error(string.format("'%s' validation option is expected to be a boolean.", str_const.require_exp_claim), 0)
  end

  if options[str_const.lifetime_grace_period] ~= nil or options[str_const.require_nbf_claim] ~= nil or options[str_const.require_exp_claim] ~= nil then
    if options[str_const.require_nbf_claim] == true then
      claim_spec[str_const.nbf] = jwt_validators.is_not_before()
    else
      claim_spec[str_const.nbf] = jwt_validators.opt_is_not_before()
    end

    if options[str_const.require_exp_claim] == true then
      claim_spec[str_const.exp] = jwt_validators.is_not_expired()
    else
      claim_spec[str_const.exp] = jwt_validators.opt_is_not_expired()
    end
  end

  return claim_spec
end

local function is_legacy_validation_options(options)

  -- Validation options MUST be a table
  if type(options) ~= str_const.table then
    return false
  end

  -- Validation options MUST have at least one of these, and must ONLY have these
  local legacy_options = { }
  legacy_options[str_const.valid_issuers]=1
  legacy_options[str_const.lifetime_grace_period]=1
  legacy_options[str_const.require_nbf_claim]=1
  legacy_options[str_const.require_exp_claim]=1

  local is_legacy = false
  for k in pairs(options) do
    if legacy_options[k] ~= nil then
      is_legacy = true
    else
      return false
    end
  end
  return is_legacy
end

-- Validates the claims for the given (parsed) object
local function validate_claims(self, jwt_obj, ...)
  local claim_specs = {...}
  if #claim_specs == 0 then
    table.insert(claim_specs, _M:get_default_validation_options(jwt_obj))
  end

  if jwt_obj[str_const.reason] ~= nil then
    return false
  end

  -- Encode the current jwt_obj and use it when calling the individual validation functions
  local jwt_json = cjson_encode(jwt_obj)

  -- Validate all our specs
  for _, claim_spec in ipairs(claim_specs) do
    if is_legacy_validation_options(claim_spec) then
      claim_spec = get_claim_spec_from_legacy_options(self, claim_spec)
    end
    for claim, fx in pairs(claim_spec) do
      if type(fx) ~= str_const.funct then
        error("Claim spec value must be a function - see jwt-validators.lua for helper functions", 0)
      end

      local val = claim == str_const.full_obj and cjson_decode(jwt_json) or jwt_obj.payload[claim]
      local success, ret = pcall(fx, val, claim, jwt_json)
      if not success then
        jwt_obj[str_const.reason] = ret.reason or string.gsub(ret, "^.-:%d-: ", "")
        return false
      elseif ret == false then
        jwt_obj[str_const.reason] = string.format("Claim '%s' ('%s') returned failure", claim, val)
        return false
      end
    end
  end

  -- Everything was good
  return true
end

--@function verify jwt object
--@param secret
--@param jwt_object
--@leeway
--@return verified jwt payload or jwt object with error code
function _M.verify_jwt_obj(self, secret, jwt_obj, ...)
  if not jwt_obj.valid then
    return jwt_obj
  end

  -- validate any claims that have been passed in
  if not validate_claims(self, jwt_obj, ...) then
    return jwt_obj
  end

  -- if jwe, invoked verify jwe
  if jwt_obj[str_const.header][str_const.enc] then
    return verify_jwe_obj(secret, jwt_obj)
  end

  local alg = jwt_obj[str_const.header][str_const.alg]

  local jwt_str = string_format(str_const.regex_jwt_join_str, jwt_obj.raw_header , jwt_obj.raw_payload , jwt_obj.signature)

  if self.alg_whitelist ~= nil then
    if self.alg_whitelist[alg] == nil then
      return {verified=false, reason="whitelist unsupported alg: " .. alg}
    end
  end

  if alg == str_const.HS256 or alg == str_const.HS512 then
    local success, ret = pcall(_M.sign, self, secret, jwt_obj)
    if not success then
      -- syntax check
      jwt_obj[str_const.reason] = ret[str_const.reason] or str_const.internal_error
    elseif jwt_str ~= ret then
      -- signature check
      jwt_obj[str_const.reason] = "signature mismatch: " .. jwt_obj[str_const.signature]
    end
  elseif alg == str_const.RS256 or alg == str_const.RS512 then
    local cert, err
    if self.trusted_certs_file ~= nil then
      local cert_str = extract_certificate(jwt_obj, self.x5u_content_retriever)
      if not cert_str then
        return jwt_obj
      end
      cert, err = evp.Cert:new(cert_str)
      if not cert then
        jwt_obj[str_const.reason] = "Unable to extract signing cert from JWT: " .. err
        return jwt_obj
      end
      -- Try validating against trusted CA's, then a cert passed as secret
      local trusted, err = cert:verify_trust(self.trusted_certs_file)
      if not trusted then
        jwt_obj[str_const.reason] = "Cert used to sign the JWT isn't trusted: " .. err
        return jwt_obj
      end
    elseif secret ~= nil then
      local err
      if secret:find("CERTIFICATE") then
        cert, err = evp.Cert:new(secret)
      elseif secret:find("PUBLIC KEY") then
        cert, err = evp.PublicKey:new(secret)
      end
      if not cert then
        jwt_obj[str_const.reason] = "Decode secret is not a valid cert/public key: " .. (err and err or secret)
        return jwt_obj
      end
    else
      jwt_obj[str_const.reason] = "No trusted certs loaded"
      return jwt_obj
    end
    local verifier, err = evp.RSAVerifier:new(cert)
    if not verifier then
      -- Internal error case, should not happen...
      jwt_obj[str_const.reason] = "Failed to build verifier " .. err
      return jwt_obj
    end

    -- assemble jwt parts
    local raw_header = get_raw_part(str_const.header, jwt_obj)
    local raw_payload = get_raw_part(str_const.payload, jwt_obj)

    local message =string_format(str_const.regex_join_msg, raw_header ,  raw_payload)
    local sig = _M:jwt_decode(jwt_obj[str_const.signature], false)

    if not sig then
      jwt_obj[str_const.reason] = "Wrongly encoded signature"
      return jwt_obj
    end

    local verified = false
    local err = "verify error: reason unknown"

    if alg == str_const.RS256 then
      verified, err = verifier:verify(message, sig, evp.CONST.SHA256_DIGEST)
    elseif alg == str_const.RS512 then
      verified, err = verifier:verify(message, sig, evp.CONST.SHA512_DIGEST)
    end
    if not verified then
      jwt_obj[str_const.reason] = err
    end
  else
    jwt_obj[str_const.reason] = "Unsupported algorithm " .. alg
  end

  if not jwt_obj[str_const.reason] then
    jwt_obj[str_const.verified] = true
    jwt_obj[str_const.reason] = str_const.everything_awesome
  end
  return jwt_obj

end


function _M.verify(self, secret, jwt_str, ...)
  local jwt_obj = _M.load_jwt(self, jwt_str, secret)
  if not jwt_obj.valid then
    return {verified=false, reason=jwt_obj[str_const.reason]}
  end
  return  _M.verify_jwt_obj(self, secret, jwt_obj, ...)

end

return _M
