local _M = {_VERSION="0.1.5"}

--[[
  This file defines "validators" to be used in validating a spec.  A "validator" is simply a function with
  a signature that matches:

    function(val, claim, jwt_json)

  This function returns either true or false.  If a validator needs to give more information on why it failed,
  then it can also raise an error (which will be used in the "reason" part of the validated jwt_obj).  If a
  validator returns nil, then it is assumed to have passed (same as returning true) and that you just forgot
  to actually return a value.

  There is a special claim name of "__jwt" that can be used to validate the entire jwt_obj.

  "val" is the value being tested.  It may be nil if the claim doesn't exist in the jwt_obj.  If the function
  is being called for the "__jwt" claim, then "val" will contain a deep clone of the full jwt object.

  "claim" is the claim that is being tested.  It is passed in just in case a validator needs to do additional
  checks.  It will be the string "__jwt" if the validator is being called for the entire jwt_object.

  "jwt_json" is a json-encoded representation of the full object that is being tested.  It will never be nil,
  and can always be decoded using cjson.decode(jwt_json).
]]--


--[[
    A function which will define a validator.  It creates both "opt_" and required (non-"opt_")
    versions.  The function that is passed in is the *optional* version.
]]--
local function define_validator(name, fx)
  _M["opt_" .. name] = fx
  _M[name] = function(...) return _M.chain(_M.required(), fx(...)) end
end

-- Validation messages
local messages = {
  nil_validator = "Cannot create validator for nil %s.",
  wrong_type_validator = "Cannot create validator for non-%s %s.",
  empty_table_validator = "Cannot create validator for empty table %s.",
  wrong_table_type_validator = "Cannot create validator for non-%s table %s.",
  required_claim = "'%s' claim is required.",
  wrong_type_claim = "'%s' is malformed.  Expected to be a %s.",
  missing_claim = "Missing one of claims - [ %s ]."
}

-- Local function to make sure that a value is non-nil or raises an error
local function ensure_not_nil(v, e, ...)
  return v ~= nil and v or error(string.format(e, ...), 0)
end

-- Local function to make sure that a value is the given type
local function ensure_is_type(v, t, e, ...)
  return type(v) == t and v or error(string.format(e, ...), 0)
end

-- Local function to make sure that a value is a (non-empty) table
local function ensure_is_table(v, e, ...)
  ensure_is_type(v, "table", e, ...)
  return ensure_not_nil(next(v), e, ...)
end

-- Local function to make sure all entries in the table are the given type
local function ensure_is_table_type(v, t, e, ...)
  if v ~= nil then
    ensure_is_table(v, e, ...)
    for _,val in ipairs(v) do
      ensure_is_type(val, t, e, ...)
    end
  end
  return v
end

-- Local function to ensure that a number is non-negative (positive or 0)
local function ensure_is_non_negative(v, e, ...)
  if v ~= nil then
    ensure_is_type(v, "number", e, ...)
    if v >= 0 then
      return v
    else
      error(string.format(e, ...), 0)
    end
  end
end

-- A local function which returns simple equality
local function equality_function(val, check)
  return val == check
end

-- A local function which returns string match
local function string_match_function(val, pattern)
  return string.match(val, pattern) ~= nil
end

--[[
    A local function which returns truth on existence of check in vals.
    Adopted from auth0/nginx-jwt table_contains by @twistedstream
]]--
local function table_contains_function(vals, check)
    for _, val in pairs(vals) do
        if val == check then return true end
    end
    return false
end


-- A local function which returns numeric greater than comparison
local function greater_than_function(val, check)
  return val > check
end

-- A local function which returns numeric greater than or equal comparison
local function greater_than_or_equal_function(val, check)
  return val >= check
end

-- A local function which returns numeric less than comparison
local function less_than_function(val, check)
  return val < check
end

-- A local function which returns numeric less than or equal comparison
local function less_than_or_equal_function(val, check)
  return val <= check
end


--[[
    Returns a validator that chains the given functions together, one after
    another - as long as they keep passing their checks.
]]--
function _M.chain(...)
  local chain_functions = {...}
  for _, fx in ipairs(chain_functions) do
    ensure_is_type(fx, "function", messages.wrong_type_validator, "function", "chain_function")
  end

  return function(val, claim, jwt_json)
    for _, fx in ipairs(chain_functions) do
      if fx(val, claim, jwt_json) == false then
        return false
      end
    end
    return true
  end
end

--[[
    Returns a validator that returns false if a value doesn't exist.  If
    the value exists and a chain_function is specified, then the value of
        chain_function(val, claim, jwt_json)
    will be returned, otherwise, true will be returned.  This allows for
    specifying that a value is both required *and* it must match some
    additional check.  This function will be used in the "required_*" shortcut
    functions for simplification.
]]--
function _M.required(chain_function)
  if chain_function ~= nil then
    return _M.chain(_M.required(), chain_function)
  end

  return function(val, claim, jwt_json)
    ensure_not_nil(val, messages.required_claim, claim)
    return true
  end
end

--[[
    Returns a validator which errors with a message if *NONE* of the given claim
    keys exist.  It is expected that this function is used against a full jwt object.
    The claim_keys must be a non-empty table of strings.
]]--
function _M.require_one_of(claim_keys)
  ensure_not_nil(claim_keys, messages.nil_validator, "claim_keys")
  ensure_is_type(claim_keys, "table", messages.wrong_type_validator, "table", "claim_keys")
  ensure_is_table(claim_keys, messages.empty_table_validator, "claim_keys")
  ensure_is_table_type(claim_keys, "string", messages.wrong_table_type_validator, "string", "claim_keys")

  return function(val, claim, jwt_json)
    ensure_is_type(val, "table", messages.wrong_type_claim, claim, "table")
    ensure_is_type(val.payload, "table", messages.wrong_type_claim, claim .. ".payload", "table")

    for i, v in ipairs(claim_keys) do
      if val.payload[v] ~= nil then return true end
    end

    error(string.format(messages.missing_claim, table.concat(claim_keys, ", ")), 0)
  end
end

--[[
    Returns a validator that checks if the result of calling the given function for
    the tested value and the check value returns true.  The value of check_val and
    check_function cannot be nil.  The optional name is used for error messages and
    defaults to "check_value".  The optional check_type is used to make sure that
    the check type matches and defaults to type(check_val).  The first parameter
    passed to check_function will *never* be nil (check succeeds if value is nil).
    Use the required version to fail on nil.  If the check_function raises an
    error, that will be appended to the error message.
]]--
define_validator("check", function(check_val, check_function, name, check_type)
  name = name or "check_val"
  ensure_not_nil(check_val, messages.nil_validator, name)

  ensure_not_nil(check_function, messages.nil_validator, "check_function")
  ensure_is_type(check_function, "function", messages.wrong_type_validator, "function", "check_function")

  check_type = check_type or type(check_val)
  return function(val, claim, jwt_json)
    if val == nil then return true end

    ensure_is_type(val, check_type, messages.wrong_type_claim, claim, check_type)
    return check_function(val, check_val)
  end
end)


--[[
    Returns a validator that checks if a value exactly equals the given check_value.
    If the value is nil, then this check succeeds.  The value of check_val cannot be
    nil.
]]--
define_validator("equals", function(check_val)
  return _M.opt_check(check_val, equality_function, "check_val")
end)


--[[
    Returns a validator that checks if a value matches the given pattern.  The value
    of pattern must be a string.
]]--
define_validator("matches", function (pattern)
  ensure_is_type(pattern, "string", messages.wrong_type_validator, "string", "pattern")
  return _M.opt_check(pattern, string_match_function, "pattern", "string")
end)


--[[
    Returns a validator which calls the given function for each of the given values
    and the tested value.  If any of these calls return true, then this function
    returns true.  The value of check_values must be a non-empty table with all the
    same types, and the value of check_function must not be nil.  The optional name
    is used for error messages and defaults to "check_values".  The optional
    check_type is used to make sure that the check type matches and defaults to
    type(check_values[1]) - the table type.
]]--
define_validator("any_of", function(check_values, check_function, name, check_type, table_type)
  name = name or "check_values"
  ensure_not_nil(check_values, messages.nil_validator, name)
  ensure_is_type(check_values, "table", messages.wrong_type_validator, "table", name)
  ensure_is_table(check_values, messages.empty_table_validator, name)

  table_type = table_type or type(check_values[1])
  ensure_is_table_type(check_values, table_type, messages.wrong_table_type_validator, table_type, name)

  ensure_not_nil(check_function, messages.nil_validator, "check_function")
  ensure_is_type(check_function, "function", messages.wrong_type_validator, "function", "check_function")

  check_type = check_type or table_type
  return _M.opt_check(check_values, function(v1, v2)
    for i, v in ipairs(v2) do
      if check_function(v1, v) then return true end
    end
    return false
  end, name, check_type)
end)


--[[
    Returns a validator that checks if a value exactly equals any of the given values.
]]--
define_validator("equals_any_of", function(check_values)
  return _M.opt_any_of(check_values, equality_function, "check_values")
end)


--[[
    Returns a validator that checks if a value matches any of the given patterns.
]]--
define_validator("matches_any_of", function(patterns)
  return _M.opt_any_of(patterns, string_match_function, "patterns", "string", "string")
end)

--[[
    Returns a validator that checks if a value of expected type string exists in any of the given values.
    The value of check_values must be a non-empty table with all the same types.
    The optional name is used for error messages and defaults to "check_values".
]]--
define_validator("contains_any_of", function(check_values, name)
  return _M.opt_any_of(check_values, table_contains_function, name, "table", "string")
end)

--[[
    Returns a validator that checks how a value compares (numerically) to a given
    check_value.  The value of check_val cannot be nil and must be a number.
]]--
define_validator("greater_than", function(check_val)
  ensure_is_type(check_val, "number", messages.wrong_type_validator, "number", "check_val")
  return _M.opt_check(check_val, greater_than_function, "check_val", "number")
end)
define_validator("greater_than_or_equal", function(check_val)
  ensure_is_type(check_val, "number", messages.wrong_type_validator, "number", "check_val")
  return _M.opt_check(check_val, greater_than_or_equal_function, "check_val", "number")
end)
define_validator("less_than", function(check_val)
  ensure_is_type(check_val, "number", messages.wrong_type_validator, "number", "check_val")
  return _M.opt_check(check_val, less_than_function, "check_val", "number")
end)
define_validator("less_than_or_equal", function(check_val)
  ensure_is_type(check_val, "number", messages.wrong_type_validator, "number", "check_val")
  return _M.opt_check(check_val, less_than_or_equal_function, "check_val", "number")
end)


--[[
    A function to set the leeway (in seconds) used for is_not_before and is_not_expired.  The
    default is to use 0 seconds
]]--
local system_leeway = 0
function _M.set_system_leeway(leeway)
  ensure_is_type(leeway, "number", "leeway must be a non-negative number")
  ensure_is_non_negative(leeway, "leeway must be a non-negative number")
  system_leeway = leeway
end


--[[
    A function to set the system clock used for is_not_before and is_not_expired.  The
    default is to use ngx.now
]]--
local system_clock = ngx.now
function _M.set_system_clock(clock)
  ensure_is_type(clock, "function", "clock must be a function")
  -- Check that clock returns the correct value
  local t = clock()
  ensure_is_type(t, "number", "clock function must return a non-negative number")
  ensure_is_non_negative(t, "clock function must return a non-negative number")
  system_clock = clock
end

-- Local helper function for date validation
local function validate_is_date(val, claim, jwt_json)
  ensure_is_non_negative(val, messages.wrong_type_claim, claim, "positive numeric value")
  return true
end

-- Local helper for date formatting
local function format_date_on_error(date_check_function, error_msg)
  ensure_is_type(date_check_function, "function", messages.wrong_type_validator, "function", "date_check_function")
  ensure_is_type(error_msg, "string", messages.wrong_type_validator, "string", error_msg)
  return function(val, claim, jwt_json)
    local ret = date_check_function(val, claim, jwt_json)
    if ret == false then
      error(string.format("'%s' claim %s %s", claim, error_msg, ngx.http_time(val)), 0)
    end
    return true
  end
end

--[[
    Returns a validator that checks if the current time is not before the tested value
    within the system's leeway.  This means that:
      val <= (system_clock() + system_leeway).
]]--
define_validator("is_not_before", function()
  return format_date_on_error(
     _M.chain(validate_is_date,
        function(val)
           return val and less_than_or_equal_function(val, (system_clock() + system_leeway))
        end),
     "not valid until"
  )
end)


--[[
    Returns a validator that checks if the current time is not equal to or after the
    tested value within the system's leeway.  This means that:
      val > (system_clock() - system_leeway).
]]--
define_validator("is_not_expired", function()
  return format_date_on_error(
     _M.chain(validate_is_date,
       function(val)
          return val and greater_than_function(val, (system_clock() - system_leeway))
       end),
     "expired at"
  )
end)

--[[
    Returns a validator that checks if the current time is the same as the tested value
    within the system's leeway.  This means that:
      val >= (system_clock() - system_leeway) and val <= (system_clock() + system_leeway).
]]--
define_validator("is_at", function()
  local now = system_clock()
  return format_date_on_error(
    _M.chain(validate_is_date,
             function(val)
                local now = system_clock()
                return val and
                   greater_than_or_equal_function(val, now - system_leeway) and
                   less_than_or_equal_function(val, now + system_leeway)
             end),
    "is only valid at"
  )
end)


return _M
