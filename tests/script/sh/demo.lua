funcName = "test"

global =  {} 

function test_init()
  return global  
end

function test_add(rows, ans, key)
  t = {}
  t["sum"] = 0.0
  t["num"] = 0
  for i=1, #rows do
    t["sum"] = t["sum"] + rows[i] * rows[i]  
  end
  t["num"] = #rows  


  if (ans[key] ~= nil)    
  then
    ans[key]["sum"] = ans[key]["sum"] + t["sum"]
    ans[key]["num"] = ans[key]["num"] + t["num"]
  else 
    ans[key] = t
  end
  
  return ans; 
end

function test_finalize(ans, key) 
  local ret = 0.0 

  if (ans[key] ~= nil and ans[key]["num"] == 30000)    
  then
    ret = ans[key]["sum"]/ans[key]["num"]
    ans[key]["sum"] = 0.0 
    ans[key]["num"] = 0
  else 
    ret = inf 
  end
  
  return ret, ans
end  
