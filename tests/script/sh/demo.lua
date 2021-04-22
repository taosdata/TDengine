funcName = "test"

global =  {} 
global["sum"] = 0.0
global["num"] = 0

function test_init()
  return global  
end

function test_add(rows, ans)
  for i=1, #rows do
    ans["sum"] = ans["sum"] + rows[i] * rows[i]  
  end
  ans["num"] = ans["num"] + #rows  
  return ans; 
end

function test_finalize(ans) 
  return ans["sum"]/ans["num"]; 
end  
