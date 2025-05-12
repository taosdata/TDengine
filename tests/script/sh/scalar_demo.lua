funcName = "test"

ans = 1 

function test_init()
  return ans  
end

function test_add(rows, ans)
  for i=1, #rows do
    rows[i] = rows[i] + ans  
  end
  return rows 
end
