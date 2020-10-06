#include <sql.h>

#include <stdio.h>


int main(void) {
  SQLRETURN r;
  SQLHENV env = {0};
  SQLHDBC conn = {0};
  r = SQLAllocEnv(&env);
  if (r!=SQL_SUCCESS) return 1;
  do {
    r = SQLAllocConnect(env, &conn);
    if (r!=SQL_SUCCESS) break;
    SQLFreeConnect(conn);
  } while (0);
  SQLFreeEnv(env);
  return r ? 1 : 0;
}

