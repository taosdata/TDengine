package com.taosdata.example.springbootdemo.util;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.util.Map;

@Aspect
@Component
public class TaosAspect {

    @Around("execution(java.util.Map<String,Object> com.taosdata.example.springbootdemo.dao.*.*(..))")
    public Object handleType(ProceedingJoinPoint joinPoint) {
        Map<String, Object> result = null;
        try {
            result = (Map<String, Object>) joinPoint.proceed();
            for (String key : result.keySet()) {
                Object obj = result.get(key);
                if (obj instanceof byte[]) {
                    obj = new String((byte[]) obj);
                    result.put(key, obj);
                }
                if (obj instanceof Timestamp) {
                    obj = ((Timestamp) obj).getTime();
                    result.put(key, obj);
                }
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return result;
    }
}
