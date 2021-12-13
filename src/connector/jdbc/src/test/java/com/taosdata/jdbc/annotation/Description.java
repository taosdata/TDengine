package com.taosdata.jdbc.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface Description {
    String message();

    // The test is under some conditions
    String condition() default "";

    // git blame author
    String author() default "";

    // since which version;
    String version() default "";
}
