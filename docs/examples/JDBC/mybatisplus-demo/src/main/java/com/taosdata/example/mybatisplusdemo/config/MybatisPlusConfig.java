package com.taosdata.example.mybatisplusdemo.config;

import com.baomidou.mybatisplus.extension.plugins.PaginationInterceptor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MybatisPlusConfig {


    /** mybatis 3.4.1 pagination config start ***/
//    @Bean
//    public MybatisPlusInterceptor mybatisPlusInterceptor() {
//        MybatisPlusInterceptor interceptor = new MybatisPlusInterceptor();
//        interceptor.addInnerInterceptor(new PaginationInnerInterceptor());
//        return interceptor;
//    }

//    @Bean
//    public ConfigurationCustomizer configurationCustomizer() {
//        return configuration -> configuration.setUseDeprecatedExecutor(false);
//    }

    @Bean
    public PaginationInterceptor paginationInterceptor() {
//        return new PaginationInterceptor();
        PaginationInterceptor paginationInterceptor = new PaginationInterceptor();
        //TODO: mybatis-plus do not support TDengine, use postgresql Dialect
        paginationInterceptor.setDialectType("postgresql");

        return paginationInterceptor;
    }

}
