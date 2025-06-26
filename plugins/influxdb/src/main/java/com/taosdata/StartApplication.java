package com.taosdata;

import org.bouncycastle.jce.ECNamedCurveTable;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.jce.spec.ECNamedCurveParameterSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.security.Provider;
import java.security.Security;

/**
 * 程序启动入口
 *
 * @author ZYP
 */
@SpringBootApplication
public class StartApplication {
    private static final Logger log = LoggerFactory.getLogger(StartApplication.class);

    public static void main(String[] args) {
        {
            Security.addProvider(new BouncyCastleProvider());
            Provider bcProvider = Security.getProvider("BC");
            log.info("BC Provider Info: {}", (bcProvider != null ? bcProvider.getInfo() : "load failed"));

            // 检查 SM2 曲线是否加载
            ECNamedCurveParameterSpec sm2Spec = ECNamedCurveTable.getParameterSpec("sm2p256v1");
            log.info("SM2 Curve Info: {} ", (sm2Spec != null ? sm2Spec.getName() : "not found"));
        }

        SpringApplication.run(StartApplication.class, args);
    }
}
