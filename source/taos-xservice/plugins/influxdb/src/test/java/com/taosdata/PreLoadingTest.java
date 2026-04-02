package com.taosdata;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
class PreLoadingTest {

    @InjectMocks
    private PreLoading preLoading;

    @Disabled
    @DisplayName("测试参数为空")
    @Test
    void run_noArgs() {
        preLoading.run();
        // exit code is 1
    }

    @Disabled
    @DisplayName("测试输出版本信息")
    @Test
    void run_printVersion() {
        preLoading.run("-v");
        // exit code is 0
    }
}