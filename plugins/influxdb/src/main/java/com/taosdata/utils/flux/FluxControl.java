package com.taosdata.utils.flux;

/**
 * 速度控制
 *
 * @author ZYP
 */
public class FluxControl {

    // 滑动窗口，如果不是1000，后面“判断是否可发”需要修改
    private final static int WINDOW = 1000;
    // 实例名
    private String fluxname;
    // 开始时间
    private long time_start;
    // 上次校准时间
    private long time_aline;
    // 上次校准时间窗口中值（处理）
    private long time_aline_middle;
    // 上次校准时间后统计量
    private long amount_aline;
    // 上次校准时间中值后统计量
    private long amount_aline_middle;
    // 当前实际速度
    private long speed;

    public FluxControl(String fluxname) {
        this.fluxname = fluxname;
        this.time_start = 0L;
        this.time_aline = 0L;
        this.time_aline_middle = 0L;
        this.amount_aline = 0;
        this.amount_aline_middle = 0;
        this.speed = 0;
    }

    public void cycleCheck(long amount, long limit) {
        do {
            if (check(amount, limit)) {
                break;
            }
            try {

            } catch (Exception e) {
                e.printStackTrace();
            }
        } while (true);
    }

    public synchronized boolean check(long amount, long limit) {
        // 当前时间
        long time_now = System.currentTimeMillis();
        // 是否第一次发送
        boolean startFlag = false;
        if (this.time_start == 0L) {
            this.time_start = time_now;
            this.time_aline = time_now - WINDOW;
            this.time_aline_middle = time_now - WINDOW / 2;
            startFlag = true;
        }
        // 距离上次校准时间的时间间隔
        long time_interval = time_now - this.time_aline;
        // 是否可发（根据限速）
        boolean allowSend_limit = false;
        // 是否可发（根据速度计算）
        boolean allowSend_speed = true;
        // 不限速，可以一直发
        if (limit < 0 || limit == Integer.MAX_VALUE) {
            allowSend_limit = true;
        }
        // 限速为零，禁止发
        if (limit == 0) {
            allowSend_limit = false;
        }
        // 判断不同情况
        if (startFlag || this.time_aline_middle < this.time_start) {
            /* 第一次发送或刚开始window内发送，使用校准 */
            // 间隔内允许发送量
            long amount_max = (limit * time_interval) / 1000;
            // 根据速度是否可发
            if (this.amount_aline + amount >= amount_max) {
                allowSend_speed = false;
            }
            // 按一个窗口计算速度
            refreshSpeed(amount, WINDOW, time_now, true, allowSend_limit || allowSend_speed);
        } else if (time_interval < WINDOW / 2) {
            /* 间隔过短，使用校准中值 */
            time_interval = time_now - this.time_aline_middle;
            // 间隔内允许发送量
            long amount_max = (limit * time_interval) / 1000;
            // 根据速度是否可发
            if (this.amount_aline_middle + amount >= amount_max) {
                allowSend_speed = false;
            }
            // 计算速度
            refreshSpeed(amount, time_interval, time_now, false, allowSend_limit || allowSend_speed);
        } else if (time_interval < WINDOW) {
            /* 普通间隔，使用校准 */
            // 间隔内允许发送量
            long amount_max = (limit * time_interval) / 1000;
            // 根据速度是否可发
            if (this.amount_aline + amount >= amount_max) {
                allowSend_speed = false;
            }
            // 计算速度
            refreshSpeed(amount, time_interval, time_now, true, allowSend_limit || allowSend_speed);
        } else if (time_interval < WINDOW * 2) {
            /* 超过一个窗口，校准中值 */
            time_interval = time_now - this.time_aline_middle;
            // 间隔内允许发送量
            long amount_max = (limit * time_interval) / 1000;
            // 根据速度是否可发
            if (this.amount_aline_middle + amount >= amount_max) {
                allowSend_speed = false;
            }
            // 计算速度
            refreshSpeed(amount, time_interval, time_now, false, allowSend_limit || allowSend_speed);
        } else {
            /* 超过两个窗口，使用校准 */
            // 重置校准量与校准中值量
            this.amount_aline = 0;
            this.amount_aline_middle = 0;
            // 间隔内允许发送量
            long amount_max = (limit * time_interval) / 1000;
            // 根据速度是否可发
            if (this.amount_aline + amount >= amount_max) {
                allowSend_speed = false;
            }
            // 按一个窗口计算速度
            refreshSpeed(amount, WINDOW, time_now, true, allowSend_limit || allowSend_speed);
        }
        return allowSend_limit || allowSend_speed;
    }

    /**
     * 更新发送速度
     *
     * @param amount
     * @param time_interval
     * @param time_now
     * @param aline
     */
    private void refreshSpeed(long amount, long time_interval, long time_now, boolean aline, boolean allowSend) {
        if (aline) {
            if (allowSend) {
                this.amount_aline += amount;
                this.amount_aline_middle += amount;
            }
            // 使用校准，计算实际速度
            this.speed = (this.amount_aline * 1000) / time_interval;
        } else {
            if (allowSend) {
                this.amount_aline += amount;
                this.amount_aline_middle += amount;
            }
            // 使用校准中值，计算实际速度
            this.speed = (this.amount_aline_middle * 1000) / time_interval;
        }
        // 更改校准时间及校准量
        if (time_now > this.time_aline + WINDOW) {
            this.time_aline = ((time_now - this.time_start) / 1000L) * 1000L + this.time_start;
            if (allowSend) {
                this.amount_aline = amount;
            } else {
                this.amount_aline = 0;
            }
        }
        // 更改校准时间中值及校准中值量
        if (time_now > this.time_aline_middle + WINDOW) {
            // edit by zyp at 2018.09.21 避免长时间不发送时赋值错误
            if (time_now > this.time_aline + WINDOW / 2) {
                this.time_aline_middle = this.time_aline + WINDOW / 2;
                this.amount_aline_middle = amount;
            } else {
                this.time_aline_middle = this.time_aline - WINDOW / 2;
                this.amount_aline_middle = amount;
            }
            if (allowSend) {
                this.amount_aline_middle = amount;
            } else {
                this.amount_aline_middle = 0;
            }
        }
    }

    /**
     * 查询当前速度
     *
     * @return
     */
    public long getSpeed() {
        // 当前时间
        long time_now = System.currentTimeMillis();
        if (time_now > this.time_aline + WINDOW * 2) {
            // 长时间未发送，速度为零
            return 0;
        } else {
            // 返回当前速度
            return this.speed;
        }
    }

    /**
     * 查询名称
     *
     * @return
     */
    public String getName() {
        return this.fluxname;
    }
}
