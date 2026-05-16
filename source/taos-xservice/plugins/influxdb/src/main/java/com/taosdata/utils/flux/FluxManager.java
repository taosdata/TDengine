package com.taosdata.utils.flux;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 速度控制管理
 *
 * @author ZYP
 */
public class FluxManager {

    private ConcurrentHashMap<String, FluxControl> fluxControlMap;

    private FluxManager() {
        this.fluxControlMap = new ConcurrentHashMap<>();
    }

    private static class FluxManagerHolder {
        private static FluxManager instance = new FluxManager();
    }

    public static FluxManager getInstance() {
        return FluxManagerHolder.instance;
    }

    public FluxControl getFluxControl(String fluxname) {
        FluxControl fluxControl;
        if (this.fluxControlMap.containsKey(fluxname)) {
            fluxControl = this.fluxControlMap.get(fluxname);
        } else {
            fluxControl = new FluxControl(fluxname);
            this.fluxControlMap.put(fluxname, fluxControl);
        }
        return fluxControl;
    }
}
