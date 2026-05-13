import logger, { setLevel } from "@src/common/log";

describe("log level print", () => {
    test("normal connect", async () => {
        logger.info("log level is info");
        logger.debug("log level is debug");
        logger.error("log level is error");

        let isLevel = logger.isLevelEnabled("info");
        expect(isLevel).toEqual(true);

        setLevel("debug");
        logger.debug("log level is debug");
        isLevel = logger.isLevelEnabled("debug");
        expect(isLevel).toEqual(true);

        setLevel("error");
        logger.error("log level is error");
        logger.debug("log level is debug");
        isLevel = logger.isLevelEnabled("error");
        expect(isLevel).toEqual(true);
    });
});
