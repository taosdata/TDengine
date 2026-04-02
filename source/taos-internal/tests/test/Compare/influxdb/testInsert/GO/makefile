target = insertInfluxdb

SRC_DIR = src

src = $(wildcard $(SRC_DIR)/*.go)

$(target) : $(src)
	go build -o $@ $^

.PHONY: clean

clean:
	rm -rf $(target)
