/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0 
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * https://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License. 
 */

#include "avro/allocation.h"
#include "avro/refcount.h"
#include "avro/errors.h"
#include "avro/io.h"
#include "avro_private.h"
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include "dump.h"

enum avro_io_type_t {
	AVRO_FILE_IO,
	AVRO_MEMORY_IO
};
typedef enum avro_io_type_t avro_io_type_t;

struct avro_reader_t_ {
	avro_io_type_t type;
	volatile int  refcount;
};

struct avro_writer_t_ {
	avro_io_type_t type;
	volatile int  refcount;
};

struct _avro_reader_file_t {
	struct avro_reader_t_ reader;
	FILE *fp;
	int should_close;
	char *cur;
	char *end;
	char buffer[4096];
};

struct _avro_writer_file_t {
	struct avro_writer_t_ writer;
	FILE *fp;
	int should_close;
};

struct _avro_reader_memory_t {
	struct avro_reader_t_ reader;
	const char *buf;
	int64_t len;
	int64_t read;
};

struct _avro_writer_memory_t {
	struct avro_writer_t_ writer;
	const char *buf;
	int64_t len;
	int64_t written;
};

#define avro_io_typeof(obj)      ((obj)->type)
#define is_memory_io(obj)        (obj && avro_io_typeof(obj) == AVRO_MEMORY_IO)
#define is_file_io(obj)          (obj && avro_io_typeof(obj) == AVRO_FILE_IO)

#define avro_reader_to_memory(reader_)  container_of(reader_, struct _avro_reader_memory_t, reader)
#define avro_reader_to_file(reader_)    container_of(reader_, struct _avro_reader_file_t, reader)
#define avro_writer_to_memory(writer_)  container_of(writer_, struct _avro_writer_memory_t, writer)
#define avro_writer_to_file(writer_)    container_of(writer_, struct _avro_writer_file_t, writer)

static void reader_init(avro_reader_t reader, avro_io_type_t type)
{
	reader->type = type;
	avro_refcount_set(&reader->refcount, 1);
}

static void writer_init(avro_writer_t writer, avro_io_type_t type)
{
	writer->type = type;
	avro_refcount_set(&writer->refcount, 1);
}

avro_reader_t avro_reader_file_fp(FILE * fp, int should_close)
{
	struct _avro_reader_file_t *file_reader =
	    (struct _avro_reader_file_t *) avro_new(struct _avro_reader_file_t);
	if (!file_reader) {
		avro_set_error("Cannot allocate new file reader");
		return NULL;
	}
	memset(file_reader, 0, sizeof(struct _avro_reader_file_t));
	file_reader->fp = fp;
	file_reader->should_close = should_close;
	reader_init(&file_reader->reader, AVRO_FILE_IO);
	return &file_reader->reader;
}

avro_reader_t avro_reader_file(FILE * fp)
{
	return avro_reader_file_fp(fp, 1);
}

avro_writer_t avro_writer_file_fp(FILE * fp, int should_close)
{
	struct _avro_writer_file_t *file_writer =
	    (struct _avro_writer_file_t *) avro_new(struct _avro_writer_file_t);
	if (!file_writer) {
		avro_set_error("Cannot allocate new file writer");
		return NULL;
	}
	file_writer->fp = fp;
	file_writer->should_close = should_close;
	writer_init(&file_writer->writer, AVRO_FILE_IO);
	return &file_writer->writer;
}

avro_writer_t avro_writer_file(FILE * fp)
{
	return avro_writer_file_fp(fp, 1);
}

avro_reader_t avro_reader_memory(const char *buf, int64_t len)
{
	struct _avro_reader_memory_t *mem_reader =
	    (struct _avro_reader_memory_t *) avro_new(struct _avro_reader_memory_t);
	if (!mem_reader) {
		avro_set_error("Cannot allocate new memory reader");
		return NULL;
	}
	mem_reader->buf = buf;
	mem_reader->len = len;
	mem_reader->read = 0;
	reader_init(&mem_reader->reader, AVRO_MEMORY_IO);
	return &mem_reader->reader;
}

void
avro_reader_memory_set_source(avro_reader_t reader, const char *buf, int64_t len)
{
	if (is_memory_io(reader)) {
		struct _avro_reader_memory_t *mem_reader = avro_reader_to_memory(reader);
		mem_reader->buf = buf;
		mem_reader->len = len;
		mem_reader->read = 0;
	}
}

avro_writer_t avro_writer_memory(const char *buf, int64_t len)
{
	struct _avro_writer_memory_t *mem_writer =
	    (struct _avro_writer_memory_t *) avro_new(struct _avro_writer_memory_t);
	if (!mem_writer) {
		avro_set_error("Cannot allocate new memory writer");
		return NULL;
	}
	mem_writer->buf = buf;
	mem_writer->len = len;
	mem_writer->written = 0;
	writer_init(&mem_writer->writer, AVRO_MEMORY_IO);
	return &mem_writer->writer;
}

void
avro_writer_memory_set_dest(avro_writer_t writer, const char *buf, int64_t len)
{
	if (is_memory_io(writer)) {
		struct _avro_writer_memory_t *mem_writer = avro_writer_to_memory(writer);
		mem_writer->buf = buf;
		mem_writer->len = len;
		mem_writer->written = 0;
	}
}

static int
avro_read_memory(struct _avro_reader_memory_t *reader, void *buf, int64_t len)
{
	if (len > 0) {
		if ((reader->len - reader->read) < len) {
			avro_prefix_error("Cannot read %" PRIsz " bytes from memory buffer",
					  (size_t) len);
			return ENOSPC;
		}
		memcpy(buf, reader->buf + reader->read, len);
		reader->read += len;
	}
	return 0;
}

#define bytes_available(reader) (reader->end - reader->cur)
#define buffer_reset(reader) {reader->cur = reader->end = reader->buffer;}

static int
avro_read_file(struct _avro_reader_file_t *reader, void *buf, int64_t len)
{
	int64_t needed = len;
	char *p = (char *) buf;
	int rval;

	if (len == 0) {
		return 0;
	}

	if (needed > (int64_t) sizeof(reader->buffer)) {
		if (bytes_available(reader) > 0) {
			memcpy(p, reader->cur, bytes_available(reader));
			p += bytes_available(reader);
			needed -= bytes_available(reader);
			buffer_reset(reader);
		}
		rval = fread(p, 1, needed, reader->fp);
		if (rval != needed) {
			avro_set_error("Cannot read %" PRIsz " bytes from file",
				       (size_t) needed);
			return EILSEQ;
		}
		return 0;
	} else if (needed <= bytes_available(reader)) {
		memcpy(p, reader->cur, needed);
		reader->cur += needed;
		return 0;
	} else {
		memcpy(p, reader->cur, bytes_available(reader));
		p += bytes_available(reader);
		needed -= bytes_available(reader);

		rval =
		    fread(reader->buffer, 1, sizeof(reader->buffer),
			  reader->fp);
		if (rval == 0) {
			avro_set_error("Cannot read %" PRIsz " bytes from file",
				       (size_t) needed);
			return EILSEQ;
		}
		reader->cur = reader->buffer;
		reader->end = reader->cur + rval;

		if (bytes_available(reader) < needed) {
			avro_set_error("Cannot read %" PRIsz " bytes from file",
				       (size_t) needed);
			return EILSEQ;
		}
		memcpy(p, reader->cur, needed);
		reader->cur += needed;
		return 0;
	}
	avro_set_error("Cannot read %" PRIsz " bytes from file",
		       (size_t) needed);
	return EILSEQ;
}

int avro_read(avro_reader_t reader, void *buf, int64_t len)
{
	if (buf && len >= 0) {
		if (is_memory_io(reader)) {
			return avro_read_memory(avro_reader_to_memory(reader),
						buf, len);
		} else if (is_file_io(reader)) {
			return avro_read_file(avro_reader_to_file(reader), buf,
					      len);
		}
	}
	return EINVAL;
}

static int avro_skip_memory(struct _avro_reader_memory_t *reader, int64_t len)
{
	if (len > 0) {
		if ((reader->len - reader->read) < len) {
			avro_set_error("Cannot skip %" PRIsz " bytes in memory buffer",
				       (size_t) len);
			return ENOSPC;
		}
		reader->read += len;
	}
	return 0;
}

static int avro_skip_file(struct _avro_reader_file_t *reader, int64_t len)
{
	int rval;
	int64_t needed = len;

	if (len == 0) {
		return 0;
	}
	if (needed <= bytes_available(reader)) {
		reader->cur += needed;
	} else {
		needed -= bytes_available(reader);
		buffer_reset(reader);
		rval = fseek(reader->fp, needed, SEEK_CUR);
		if (rval < 0) {
			avro_set_error("Cannot skip %" PRIsz " bytes in file",
				       (size_t) len);
			return rval;
		}
	}
	return 0;
}

int avro_skip(avro_reader_t reader, int64_t len)
{
	if (len >= 0) {
		if (is_memory_io(reader)) {
			return avro_skip_memory(avro_reader_to_memory(reader),
						len);
		} else if (is_file_io(reader)) {
			return avro_skip_file(avro_reader_to_file(reader), len);
		}
	}
	return 0;
}

static int
avro_write_memory(struct _avro_writer_memory_t *writer, void *buf, int64_t len)
{
	if (len) {
		if ((writer->len - writer->written) < len) {
			avro_set_error("Cannot write %" PRIsz " bytes in memory buffer",
				       (size_t) len);
			return ENOSPC;
		}
		memcpy((void *)(writer->buf + writer->written), buf, len);
		writer->written += len;
	}
	return 0;
}

static int
avro_write_file(struct _avro_writer_file_t *writer, void *buf, int64_t len)
{
	int rval;
	if (len > 0) {
		rval = fwrite(buf, len, 1, writer->fp);
		if (rval == 0) {
			return EIO;
		}
	}
	return 0;
}

int avro_write(avro_writer_t writer, void *buf, int64_t len)
{
	if (buf && len >= 0) {
		if (is_memory_io(writer)) {
			return avro_write_memory(avro_writer_to_memory(writer),
						 buf, len);
		} else if (is_file_io(writer)) {
			return avro_write_file(avro_writer_to_file(writer), buf,
					       len);
		}
	}
	return EINVAL;
}

void
avro_reader_reset(avro_reader_t reader)
{
	if (is_memory_io(reader)) {
		avro_reader_to_memory(reader)->read = 0;
	}
}

void avro_writer_reset(avro_writer_t writer)
{
	if (is_memory_io(writer)) {
		avro_writer_to_memory(writer)->written = 0;
	}
}

int64_t avro_writer_tell(avro_writer_t writer)
{
	if (is_memory_io(writer)) {
		return avro_writer_to_memory(writer)->written;
	}
	return EINVAL;
}

void avro_writer_flush(avro_writer_t writer)
{
	if (is_file_io(writer)) {
		fflush(avro_writer_to_file(writer)->fp);
	}
}

void avro_writer_dump(avro_writer_t writer, FILE * fp)
{
	if (is_memory_io(writer)) {
		dump(fp, (char *)avro_writer_to_memory(writer)->buf,
		     avro_writer_to_memory(writer)->written);
	}
}

void avro_reader_dump(avro_reader_t reader, FILE * fp)
{
	if (is_memory_io(reader)) {
		dump(fp, (char *)avro_reader_to_memory(reader)->buf,
		     avro_reader_to_memory(reader)->read);
	}
}

void avro_reader_free(avro_reader_t reader)
{
	if (is_memory_io(reader)) {
		avro_freet(struct _avro_reader_memory_t, reader);
	} else if (is_file_io(reader)) {
		if (avro_reader_to_file(reader)->should_close) {
			fclose(avro_reader_to_file(reader)->fp);
		}
		avro_freet(struct _avro_reader_file_t, reader);
	}
}

void avro_writer_free(avro_writer_t writer)
{
	if (is_memory_io(writer)) {
		avro_freet(struct _avro_writer_memory_t, writer);
	} else if (is_file_io(writer)) {
		if (avro_writer_to_file(writer)->should_close) {
			fclose(avro_writer_to_file(writer)->fp);
		}
		avro_freet(struct _avro_writer_file_t, writer);
	}
}

int avro_reader_is_eof(avro_reader_t reader)
{
	if (is_file_io(reader)) {
		struct _avro_reader_file_t *file = avro_reader_to_file(reader);
		if (feof(file->fp)) {
			return file->cur == file->end;
		}
	}
	return 0;
}
