/**
 *  @file callZlib.c
 *  @author Sheng Di
 *  @date June, 2016
 *  @brief gzip compressor code: the interface to call zlib
 *  (C) 2016 by Mathematics and Computer Science (MCS), Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */


#include <stdio.h>
#include <stdlib.h>
#include <zlib.h>
#include <sz.h>

#if MAX_MEM_LEVEL >= 8
#define DEF_MEM_LEVEL 8
#else
#define DEF_MEM_LEVEL MAX_MEM_LEVEL
#endif


#define CHECK_ERR(err, msg) { \
    if (err != Z_OK && err != Z_STREAM_END) { \
        fprintf(stderr, "%s error: %d\n", msg, err); \
        return SZ_NSCS; \
    } \
}

int isZlibFormat(unsigned char magic1, unsigned char magic2)
{
	if(magic1==104&&magic2==5) //DC+BS
		return 1;
	if(magic1==104&&magic2==129) //DC+DC
		return 1;
	if(magic1==104&&magic2==222) //DC+BC
		return 1;		
	if(magic1==120&&magic2==1) //BC+BS
		return 1;
	if(magic1==120&&magic2==94) //BC+? 
		return 1;		
	if(magic1==120&&magic2==156) //BC+DC
		return 1;
	if(magic1==120&&magic2==218) //BC+BS
		return 1;
	return 0;
}

/*zlib_compress() is only valid for median-size data compression. */
unsigned long zlib_compress(unsigned char* data, unsigned long dataLength, unsigned char** compressBytes, int level)
{	
	z_stream stream = {0};

    stream.next_in = data;
    stream.avail_in = dataLength;
#ifdef MAXSEG_64K
    /* Check for source > 64K on 16-bit machine: */
    if ((uLong)stream.avail_in != dataLength) return Z_BUF_ERROR;
#endif

    uLong estCmpLen = deflateBound(&stream, dataLength);	
	unsigned long outSize = estCmpLen;
    	
	*compressBytes = (unsigned char*)malloc(sizeof(unsigned char)*estCmpLen);
	int err = compress2(*compressBytes, &outSize, data, dataLength, level);
	if(err!=Z_OK)
	{
		printf("Error: err_code=%d; the reason may be your data size is too large (>=2^32), which cannot be compressed by standalone zlib_compress. Sol: inflace_init, ....\n", err);
		exit(0);
	}
	return outSize;
}

unsigned long zlib_compress2(unsigned char* data, unsigned long dataLength, unsigned char** compressBytes, int level)
{
	unsigned long outSize;
	
	z_stream stream = {0};
    int err;

    stream.next_in = data;
    stream.avail_in = dataLength;
#ifdef MAXSEG_64K
    /* Check for source > 64K on 16-bit machine: */
    if ((uLong)stream.avail_in != dataLength) return Z_BUF_ERROR;
#endif

    uLong estCmpLen = deflateBound(&stream, dataLength);
	*compressBytes = (unsigned char*)malloc(sizeof(unsigned char)*estCmpLen);

    stream.next_out = *compressBytes;
    stream.avail_out = estCmpLen;
    //stream.avail_out = dataLength*10;
    //if ((uLong)stream.avail_out != dataLength*10) return Z_BUF_ERROR;

    stream.zalloc = (alloc_func)0;
    stream.zfree = (free_func)0;
    stream.opaque = (voidpf)0;
//	stream.data_type = Z_TEXT;

    //err = deflateInit(&stream, level); //default  windowBits == 15.
    int windowBits = 14; //8-15
    if(confparams_cpr->szMode==SZ_BEST_COMPRESSION)
		windowBits = 15;
	
    err = deflateInit2(&stream, level, Z_DEFLATED, windowBits, DEF_MEM_LEVEL,
                         Z_DEFAULT_STRATEGY);//Z_FIXED); //Z_DEFAULT_STRATEGY
    if (err != Z_OK) return err;

    err = deflate(&stream, Z_FINISH);
    if (err != Z_STREAM_END) {
        deflateEnd(&stream);
        return err == Z_OK ? Z_BUF_ERROR : err;
    }

    err = deflateEnd(&stream);
    
    outSize = stream.total_out;
    return outSize;
}

unsigned long zlib_compress3(unsigned char* data, unsigned long dataLength, unsigned char* compressBytes, int level)
{
	unsigned long outSize = 0;

	z_stream stream = {0};
    int err;

    stream.next_in = data;
    stream.avail_in = dataLength;
#ifdef MAXSEG_64K
    /* Check for source > 64K on 16-bit machine: */
    if ((uLong)stream.avail_in != dataLength) return Z_BUF_ERROR;
#endif

    stream.next_out = compressBytes;
    stream.avail_out = dataLength;
    stream.zalloc = (alloc_func)0;
    stream.zfree = (free_func)0;
    stream.opaque = (voidpf)0;

    //err = deflateInit(&stream, level); //default  windowBits == 15.
    int windowBits = 14; //8-15
    if(confparams_cpr->szMode==SZ_BEST_COMPRESSION)
		windowBits = 15;

    err = deflateInit2(&stream, level, Z_DEFLATED, windowBits, DEF_MEM_LEVEL,
                         Z_DEFAULT_STRATEGY);//Z_FIXED); //Z_DEFAULT_STRATEGY
    if (err != Z_OK) return err;

    err = deflate(&stream, Z_FINISH);
    if (err != Z_STREAM_END) {
        deflateEnd(&stream);
        return err == Z_OK ? Z_BUF_ERROR : err;
    }

    err = deflateEnd(&stream);

    outSize = stream.total_out;
    return outSize;
}

unsigned long zlib_compress4(unsigned char* data, unsigned long dataLength, unsigned char** compressBytes, int level)
{
    z_stream c_stream = {0}; /* compression stream */
    int err = 0;

    c_stream.zalloc = (alloc_func)0;
    c_stream.zfree = (free_func)0;
    c_stream.opaque = (voidpf)0;

    int windowBits = 14; //8-15
    if(confparams_cpr->szMode==SZ_BEST_COMPRESSION)
		windowBits = 15;
    
    err = deflateInit2(&c_stream, level, Z_DEFLATED, windowBits, DEF_MEM_LEVEL,
                         Z_DEFAULT_STRATEGY);//Z_FIXED); //Z_DEFAULT_STRATEGY
    CHECK_ERR(err, "deflateInit");

    uLong estCmpLen = deflateBound(&c_stream, dataLength);
	*compressBytes = (unsigned char*)malloc(sizeof(unsigned char)*estCmpLen);	

    c_stream.next_in  = data;
    c_stream.next_out = *compressBytes;

    while (c_stream.total_in < dataLength && c_stream.total_out < estCmpLen) {
        c_stream.avail_in = c_stream.avail_out = SZ_ZLIB_BUFFER_SIZE; /* force small buffers */
        err = deflate(&c_stream, Z_NO_FLUSH);
        CHECK_ERR(err, "deflate");
    }
    /* Finish the stream, still forcing small buffers: */
    for (;;) {
        c_stream.avail_out = 1;
        err = deflate(&c_stream, Z_FINISH);
        if (err == Z_STREAM_END) break;
        CHECK_ERR(err, "deflate");
    }

    err = deflateEnd(&c_stream);
    CHECK_ERR(err, "deflateEnd");
    
    return c_stream.total_out;	
}

unsigned long zlib_compress5(unsigned char* data, unsigned long dataLength, unsigned char** compressBytes, int level)
{
	int ret, flush;
	unsigned have;
	z_stream strm;
	unsigned char* in = data;

	/* allocate deflate state */
	strm.zalloc = Z_NULL;
	strm.zfree = Z_NULL;
	strm.opaque = Z_NULL;
	ret = deflateInit(&strm, level);
	//int windowBits = 15;
    //ret = deflateInit2(&strm, level, Z_DEFLATED, windowBits, DEF_MEM_LEVEL, Z_DEFAULT_STRATEGY);//Z_FIXED); //Z_DEFAULT_STRATEGY

	if (ret != Z_OK)
		return ret;

	size_t p_size = 0, av_in = 0;
    uLong estCmpLen = deflateBound(&strm, dataLength);
   	*compressBytes = (unsigned char*)malloc(sizeof(unsigned char)*estCmpLen);	
	unsigned char* out = *compressBytes; 

	/* compress until end of file */
	do {		
		p_size += SZ_ZLIB_BUFFER_SIZE;
		if(p_size>=dataLength)
		{
			av_in = dataLength - (p_size - SZ_ZLIB_BUFFER_SIZE);
			flush = Z_FINISH;
		}
		else
		{
			av_in = SZ_ZLIB_BUFFER_SIZE;
			flush = Z_NO_FLUSH;
		}
		strm.avail_in = av_in;
		strm.next_in = in;

		/* run deflate() on input until output buffer not full, finish
		   compression if all of source has been read in */
		do {
			strm.avail_out = SZ_ZLIB_BUFFER_SIZE;
			strm.next_out = out;
			ret = deflate(&strm, flush);    /* no bad return value */

			have = SZ_ZLIB_BUFFER_SIZE - strm.avail_out;
			out += have;
		} while (strm.avail_out == 0);

		in+=av_in;

		/* done when last data in file processed */
	} while (flush != Z_FINISH);

	/* clean up and return */
	(void)deflateEnd(&strm);	
	
	return strm.total_out;	
}

unsigned long zlib_uncompress(unsigned char* compressBytes, unsigned long cmpSize, unsigned char** oriData, unsigned long targetOriSize)
{
	unsigned long outSize = targetOriSize;
	*oriData = (unsigned char*)malloc(sizeof(unsigned char)*targetOriSize);	
	int status = uncompress(*oriData, &outSize, compressBytes, cmpSize); 
	if(status!=Z_OK)
	{
		printf("Error: Zlib decompression error; status=%d\n", status);
		exit(0);
	}
	
	return outSize;
}

unsigned long zlib_uncompress2 (unsigned char* compressBytes, unsigned long cmpSize, unsigned char** oriData, unsigned long targetOriSize)
{
    z_stream stream = {0};

	unsigned long outSize;
	*oriData = (unsigned char*)malloc(sizeof(unsigned char)*targetOriSize);

    stream.zalloc = Z_NULL;
    stream.zfree = Z_NULL;
    stream.opaque = Z_NULL;
//	stream.data_type = Z_TEXT;

    stream.next_in = compressBytes;
    stream.avail_in = cmpSize;
    /* Check for source > 64K on 16-bit machine: */
    if ((unsigned long)stream.avail_in != cmpSize) 
    {
		printf("Error: zlib_uncompress2: stream.avail_in != cmpSize");
		//exit(1);
		return SZ_NSCS; //-1
	}

    stream.next_out = *oriData;
    stream.avail_out = targetOriSize;
    //if ((uLong)stream.avail_out != *destLen) return Z_BUF_ERROR;

    int err = inflateInit(&stream);
    //int windowBits = 15;
    //int err = inflateInit2(&stream, windowBits);
    if (err != Z_OK)
    {
		printf("Error: zlib_uncompress2: err != Z_OK\n");
		return SZ_NSCS;
	}

    err = inflate(&stream, Z_FINISH);
    if (err != Z_STREAM_END) {
        inflateEnd(&stream);
        if (err == Z_NEED_DICT || (err == Z_BUF_ERROR && stream.avail_in == 0))
            return Z_DATA_ERROR;
        return err;
    }
    outSize = stream.total_out;
    inflateEnd(&stream);
    return outSize;
}

unsigned long zlib_uncompress3(unsigned char* compressBytes, unsigned long cmpSize, unsigned char** oriData, unsigned long targetOriSize)
{
	int status;
	z_stream z_strm; /* decompression stream */
	
	size_t nalloc = 65536*4;

	*oriData = (unsigned char*)malloc(sizeof(unsigned char)*targetOriSize);		
	memset(&z_strm, 0, sizeof(z_strm));


    /*d_stream.zalloc = (alloc_func)0;
    d_stream.zfree = (free_func)0;
    d_stream.opaque = (voidpf)0;*/

	z_strm.next_in  = compressBytes;
	z_strm.avail_in = 0;
	z_strm.next_out = *oriData;
	z_strm.avail_out = targetOriSize;
	
	status = inflateInit(&z_strm);
	CHECK_ERR(status, "inflateInit");
	
	do{
		z_strm.avail_in = z_strm.avail_out = SZ_ZLIB_BUFFER_SIZE; /* force small buffers */		
		/* Uncompress some data */
		status = inflate(&z_strm, Z_SYNC_FLUSH);
		
		/* Check if we are done uncompressing data */
		if (Z_STREAM_END==status)
			break;  /*done*/				

		if (Z_OK!=status) {
			(void)inflateEnd(&z_strm);
			printf("Error: inflate() failed\n");
			exit(0);
		}	
		else
		{
			/* If we're not done and just ran out of buffer space, get more */
			if(0 == z_strm.avail_out) {
				void *new_outbuf;         /* Pointer to new output buffer */

				/* Allocate a buffer twice as big */
				nalloc *= 2;
				if(NULL == (new_outbuf = realloc(*oriData, nalloc))) {
					(void)inflateEnd(&z_strm);
					printf("Error: memory allocation failed for deflate uncompression\n");
					exit(0);
				} /* end if */
				*oriData = new_outbuf;

				/* Update pointers to buffer for next set of uncompressed data */
				z_strm.next_out = (*oriData) + z_strm.total_out;
				z_strm.avail_out = (uInt)(nalloc - z_strm.total_out);
			} /* end if */			
		} /* end else*/
	}while(status==Z_OK);

	status = inflateEnd(&z_strm);
	CHECK_ERR(status, "inflateEnd");

	return z_strm.total_out;
}

unsigned long zlib_uncompress4(unsigned char* compressBytes, unsigned long cmpSize, unsigned char** oriData, unsigned long targetOriSize)
{
    int ret;
    unsigned int have;
    z_stream strm;
    unsigned char *in = compressBytes;
    unsigned char *out;

	*oriData = (unsigned char*)malloc(sizeof(unsigned char)*targetOriSize);		
	out = *oriData;

    /* allocate inflate state */
    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    strm.avail_in = 0;
    strm.next_in = Z_NULL;
    ret = inflateInit(&strm);
    if (ret != Z_OK)
	{
        return ret;
	}

	size_t p_size = 0, av_in = 0;
    /* decompress until deflate stream ends or end of file */
    do {
		p_size += SZ_ZLIB_BUFFER_SIZE;
		if(p_size>cmpSize)
			av_in = cmpSize - (p_size - SZ_ZLIB_BUFFER_SIZE);
		else
			av_in = SZ_ZLIB_BUFFER_SIZE;
		strm.avail_in = av_in;
        
        if (strm.avail_in == 0)
            break;
        strm.next_in = in;

        /* run inflate() on input until output buffer not full */
        do {
            strm.avail_out = SZ_ZLIB_BUFFER_SIZE;
            strm.next_out = out;
            ret = inflate(&strm, Z_NO_FLUSH);
            //assert(ret != Z_STREAM_ERROR);  /* state not clobbered */
            switch (ret) {
            case Z_NEED_DICT:
                ret = Z_DATA_ERROR;     /* and fall through */
            case Z_DATA_ERROR:
            case Z_MEM_ERROR:
                (void)inflateEnd(&strm);
                return ret;
            }
            have = SZ_ZLIB_BUFFER_SIZE - strm.avail_out;
            
            out += have;

        } while (strm.avail_out == 0);
		
		in+=av_in;
        /* done when inflate() says it's done */
    } while (ret != Z_STREAM_END);

    /* clean up and return */
    (void)inflateEnd(&strm);
    
    return strm.total_out;	
}

unsigned long zlib_uncompress65536bytes(unsigned char* compressBytes, unsigned long cmpSize, unsigned char** oriData)
{
	int err;
	unsigned long targetOriSize = 65536;
	z_stream d_stream = {0}; /* decompression stream */

	*oriData = (unsigned char*)malloc(sizeof(unsigned char)*targetOriSize);

    d_stream.zalloc = (alloc_func)0;
    d_stream.zfree = (free_func)0;
    d_stream.opaque = (voidpf)0;

	d_stream.next_in  = compressBytes;
	d_stream.avail_in = 0;
	d_stream.next_out = *oriData;

	err = inflateInit(&d_stream);
	CHECK_ERR(err, "inflateInit");

	while (d_stream.total_out < targetOriSize && d_stream.total_in < cmpSize) {
		d_stream.avail_in = d_stream.avail_out = SZ_ZLIB_BUFFER_SIZE; /* force small buffers */
		//err = inflate(&d_stream, Z_NO_FLUSH);
		err = inflate(&d_stream, Z_SYNC_FLUSH);
		if (err == Z_STREAM_END) break;
		if(err<0)
			break;
	}
	
	if(err<0)
		return d_stream.total_out;
	err = inflateEnd(&d_stream);
	
	CHECK_ERR(err, "inflateEnd");

	return d_stream.total_out;
}

unsigned long zlib_uncompress5(unsigned char* compressBytes, unsigned long cmpSize, unsigned char** oriData, unsigned long targetOriSize)
{
	int err;
	z_stream d_stream = {0}; /* decompression stream */

	*oriData = (unsigned char*)malloc(sizeof(unsigned char)*targetOriSize);		

    d_stream.zalloc = (alloc_func)0;
    d_stream.zfree = (free_func)0;
    d_stream.opaque = (voidpf)0;

	d_stream.next_in  = compressBytes;
	d_stream.avail_in = 0;
	d_stream.next_out = *oriData;

	err = inflateInit(&d_stream);
	CHECK_ERR(err, "inflateInit");

	while (d_stream.total_out < targetOriSize && d_stream.total_in < cmpSize) {
		d_stream.avail_in = d_stream.avail_out = SZ_ZLIB_BUFFER_SIZE; /* force small buffers */
		//err = inflate(&d_stream, Z_NO_FLUSH);
		err = inflate(&d_stream, Z_SYNC_FLUSH);
		if (err == Z_STREAM_END) break;
		CHECK_ERR(err, "inflate");
	}
	
	err = inflateEnd(&d_stream);
	
	CHECK_ERR(err, "inflateEnd");

	return d_stream.total_out;
}
