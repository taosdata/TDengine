#ifndef __STUB_H__
#define __STUB_H__

#ifdef _WIN32 
//windows
#include <windows.h>
#include <processthreadsapi.h>
#else
//linux or macos
#include <unistd.h>
#include <sys/mman.h>
#endif
//c
#include <cstddef>
#include <cstdint>
#include <cstring>
//c++
#include <map>
//valgrind
#ifdef __VALGRIND__
#include <valgrind/valgrind.h>
#endif

#define ADDR(CLASS_NAME,MEMBER_NAME) (&CLASS_NAME::MEMBER_NAME)


/**********************************************************
                  replace function
**********************************************************/
#ifdef __VALGRIND__
#define VALGRIND_CACHE_FLUSH(addr, size) VALGRIND_DISCARD_TRANSLATIONS(addr, size)
#else
#define VALGRIND_CACHE_FLUSH(addr, size)
#endif

#ifdef _WIN32 
#define CACHEFLUSH(addr, size) FlushInstructionCache(GetCurrentProcess(), addr, size);VALGRIND_CACHE_FLUSH(addr, size)
#else
#define CACHEFLUSH(addr, size) __builtin___clear_cache(addr, addr + size);VALGRIND_CACHE_FLUSH(addr, size)
#endif

#if defined(__aarch64__) || defined(_M_ARM64)
    #define CODESIZE 16U
    #define CODESIZE_MIN 16U
    #define CODESIZE_MAX CODESIZE
    // ldr x9, +8 
    // br x9 
    // addr 
    #define REPLACE_FAR(t, fn, fn_stub)\
        ((uint32_t*)fn)[0] = 0x58000040 | 9;\
        ((uint32_t*)fn)[1] = 0xd61f0120 | (9 << 5);\
        *(long long *)(fn + 8) = (long long )fn_stub;\
        CACHEFLUSH((char *)fn, CODESIZE);
    #define REPLACE_NEAR(t, fn, fn_stub) REPLACE_FAR(t, fn, fn_stub)
#elif defined(__arm__) || defined(_M_ARM)
    #define CODESIZE 8U
    #define CODESIZE_MIN 8U
    #define CODESIZE_MAX CODESIZE
    // ldr pc, [pc, #-4]
    #define REPLACE_FAR(t, fn, fn_stub)\
        if ((uintptr_t)fn & 0x00000001) { \
          *(uint16_t *)&f[0] = 0xf8df;\
          *(uint16_t *)&f[2] = 0xf000;\
          *(uint16_t *)&f[4] = (uint16_t)(fn_stub & 0xffff);\
          *(uint16_t *)&f[6] = (uint16_t)(fn_stub >> 16);\
        } else { \
          ((uint32_t*)fn)[0] = 0xe51ff004;\
          ((uint32_t*)fn)[1] = (uint32_t)fn_stub;\
        }\
        CACHEFLUSH((char *)((uintptr_t)fn & 0xfffffffe), CODESIZE);
    #define REPLACE_NEAR(t, fn, fn_stub) REPLACE_FAR(t, fn, fn_stub)
#elif defined(__thumb__) || defined(_M_THUMB)
    #define CODESIZE 12
    #define CODESIZE_MIN 12
    #define CODESIZE_MAX CODESIZE
    // NOP
    // LDR.W PC, [PC]
    #define REPLACE_FAR(t, fn, fn_stub)\
        uint32_t clearBit0 = fn & 0xfffffffe;\
        char *f = (char *)clearBit0;\
        if (clearBit0 % 4 != 0) {\
            *(uint16_t *)&f[0] = 0xbe00;\
        }\
        *(uint16_t *)&f[2] = 0xf8df;\
        *(uint16_t *)&f[4] = 0xf000;\
        *(uint16_t *)&f[6] = (uint16_t)(fn_stub & 0xffff);\
        *(uint16_t *)&f[8] = (uint16_t)(fn_stub >> 16);\
        CACHEFLUSH((char *)f, CODESIZE);
    #define REPLACE_NEAR(t, fn, fn_stub) REPLACE_FAR(t, fn, fn_stub)
#elif defined(__mips64)
    #define CODESIZE 80U
    #define CODESIZE_MIN 80U
    #define CODESIZE_MAX CODESIZE
    //MIPS has no PC pointer, so you need to manually enter and exit the stack
    //120000ce0:  67bdffe0    daddiu  sp, sp, -32  //enter the stack
    //120000ce4:  ffbf0018    sd  ra, 24(sp)
    //120000ce8:  ffbe0010    sd  s8, 16(sp)
    //120000cec:  ffbc0008    sd  gp, 8(sp)
    //120000cf0:  03a0f025    move    s8, sp

    //120000d2c:  03c0e825    move    sp, s8  //exit the stack
    //120000d30:  dfbf0018    ld  ra, 24(sp)
    //120000d34:  dfbe0010    ld  s8, 16(sp)
    //120000d38:  dfbc0008    ld  gp, 8(sp)
    //120000d3c:  67bd0020    daddiu  sp, sp, 32
    //120000d40:  03e00008    jr  ra
    #define REPLACE_FAR(t, fn, fn_stub)\
        ((uint32_t *)fn)[0] = 0x67bdffe0;\
        ((uint32_t *)fn)[1] = 0xffbf0018;\
        ((uint32_t *)fn)[2] = 0xffbe0010;\
        ((uint32_t *)fn)[3] = 0xffbc0008;\
        ((uint32_t *)fn)[4] = 0x03a0f025;\
        *(uint16_t *)(fn + 20) = (long long)fn_stub >> 32;\
        *(fn + 22) = 0x19;\
        *(fn + 23) = 0x24;\
        ((uint32_t *)fn)[6] = 0x0019cc38;\
        *(uint16_t *)(fn + 28) = (long long)fn_stub >> 16;\
        *(fn + 30) = 0x39;\
        *(fn + 31) = 0x37;\
        ((uint32_t *)fn)[8] = 0x0019cc38;\
        *(uint16_t *)(fn + 36) = (long long)fn_stub;\
        *(fn + 38) = 0x39;\
        *(fn + 39) = 0x37;\
        ((uint32_t *)fn)[10] = 0x0320f809;\
        ((uint32_t *)fn)[11] = 0x00000000;\
        ((uint32_t *)fn)[12] = 0x00000000;\
        ((uint32_t *)fn)[13] = 0x03c0e825;\
        ((uint32_t *)fn)[14] = 0xdfbf0018;\
        ((uint32_t *)fn)[15] = 0xdfbe0010;\
        ((uint32_t *)fn)[16] = 0xdfbc0008;\
        ((uint32_t *)fn)[17] = 0x67bd0020;\
        ((uint32_t *)fn)[18] = 0x03e00008;\
        ((uint32_t *)fn)[19] = 0x00000000;\
        CACHEFLUSH((char *)fn, CODESIZE);
    #define REPLACE_NEAR(t, fn, fn_stub) REPLACE_FAR(t, fn, fn_stub)
#elif defined(__riscv) && __riscv_xlen == 64
    #define CODESIZE 24U
    #define CODESIZE_MIN 24U
    #define CODESIZE_MAX CODESIZE
    // absolute offset(64)
    // auipc t1,0
    // addi t1, t1, 16
    // ld t1,0(t1)
    // jalr x0, t1, 0
    // addr
    #define REPLACE_FAR(t, fn, fn_stub)\
        unsigned int auipc = 0x317;\
        *(unsigned int *)(fn) = auipc;\
        unsigned int addi = 0x1030313;\
        *(unsigned int *)(fn + 4) = addi;\
        unsigned int ld = 0x33303;\
        *(unsigned int *)(fn + 8) = ld;\
        unsigned int jalr = 0x30067;\
        *(unsigned int *)(fn + 12) = jalr;\
        *(unsigned long long*)(fn + 16) = (unsigned long long)fn_stub;\
        CACHEFLUSH((char *)fn, CODESIZE);
    #define REPLACE_NEAR(t, fn, fn_stub) REPLACE_FAR(t, fn, fn_stub)
#elif defined(__riscv) && __riscv_xlen == 32
    #define CODESIZE 20U
    #define CODESIZE_MIN 20U
    #define CODESIZE_MAX CODESIZE
    // absolute offset(32)
    // auipc t1,0
    // addi t1, t1, 16
    // lw t1,0(t1)
    // jalr x0, t1, 0
    // addr
    #define REPLACE_FAR(t, fn, fn_stub)\
        unsigned int auipc = 0x317;\
        *(unsigned int *)(fn) = auipc;\
        unsigned int addi = 0x1030313;\
        *(unsigned int *)(fn + 4) = addi;\
        unsigned int lw = 0x32303;\
        *(unsigned int *)(fn + 8) = lw;\
        unsigned int jalr = 0x30067;\
        *(unsigned int *)(fn + 12) = jalr;\
        *(unsigned int*)(fn + 16) = (unsigned int)fn_stub;\
        CACHEFLUSH((char *)fn, CODESIZE);
    #define REPLACE_NEAR(t, fn, fn_stub) REPLACE_FAR(t, fn, fn_stub)
#elif defined(__loongarch64) 
    #define CODESIZE 20U
    #define CODESIZE_MIN 20U
    #define CODESIZE_MAX CODESIZE
    // absolute offset(64)
    // PCADDI rd, si20 | 0 0 0 1 1 0 0 si20 rd
    // LD.D rd, rj, si12 | 0 0 1 0 1 0 0 0 1 1 si12 rj rd
    // JIRL rd, rj, offs | 0 1 0 0 1 1 offs[15:0] rj rd
    // addr
    #define REPLACE_FAR(t, fn, fn_stub)\
        unsigned int rd = 17;\
        unsigned int off = 12 >> 2;\
        unsigned int pcaddi = 0x0c << (32 - 7) | off << 5 | rd ;\
        rd = 17;\
        int rj = 17;\
        off = 0;\
        unsigned int ld_d = 0xa3 << 22 | off << 10 | rj << 5 | rd ;\
        rd = 0;\
        rj = 17;\
        off = 0;\
        unsigned int jirl = 0x13 << 26 | off << 10 | rj << 5| rd;\
        *(unsigned int *)fn = pcaddi;\
        *(unsigned int *)(fn + 4) = ld_d;\
        *(unsigned int *)(fn + 8) = jirl;\
        *(unsigned long long*)(fn + 12) = (unsigned long long)fn_stub;\
        CACHEFLUSH((char *)fn, CODESIZE);
    #define REPLACE_NEAR(t, fn, fn_stub) REPLACE_FAR(t, fn, fn_stub)
#elif defined(__powerpc64__)
    #define CODESIZE 20U
    #define CODESIZE_MIN 20U
    #define CODESIZE_MAX CODESIZE
    // lis r12, fn_stub@highest
    // ori r12, r12, fn_stub@higher
    // rldicr r12, r12, 32, 31
    // ori r12, r12, fn_stub@high
    // ori r12, r12, fn_stub@l
    // mtctr r12
    // bctr
    #define REPLACE_FAR(t, fn, fn_stub)\
        ((uint32_t*)fn)[0] = 0x3c000000 | (((uintptr_t)fn_stub >> 48) & 0xffff);\
        ((uint32_t*)fn)[1] = 0x60000000 | (((uintptr_t)fn_stub >> 32) & 0xffff);\
        ((uint32_t*)fn)[2] = 0x78000000 | ((((uintptr_t)fn_stub >> 32) & 0xffff) << 16);\
        ((uint32_t*)fn)[3] = 0x60000000 | (((uintptr_t)fn_stub >> 16) & 0xffff);\
        ((uint32_t*)fn)[4] = 0x60000000 | ((uintptr_t)fn_stub & 0xffff);\
        ((uint32_t*)fn)[5] = 0x7d8903a6;\
        ((uint32_t*)fn)[6] = 0x4e800420;\
        CACHEFLUSH((char *)fn, CODESIZE);
    #define REPLACE_NEAR(t, fn, fn_stub) REPLACE_FAR(t, fn, fn_stub)
#elif defined(__alpha__)
    #define CODESIZE 16U
    #define CODESIZE_MIN 16U
    #define CODESIZE_MAX CODESIZE
    // ldah t12, high(fn_stub)
    // lda t12, low(fn_stub)(t12)
    // jmp zero, (t12), 0
    #define REPLACE_FAR(t, fn, fn_stub)\
        ((uint32_t*)fn)[0] = 0x279f0000 | (((uintptr_t)fn_stub >> 32) & 0xffff);\
        ((uint32_t*)fn)[1] = 0x201f0000 | ((uintptr_t)fn_stub & 0xffff);\
        ((uint32_t*)fn)[2] = 0x6bfb0000;\
        CACHEFLUSH((char *)fn, CODESIZE);
    #define REPLACE_NEAR(t, fn, fn_stub) REPLACE_FAR(t, fn, fn_stub)
#elif defined(__sparc__) && defined(__arch64__)
    #define CODESIZE 24U
    #define CODESIZE_MIN 24U
    #define CODESIZE_MAX CODESIZE
    // sethi %hi(fn_stub), %g1
    // jmp %g1 + %lo(fn_stub)
    // nop
    #define REPLACE_FAR(t, fn, fn_stub)\
        ((uint32_t*)fn)[0] = 0x03000000 | (((uintptr_t)fn_stub >> 42) & 0x3fffff);\
        ((uint32_t*)fn)[1] = 0x81c06000 | (((uintptr_t)fn_stub >> 32) & 0x3ff);\
        ((uint32_t*)fn)[2] = 0x01000000;\
        CACHEFLUSH((char *)fn, CODESIZE);
    #define REPLACE_NEAR(t, fn, fn_stub) REPLACE_FAR(t, fn, fn_stub)
#elif defined(__sw_64__)
    #define CODESIZE 12U
    #define CODESIZE_MIN 12U
    #define CODESIZE_MAX CODESIZE
    // bis zero, zero, v0
    // ldq v0, fn_stub
    // jmp zero, (v0)
    #define REPLACE_FAR(t, fn, fn_stub)\
        ((uint32_t*)fn)[0] = 0x20000000;\
        ((uint32_t*)fn)[1] = 0xd2000000 | ((uintptr_t)fn_stub & 0xffffffff);\
        ((uint32_t*)fn)[2] = 0x6bfb0000;\
        CACHEFLUSH((char *)fn, CODESIZE);
    #define REPLACE_NEAR(t, fn, fn_stub) REPLACE_FAR(t, fn, fn_stub)
#elif defined(__s390x__)
    #define CODESIZE 16U
    #define CODESIZE_MIN 16U
    #define CODESIZE_MAX CODESIZE
    // lgrl %r1, fn_stub
    // br %r1
    #define REPLACE_FAR(t, fn, fn_stub)\
            ((uint32_t*)fn)[0] = 0xc0200000 | (1 << 20) | (0x0);\
            ((uint32_t*)fn)[1] = 0x07f10000;\
            *(uint64_t *)(fn + 8) = (uint64_t)fn_stub;\
            CACHEFLUSH((char *)fn, CODESIZE);
    #define REPLACE_NEAR(t, fn, fn_stub) REPLACE_FAR(t, fn, fn_stub)
#else //__i386__ _x86_64__  _M_IX86 _M_X64
    #define CODESIZE 13U
    #define CODESIZE_MIN 5U
    #define CODESIZE_MAX CODESIZE
    //13 byte(jmp m16:64)
    //movabs $0x102030405060708,%r11
    //jmpq   *%r11
    #define REPLACE_FAR(t, fn, fn_stub)\
        *fn = 0x49;\
        *(fn + 1) = 0xbb;\
        *(long long *)(fn + 2) = (long long)fn_stub;\
        *(fn + 10) = 0x41;\
        *(fn + 11) = 0xff;\
        *(fn + 12) = 0xe3;\
        CACHEFLUSH((char *)fn, CODESIZE);
    //5 byte(jmp rel32)
    #define REPLACE_NEAR(t, fn, fn_stub)\
        *fn = 0xE9;\
        *(int *)(fn + 1) = (int)(fn_stub - fn - CODESIZE_MIN);\
        CACHEFLUSH((char *)fn, CODESIZE);
#endif

struct func_stub
{
    unsigned char *fn;
    unsigned char code_buf[CODESIZE];
    bool far_jmp;
};

class Stub
{
public:
    Stub()
    {
#ifdef _WIN32
        SYSTEM_INFO sys_info;  
        GetSystemInfo(&sys_info);
        m_pagesize = sys_info.dwPageSize;
#else
        m_pagesize = sysconf(_SC_PAGE_SIZE);
#endif       

        if (m_pagesize < 0)
        {
            m_pagesize = 4096;
        }
    }
    ~Stub()
    {
        clear();
    }
    void clear()
    {
        std::map<unsigned char*,func_stub*>::iterator iter;
        struct func_stub *pstub;
        for(iter=m_result.begin(); iter != m_result.end(); iter++)
        {
            pstub = iter->second;
#ifdef _WIN32
            DWORD lpflOldProtect;
            if(0 != VirtualProtect(pageof(pstub->fn), m_pagesize * 2, PAGE_EXECUTE_READWRITE, &lpflOldProtect))
#else
            if (0 == mprotect(pageof(pstub->fn), m_pagesize * 2, PROT_READ | PROT_WRITE | PROT_EXEC))
#endif       
            {

                if(pstub->far_jmp)
                {
                    std::memcpy(pstub->fn, pstub->code_buf, CODESIZE_MAX);
                }
                else
                {
                    std::memcpy(pstub->fn, pstub->code_buf, CODESIZE_MIN);
                }

                CACHEFLUSH((char *)pstub->fn, CODESIZE);

#ifdef _WIN32
                VirtualProtect(pageof(pstub->fn), m_pagesize * 2, PAGE_EXECUTE_READ, &lpflOldProtect);
#else
                mprotect(pageof(pstub->fn), m_pagesize * 2, PROT_READ | PROT_EXEC);
#endif     
            }

            iter->second  = NULL;
            delete pstub;        
        }
        
        return;
    }
    template<typename T,typename S>
    void set(T addr, S addr_stub)
    {
        unsigned char * fn;
        unsigned char * fn_stub;
        fn = addrof(addr);
        fn_stub = addrof(addr_stub);
        struct func_stub *pstub;
        pstub = new func_stub;
        //start
        reset(fn); // 
        pstub->fn = fn;

        if(distanceof(fn, fn_stub))
        {
            pstub->far_jmp = true;
            std::memcpy(pstub->code_buf, fn, CODESIZE_MAX);
        }
        else
        {
            pstub->far_jmp = false;
            std::memcpy(pstub->code_buf, fn, CODESIZE_MIN);
        }

#ifdef _WIN32
        DWORD lpflOldProtect;
        if(0 == VirtualProtect(pageof(pstub->fn), m_pagesize * 2, PAGE_EXECUTE_READWRITE, &lpflOldProtect))
#else
        if (-1 == mprotect(pageof(pstub->fn), m_pagesize * 2, PROT_READ | PROT_WRITE | PROT_EXEC))
#endif       
        {
            throw("stub set memory protect to w+r+x faild");
        }

        if(pstub->far_jmp)
        {
            REPLACE_FAR(this, fn, fn_stub);
        }
        else
        {
            REPLACE_NEAR(this, fn, fn_stub);
        }

#ifdef _WIN32
        if(0 == VirtualProtect(pageof(pstub->fn), m_pagesize * 2, PAGE_EXECUTE_READ, &lpflOldProtect))
#else
        if (-1 == mprotect(pageof(pstub->fn), m_pagesize * 2, PROT_READ | PROT_EXEC))
#endif     
        {
            throw("stub set memory protect to r+x failed");
        }
        m_result.insert(std::pair<unsigned char*,func_stub*>(fn,pstub));
        return;
    }

    template<typename T>
    void reset(T addr)
    {
        unsigned char * fn;
        fn = addrof(addr);
        
        std::map<unsigned char*,func_stub*>::iterator iter = m_result.find(fn);
        
        if (iter == m_result.end())
        {
            return;
        }
        struct func_stub *pstub;
        pstub = iter->second;
        
#ifdef _WIN32
        DWORD lpflOldProtect;
        if(0 == VirtualProtect(pageof(pstub->fn), m_pagesize * 2, PAGE_EXECUTE_READWRITE, &lpflOldProtect))
#else
        if (-1 == mprotect(pageof(pstub->fn), m_pagesize * 2, PROT_READ | PROT_WRITE | PROT_EXEC))
#endif       
        {
            throw("stub reset memory protect to w+r+x faild");
        }

        if(pstub->far_jmp)
        {
            std::memcpy(pstub->fn, pstub->code_buf, CODESIZE_MAX);
        }
        else
        {
            std::memcpy(pstub->fn, pstub->code_buf, CODESIZE_MIN);
        }

        CACHEFLUSH((char *)pstub->fn, CODESIZE);


#ifdef _WIN32
        if(0 == VirtualProtect(pageof(pstub->fn), m_pagesize * 2, PAGE_EXECUTE_READ, &lpflOldProtect))
#else
        if (-1 == mprotect(pageof(pstub->fn), m_pagesize * 2, PROT_READ | PROT_EXEC))
#endif     
        {
            throw("stub reset memory protect to r+x failed");
        }
        m_result.erase(iter);
        delete pstub;
        
        return;
    }
private:
    void *pageof(unsigned char* addr)
    { 
#ifdef _WIN32
        return (void *)((unsigned long long)addr & ~(m_pagesize - 1));
#else
        return (void *)((unsigned long)addr & ~(m_pagesize - 1));
#endif   
    }

    template<typename T>
    unsigned char* addrof(T addr)
    {
        union 
        {
          T _s;
          unsigned char* _d;
        }ut;
        ut._s = addr;
        return ut._d;
    }

    bool distanceof(unsigned char* addr, unsigned char* addr_stub)
    {
        std::ptrdiff_t diff = addr_stub >= addr ? addr_stub - addr : addr - addr_stub;
        if((sizeof(addr) > 4) && (((diff >> 31) - 1) > 0))
        {
            return true;
        }
        return false;
    }

private:
#ifdef _WIN32
    //LLP64
    long long m_pagesize;
#else
    //LP64
    long m_pagesize;
#endif   
    std::map<unsigned char*, func_stub*> m_result;
    
};

#endif
