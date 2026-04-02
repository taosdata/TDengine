
/*** Start of inlined file: elfio_dump.hpp ***/
#ifndef ELFIO_DUMP_HPP
#define ELFIO_DUMP_HPP

#include <algorithm>
#include <string>
#include <ostream>
#include <sstream>
#include <iomanip>

/*** Start of inlined file: elfio.hpp ***/
#ifndef ELFIO_HPP
#define ELFIO_HPP

#ifdef _MSC_VER
#pragma warning( push )
#pragma warning( disable : 4996 )
#pragma warning( disable : 4355 )
#pragma warning( disable : 4244 )
#endif

#include <string>
#include <iostream>
#include <fstream>
#include <functional>
#include <algorithm>
#include <vector>
#include <deque>
#include <iterator>


/*** Start of inlined file: elf_types.hpp ***/
#ifndef ELFTYPES_H
#define ELFTYPES_H

#ifndef ELFIO_NO_OWN_TYPES
#if !defined( ELFIO_NO_CSTDINT ) && !defined( ELFIO_NO_INTTYPES )
#include <stdint.h>
#else
typedef unsigned char    uint8_t;
typedef signed char      int8_t;
typedef unsigned short   uint16_t;
typedef signed short     int16_t;
#ifdef _MSC_VER
typedef unsigned __int32 uint32_t;
typedef signed __int32   int32_t;
typedef unsigned __int64 uint64_t;
typedef signed __int64   int64_t;
#else
typedef unsigned int       uint32_t;
typedef signed int         int32_t;
typedef unsigned long long uint64_t;
typedef signed long long   int64_t;
#endif // _MSC_VER
#endif // ELFIO_NO_CSTDINT
#endif // ELFIO_NO_OWN_TYPES

namespace ELFIO {

// Attention! Platform depended definitions.
typedef uint16_t Elf_Half;
typedef uint32_t Elf_Word;
typedef int32_t  Elf_Sword;
typedef uint64_t Elf_Xword;
typedef int64_t  Elf_Sxword;

typedef uint32_t Elf32_Addr;
typedef uint32_t Elf32_Off;
typedef uint64_t Elf64_Addr;
typedef uint64_t Elf64_Off;

#define Elf32_Half  Elf_Half
#define Elf64_Half  Elf_Half
#define Elf32_Word  Elf_Word
#define Elf64_Word  Elf_Word
#define Elf32_Sword Elf_Sword
#define Elf64_Sword Elf_Sword

///////////////////////
// ELF Header Constants

// File type
#define ET_NONE   0
#define ET_REL    1
#define ET_EXEC   2
#define ET_DYN    3
#define ET_CORE   4
#define ET_LOOS   0xFE00
#define ET_HIOS   0xFEFF
#define ET_LOPROC 0xFF00
#define ET_HIPROC 0xFFFF

#define EM_NONE  0 // No machine
#define EM_M32   1 // AT&T WE 32100
#define EM_SPARC 2 // SUN SPARC
#define EM_386   3 // Intel 80386
#define EM_68K   4 // Motorola m68k family
#define EM_88K   5 // Motorola m88k family
#define EM_486   6 // Intel 80486// Reserved for future use
#define EM_860   7 // Intel 80860
#define EM_MIPS  8 // MIPS R3000 (officially, big-endian only)
#define EM_S370  9 // IBM System/370
#define EM_MIPS_RS3_LE \
	10 // MIPS R3000 little-endian (Oct 4 1999 Draft) Deprecated
#define EM_res011      11 // Reserved
#define EM_res012      12 // Reserved
#define EM_res013      13 // Reserved
#define EM_res014      14 // Reserved
#define EM_PARISC      15 // HPPA
#define EM_res016      16 // Reserved
#define EM_VPP550      17 // Fujitsu VPP500
#define EM_SPARC32PLUS 18 // Sun's "v8plus"
#define EM_960         19 // Intel 80960
#define EM_PPC         20 // PowerPC
#define EM_PPC64       21 // 64-bit PowerPC
#define EM_S390        22 // IBM S/390
#define EM_SPU         23 // Sony/Toshiba/IBM SPU
#define EM_res024      24 // Reserved
#define EM_res025      25 // Reserved
#define EM_res026      26 // Reserved
#define EM_res027      27 // Reserved
#define EM_res028      28 // Reserved
#define EM_res029      29 // Reserved
#define EM_res030      30 // Reserved
#define EM_res031      31 // Reserved
#define EM_res032      32 // Reserved
#define EM_res033      33 // Reserved
#define EM_res034      34 // Reserved
#define EM_res035      35 // Reserved
#define EM_V800        36 // NEC V800 series
#define EM_FR20        37 // Fujitsu FR20
#define EM_RH32        38 // TRW RH32
#define EM_MCORE       39 // Motorola M*Core // May also be taken by Fujitsu MMA
#define EM_RCE         39 // Old name for MCore
#define EM_ARM         40 // ARM
#define EM_OLD_ALPHA   41 // Digital Alpha
#define EM_SH          42 // Renesas (formerly Hitachi) / SuperH SH
#define EM_SPARCV9     43 // SPARC v9 64-bit
#define EM_TRICORE     44 // Siemens Tricore embedded processor
#define EM_ARC         45 // ARC Cores
#define EM_H8_300      46 // Renesas (formerly Hitachi) H8/300
#define EM_H8_300H     47 // Renesas (formerly Hitachi) H8/300H
#define EM_H8S         48 // Renesas (formerly Hitachi) H8S
#define EM_H8_500      49 // Renesas (formerly Hitachi) H8/500
#define EM_IA_64       50 // Intel IA-64 Processor
#define EM_MIPS_X      51 // Stanford MIPS-X
#define EM_COLDFIRE    52 // Motorola Coldfire
#define EM_68HC12      53 // Motorola M68HC12
#define EM_MMA         54 // Fujitsu Multimedia Accelerator
#define EM_PCP         55 // Siemens PCP
#define EM_NCPU        56 // Sony nCPU embedded RISC processor
#define EM_NDR1        57 // Denso NDR1 microprocesspr
#define EM_STARCORE    58 // Motorola Star*Core processor
#define EM_ME16        59 // Toyota ME16 processor
#define EM_ST100       60 // STMicroelectronics ST100 processor
#define EM_TINYJ       61 // Advanced Logic Corp. TinyJ embedded processor
#define EM_X86_64      62 // Advanced Micro Devices X86-64 processor
#define EM_PDSP        63 // Sony DSP Processor
#define EM_PDP10       64 // Digital Equipment Corp. PDP-10
#define EM_PDP11       65 // Digital Equipment Corp. PDP-11
#define EM_FX66        66 // Siemens FX66 microcontroller
#define EM_ST9PLUS     67 // STMicroelectronics ST9+ 8/16 bit microcontroller
#define EM_ST7         68 // STMicroelectronics ST7 8-bit microcontroller
#define EM_68HC16      69 // Motorola MC68HC16 Microcontroller
#define EM_68HC11      70 // Motorola MC68HC11 Microcontroller
#define EM_68HC08      71 // Motorola MC68HC08 Microcontroller
#define EM_68HC05      72 // Motorola MC68HC05 Microcontroller
#define EM_SVX         73 // Silicon Graphics SVx
#define EM_ST19        74 // STMicroelectronics ST19 8-bit cpu
#define EM_VAX         75 // Digital VAX
#define EM_CRIS        76 // Axis Communications 32-bit embedded processor
#define EM_JAVELIN     77 // Infineon Technologies 32-bit embedded cpu
#define EM_FIREPATH    78 // Element 14 64-bit DSP processor
#define EM_ZSP         79 // LSI Logic's 16-bit DSP processor
#define EM_MMIX        80 // Donald Knuth's educational 64-bit processor
#define EM_HUANY       81 // Harvard's machine-independent format
#define EM_PRISM       82 // SiTera Prism
#define EM_AVR         83 // Atmel AVR 8-bit microcontroller
#define EM_FR30        84 // Fujitsu FR30
#define EM_D10V        85 // Mitsubishi D10V
#define EM_D30V        86 // Mitsubishi D30V
#define EM_V850        87 // NEC v850
#define EM_M32R        88 // Renesas M32R (formerly Mitsubishi M32R)
#define EM_MN10300     89 // Matsushita MN10300
#define EM_MN10200     90 // Matsushita MN10200
#define EM_PJ          91 // picoJava
#define EM_OPENRISC    92 // OpenRISC 32-bit embedded processor
#define EM_ARC_A5      93 // ARC Cores Tangent-A5
#define EM_XTENSA      94 // Tensilica Xtensa Architecture
#define EM_VIDEOCORE   95 // Alphamosaic VideoCore processor
#define EM_TMM_GPP     96 // Thompson Multimedia General Purpose Processor
#define EM_NS32K       97 // National Semiconductor 32000 series
#define EM_TPC         98 // Tenor Network TPC processor
#define EM_SNP1K       99 // Trebia SNP 1000 processor
#define EM_ST200       100 // STMicroelectronics ST200 microcontroller
#define EM_IP2K        101 // Ubicom IP2022 micro controller
#define EM_MAX         102 // MAX Processor
#define EM_CR          103 // National Semiconductor CompactRISC
#define EM_F2MC16      104 // Fujitsu F2MC16
#define EM_MSP430      105 // TI msp430 micro controller
#define EM_BLACKFIN    106 // ADI Blackfin
#define EM_SE_C33      107 // S1C33 Family of Seiko Epson processors
#define EM_SEP         108 // Sharp embedded microprocessor
#define EM_ARCA        109 // Arca RISC Microprocessor
#define EM_UNICORE \
	110 // Microprocessor series from PKU-Unity Ltd. and MPRC of Peking University
#define EM_EXCESS       111 // eXcess: 16/32/64-bit configurable embedded CPU
#define EM_DXP          112 // Icera Semiconductor Inc. Deep Execution Processor
#define EM_ALTERA_NIOS2 113 // Altera Nios II soft-core processor
#define EM_CRX          114 // National Semiconductor CRX
#define EM_XGATE        115 // Motorola XGATE embedded processor
#define EM_C166         116 // Infineon C16x/XC16x processor
#define EM_M16C         117 // Renesas M16C series microprocessors
#define EM_DSPIC30F \
	118 // Microchip Technology dsPIC30F Digital Signal Controller
#define EM_CE            119 // Freescale Communication Engine RISC core
#define EM_M32C          120 // Renesas M32C series microprocessors
#define EM_res121        121 // Reserved
#define EM_res122        122 // Reserved
#define EM_res123        123 // Reserved
#define EM_res124        124 // Reserved
#define EM_res125        125 // Reserved
#define EM_res126        126 // Reserved
#define EM_res127        127 // Reserved
#define EM_res128        128 // Reserved
#define EM_res129        129 // Reserved
#define EM_res130        130 // Reserved
#define EM_TSK3000       131 // Altium TSK3000 core
#define EM_RS08          132 // Freescale RS08 embedded processor
#define EM_res133        133 // Reserved
#define EM_ECOG2         134 // Cyan Technology eCOG2 microprocessor
#define EM_SCORE         135 // Sunplus Score
#define EM_SCORE7        135 // Sunplus S+core7 RISC processor
#define EM_DSP24         136 // New Japan Radio (NJR) 24-bit DSP Processor
#define EM_VIDEOCORE3    137 // Broadcom VideoCore III processor
#define EM_LATTICEMICO32 138 // RISC processor for Lattice FPGA architecture
#define EM_SE_C17        139 // Seiko Epson C17 family
#define EM_TI_C6000      140 // Texas Instruments TMS320C6000 DSP family
#define EM_TI_C2000      141 // Texas Instruments TMS320C2000 DSP family
#define EM_TI_C5500      142 // Texas Instruments TMS320C55x DSP family
#define EM_res143        143 // Reserved
#define EM_res144        144 // Reserved
#define EM_res145        145 // Reserved
#define EM_res146        146 // Reserved
#define EM_res147        147 // Reserved
#define EM_res148        148 // Reserved
#define EM_res149        149 // Reserved
#define EM_res150        150 // Reserved
#define EM_res151        151 // Reserved
#define EM_res152        152 // Reserved
#define EM_res153        153 // Reserved
#define EM_res154        154 // Reserved
#define EM_res155        155 // Reserved
#define EM_res156        156 // Reserved
#define EM_res157        157 // Reserved
#define EM_res158        158 // Reserved
#define EM_res159        159 // Reserved
#define EM_MMDSP_PLUS    160 // STMicroelectronics 64bit VLIW Data Signal Processor
#define EM_CYPRESS_M8C   161 // Cypress M8C microprocessor
#define EM_R32C          162 // Renesas R32C series microprocessors
#define EM_TRIMEDIA      163 // NXP Semiconductors TriMedia architecture family
#define EM_QDSP6         164 // QUALCOMM DSP6 Processor
#define EM_8051          165 // Intel 8051 and variants
#define EM_STXP7X        166 // STMicroelectronics STxP7x family
#define EM_NDS32 \
	167 // Andes Technology compact code size embedded RISC processor family
#define EM_ECOG1         168 // Cyan Technology eCOG1X family
#define EM_ECOG1X        168 // Cyan Technology eCOG1X family
#define EM_MAXQ30        169 // Dallas Semiconductor MAXQ30 Core Micro-controllers
#define EM_XIMO16        170 // New Japan Radio (NJR) 16-bit DSP Processor
#define EM_MANIK         171 // M2000 Reconfigurable RISC Microprocessor
#define EM_CRAYNV2       172 // Cray Inc. NV2 vector architecture
#define EM_RX            173 // Renesas RX family
#define EM_METAG         174 // Imagination Technologies META processor architecture
#define EM_MCST_ELBRUS   175 // MCST Elbrus general purpose hardware architecture
#define EM_ECOG16        176 // Cyan Technology eCOG16 family
#define EM_CR16          177 // National Semiconductor CompactRISC 16-bit processor
#define EM_ETPU          178 // Freescale Extended Time Processing Unit
#define EM_SLE9X         179 // Infineon Technologies SLE9X core
#define EM_L1OM          180 // Intel L1OM
#define EM_INTEL181      181 // Reserved by Intel
#define EM_INTEL182      182 // Reserved by Intel
#define EM_res183        183 // Reserved by ARM
#define EM_res184        184 // Reserved by ARM
#define EM_AVR32         185 // Atmel Corporation 32-bit microprocessor family
#define EM_STM8          186 // STMicroeletronics STM8 8-bit microcontroller
#define EM_TILE64        187 // Tilera TILE64 multicore architecture family
#define EM_TILEPRO       188 // Tilera TILEPro multicore architecture family
#define EM_MICROBLAZE    189 // Xilinx MicroBlaze 32-bit RISC soft processor core
#define EM_CUDA          190 // NVIDIA CUDA architecture
#define EM_TILEGX        191 // Tilera TILE-Gx multicore architecture family
#define EM_CLOUDSHIELD   192 // CloudShield architecture family
#define EM_COREA_1ST     193 // KIPO-KAIST Core-A 1st generation processor family
#define EM_COREA_2ND     194 // KIPO-KAIST Core-A 2nd generation processor family
#define EM_ARC_COMPACT2  195 // Synopsys ARCompact V2
#define EM_OPEN8         196 // Open8 8-bit RISC soft processor core
#define EM_RL78          197 // Renesas RL78 family
#define EM_VIDEOCORE5    198 // Broadcom VideoCore V processor
#define EM_78KOR         199 // Renesas 78KOR family
#define EM_56800EX       200 // Freescale 56800EX Digital Signal Controller (DSC)
#define EM_BA1           201 // Beyond BA1 CPU architecture
#define EM_BA2           202 // Beyond BA2 CPU architecture
#define EM_XCORE         203 // XMOS xCORE processor family
#define EM_MCHP_PIC      204 // Microchip 8-bit PIC(r) family
#define EM_INTEL205      205 // Reserved by Intel
#define EM_INTEL206      206 // Reserved by Intel
#define EM_INTEL207      207 // Reserved by Intel
#define EM_INTEL208      208 // Reserved by Intel
#define EM_INTEL209      209 // Reserved by Intel
#define EM_KM32          210 // KM211 KM32 32-bit processor
#define EM_KMX32         211 // KM211 KMX32 32-bit processor
#define EM_KMX16         212 // KM211 KMX16 16-bit processor
#define EM_KMX8          213 // KM211 KMX8 8-bit processor
#define EM_KVARC         214 // KM211 KVARC processor
#define EM_CDP           215 // Paneve CDP architecture family
#define EM_COGE          216 // Cognitive Smart Memory Processor
#define EM_COOL          217 // iCelero CoolEngine
#define EM_NORC          218 // Nanoradio Optimized RISC
#define EM_CSR_KALIMBA   219 // CSR Kalimba architecture family
#define EM_Z80           220 // Zilog Z80
#define EM_VISIUM        221 // Controls and Data Services VISIUMcore processor
#define EM_FT32          222 // FTDI Chip FT32 high performance 32-bit RISC architecture
#define EM_MOXIE         223 // Moxie processor family
#define EM_AMDGPU        224 // AMD GPU architecture
#define EM_RISCV         243 // RISC-V
#define EM_LANAI         244 // Lanai processor
#define EM_CEVA          245 // CEVA Processor Architecture Family
#define EM_CEVA_X2       246 // CEVA X2 Processor Family
#define EM_BPF           247 // Linux BPF – in-kernel virtual machine
#define EM_GRAPHCORE_IPU 248 // Graphcore Intelligent Processing Unit
#define EM_IMG1          249 // Imagination Technologies
#define EM_NFP           250 // Netronome Flow Processor (P)
#define EM_CSKY          252 // C-SKY processor family

// File version
#define EV_NONE    0
#define EV_CURRENT 1

// Identification index
#define EI_MAG0       0
#define EI_MAG1       1
#define EI_MAG2       2
#define EI_MAG3       3
#define EI_CLASS      4
#define EI_DATA       5
#define EI_VERSION    6
#define EI_OSABI      7
#define EI_ABIVERSION 8
#define EI_PAD        9
#define EI_NIDENT     16

// Magic number
#define ELFMAG0 0x7F
#define ELFMAG1 'E'
#define ELFMAG2 'L'
#define ELFMAG3 'F'

// File class
#define ELFCLASSNONE 0
#define ELFCLASS32   1
#define ELFCLASS64   2

// Encoding
#define ELFDATANONE 0
#define ELFDATA2LSB 1
#define ELFDATA2MSB 2

// OS extensions
#define ELFOSABI_NONE    0  // No extensions or unspecified
#define ELFOSABI_HPUX    1  // Hewlett-Packard HP-UX
#define ELFOSABI_NETBSD  2  // NetBSD
#define ELFOSABI_LINUX   3  // Linux
#define ELFOSABI_SOLARIS 6  // Sun Solaris
#define ELFOSABI_AIX     7  // AIX
#define ELFOSABI_IRIX    8  // IRIX
#define ELFOSABI_FREEBSD 9  // FreeBSD
#define ELFOSABI_TRU64   10 // Compaq TRU64 UNIX
#define ELFOSABI_MODESTO 11 // Novell Modesto
#define ELFOSABI_OPENBSD 12 // Open BSD
#define ELFOSABI_OPENVMS 13 // Open VMS
#define ELFOSABI_NSK     14 // Hewlett-Packard Non-Stop Kernel
#define ELFOSABI_AROS    15 // Amiga Research OS
#define ELFOSABI_FENIXOS 16 // The FenixOS highly scalable multi-core OS
//                             64-255 Architecture-specific value range
#define ELFOSABI_AMDGPU_HSA \
	64 // AMDGPU OS for HSA compatible compute 	// kernels.
#define ELFOSABI_AMDGPU_PAL \
	65 // AMDGPU OS for AMD PAL compatible graphics  // shaders and compute kernels.
#define ELFOSABI_AMDGPU_MESA3D \
	66 // AMDGPU OS for Mesa3D compatible graphics 	// shaders and compute kernels.

// AMDGPU specific e_flags
#define EF_AMDGPU_MACH 0x0ff // AMDGPU processor selection mask.
#define EF_AMDGPU_XNACK \
	0x100 // Indicates if the XNACK target feature is   // enabled for all code contained in the ELF.
// AMDGPU processors
#define EF_AMDGPU_MACH_NONE                0x000 // Unspecified processor.
#define EF_AMDGPU_MACH_R600_R600           0x001
#define EF_AMDGPU_MACH_R600_R630           0x002
#define EF_AMDGPU_MACH_R600_RS880          0x003
#define EF_AMDGPU_MACH_R600_RV670          0x004
#define EF_AMDGPU_MACH_R600_RV710          0x005
#define EF_AMDGPU_MACH_R600_RV730          0x006
#define EF_AMDGPU_MACH_R600_RV770          0x007
#define EF_AMDGPU_MACH_R600_CEDAR          0x008
#define EF_AMDGPU_MACH_R600_CYPRESS        0x009
#define EF_AMDGPU_MACH_R600_JUNIPER        0x00a
#define EF_AMDGPU_MACH_R600_REDWOOD        0x00b
#define EF_AMDGPU_MACH_R600_SUMO           0x00c
#define EF_AMDGPU_MACH_R600_BARTS          0x00d
#define EF_AMDGPU_MACH_R600_CAICOS         0x00e
#define EF_AMDGPU_MACH_R600_CAYMAN         0x00f
#define EF_AMDGPU_MACH_R600_TURKS          0x010
#define EF_AMDGPU_MACH_R600_RESERVED_FIRST 0x011
#define EF_AMDGPU_MACH_R600_RESERVED_LAST  0x01f
#define EF_AMDGPU_MACH_R600_FIRST          EF_AMDGPU_MACH_R600_R600
#define EF_AMDGPU_MACH_R600_LAST           EF_AMDGPU_MACH_R600_TURKS
#define EF_AMDGPU_MACH_AMDGCN_GFX600       0x020
#define EF_AMDGPU_MACH_AMDGCN_GFX601       0x021
#define EF_AMDGPU_MACH_AMDGCN_GFX700       0x022
#define EF_AMDGPU_MACH_AMDGCN_GFX701       0x023
#define EF_AMDGPU_MACH_AMDGCN_GFX702       0x024
#define EF_AMDGPU_MACH_AMDGCN_GFX703       0x025
#define EF_AMDGPU_MACH_AMDGCN_GFX704       0x026
#define EF_AMDGPU_MACH_AMDGCN_GFX801       0x028
#define EF_AMDGPU_MACH_AMDGCN_GFX802       0x029
#define EF_AMDGPU_MACH_AMDGCN_GFX803       0x02a
#define EF_AMDGPU_MACH_AMDGCN_GFX810       0x02b
#define EF_AMDGPU_MACH_AMDGCN_GFX900       0x02c
#define EF_AMDGPU_MACH_AMDGCN_GFX902       0x02d
#define EF_AMDGPU_MACH_AMDGCN_GFX904       0x02e
#define EF_AMDGPU_MACH_AMDGCN_GFX906       0x02f
#define EF_AMDGPU_MACH_AMDGCN_RESERVED0    0x027
#define EF_AMDGPU_MACH_AMDGCN_RESERVED1    0x030
#define EF_AMDGPU_MACH_AMDGCN_FIRST        EF_AMDGPU_MACH_AMDGCN_GFX600
#define EF_AMDGPU_MACH_AMDGCN_LAST         EF_AMDGPU_MACH_AMDGCN_GFX906

/////////////////////
// Sections constants

// Section indexes
#define SHN_UNDEF     0
#define SHN_LORESERVE 0xFF00
#define SHN_LOPROC    0xFF00
#define SHN_HIPROC    0xFF1F
#define SHN_LOOS      0xFF20
#define SHN_HIOS      0xFF3F
#define SHN_ABS       0xFFF1
#define SHN_COMMON    0xFFF2
#define SHN_XINDEX    0xFFFF
#define SHN_HIRESERVE 0xFFFF

// Section types
#define SHT_NULL          0
#define SHT_PROGBITS      1
#define SHT_SYMTAB        2
#define SHT_STRTAB        3
#define SHT_RELA          4
#define SHT_HASH          5
#define SHT_DYNAMIC       6
#define SHT_NOTE          7
#define SHT_NOBITS        8
#define SHT_REL           9
#define SHT_SHLIB         10
#define SHT_DYNSYM        11
#define SHT_INIT_ARRAY    14
#define SHT_FINI_ARRAY    15
#define SHT_PREINIT_ARRAY 16
#define SHT_GROUP         17
#define SHT_SYMTAB_SHNDX  18
#define SHT_LOOS          0x60000000
#define SHT_HIOS          0x6fffffff
#define SHT_LOPROC        0x70000000
#define SHT_HIPROC        0x7FFFFFFF
#define SHT_LOUSER        0x80000000
#define SHT_HIUSER        0xFFFFFFFF

// Section attribute flags
#define SHF_WRITE            0x1
#define SHF_ALLOC            0x2
#define SHF_EXECINSTR        0x4
#define SHF_MERGE            0x10
#define SHF_STRINGS          0x20
#define SHF_INFO_LINK        0x40
#define SHF_LINK_ORDER       0x80
#define SHF_OS_NONCONFORMING 0x100
#define SHF_GROUP            0x200
#define SHF_TLS              0x400
#define SHF_MASKOS           0x0ff00000
#define SHF_MASKPROC         0xF0000000

// Section group flags
#define GRP_COMDAT   0x1
#define GRP_MASKOS   0x0ff00000
#define GRP_MASKPROC 0xf0000000

// Symbol binding
#define STB_LOCAL    0
#define STB_GLOBAL   1
#define STB_WEAK     2
#define STB_LOOS     10
#define STB_HIOS     12
#define STB_MULTIDEF 13
#define STB_LOPROC   13
#define STB_HIPROC   15

// Note types
#define NT_AMDGPU_METADATA         1
#define NT_AMD_AMDGPU_HSA_METADATA 10
#define NT_AMD_AMDGPU_ISA          11
#define NT_AMD_AMDGPU_PAL_METADATA 12

// Symbol types
#define STT_NOTYPE            0
#define STT_OBJECT            1
#define STT_FUNC              2
#define STT_SECTION           3
#define STT_FILE              4
#define STT_COMMON            5
#define STT_TLS               6
#define STT_LOOS              10
#define STT_AMDGPU_HSA_KERNEL 10
#define STT_HIOS              12
#define STT_LOPROC            13
#define STT_HIPROC            15

// Symbol visibility
#define STV_DEFAULT   0
#define STV_INTERNAL  1
#define STV_HIDDEN    2
#define STV_PROTECTED 3

// Undefined name
#define STN_UNDEF 0

// Relocation types
#define R_386_NONE               0
#define R_X86_64_NONE            0
#define R_AMDGPU_NONE            0
#define R_386_32                 1
#define R_X86_64_64              1
#define R_AMDGPU_ABS32_LO        1
#define R_386_PC32               2
#define R_X86_64_PC32            2
#define R_AMDGPU_ABS32_HI        2
#define R_386_GOT32              3
#define R_X86_64_GOT32           3
#define R_AMDGPU_ABS64           3
#define R_386_PLT32              4
#define R_X86_64_PLT32           4
#define R_AMDGPU_REL32           4
#define R_386_COPY               5
#define R_X86_64_COPY            5
#define R_AMDGPU_REL64           5
#define R_386_GLOB_DAT           6
#define R_X86_64_GLOB_DAT        6
#define R_AMDGPU_ABS32           6
#define R_386_JMP_SLOT           7
#define R_X86_64_JUMP_SLOT       7
#define R_AMDGPU_GOTPCREL        7
#define R_386_RELATIVE           8
#define R_X86_64_RELATIVE        8
#define R_AMDGPU_GOTPCREL32_LO   8
#define R_386_GOTOFF             9
#define R_X86_64_GOTPCREL        9
#define R_AMDGPU_GOTPCREL32_HI   9
#define R_386_GOTPC              10
#define R_X86_64_32              10
#define R_AMDGPU_REL32_LO        10
#define R_386_32PLT              11
#define R_X86_64_32S             11
#define R_AMDGPU_REL32_HI        11
#define R_X86_64_16              12
#define R_X86_64_PC16            13
#define R_AMDGPU_RELATIVE64      13
#define R_386_TLS_TPOFF          14
#define R_X86_64_8               14
#define R_386_TLS_IE             15
#define R_X86_64_PC8             15
#define R_386_TLS_GOTIE          16
#define R_X86_64_DTPMOD64        16
#define R_386_TLS_LE             17
#define R_X86_64_DTPOFF64        17
#define R_386_TLS_GD             18
#define R_X86_64_TPOFF64         18
#define R_386_TLS_LDM            19
#define R_X86_64_TLSGD           19
#define R_386_16                 20
#define R_X86_64_TLSLD           20
#define R_386_PC16               21
#define R_X86_64_DTPOFF32        21
#define R_386_8                  22
#define R_X86_64_GOTTPOFF        22
#define R_386_PC8                23
#define R_X86_64_TPOFF32         23
#define R_386_TLS_GD_32          24
#define R_X86_64_PC64            24
#define R_386_TLS_GD_PUSH        25
#define R_X86_64_GOTOFF64        25
#define R_386_TLS_GD_CALL        26
#define R_X86_64_GOTPC32         26
#define R_386_TLS_GD_POP         27
#define R_X86_64_GOT64           27
#define R_386_TLS_LDM_32         28
#define R_X86_64_GOTPCREL64      28
#define R_386_TLS_LDM_PUSH       29
#define R_X86_64_GOTPC64         29
#define R_386_TLS_LDM_CALL       30
#define R_X86_64_GOTPLT64        30
#define R_386_TLS_LDM_POP        31
#define R_X86_64_PLTOFF64        31
#define R_386_TLS_LDO_32         32
#define R_386_TLS_IE_32          33
#define R_386_TLS_LE_32          34
#define R_X86_64_GOTPC32_TLSDESC 34
#define R_386_TLS_DTPMOD32       35
#define R_X86_64_TLSDESC_CALL    35
#define R_386_TLS_DTPOFF32       36
#define R_X86_64_TLSDESC         36
#define R_386_TLS_TPOFF32        37
#define R_X86_64_IRELATIVE       37
#define R_386_SIZE32             38
#define R_386_TLS_GOTDESC        39
#define R_386_TLS_DESC_CALL      40
#define R_386_TLS_DESC           41
#define R_386_IRELATIVE          42
#define R_386_GOT32X             43
#define R_X86_64_GNU_VTINHERIT   250
#define R_X86_64_GNU_VTENTRY     251

// Segment types
#define PT_NULL    0
#define PT_LOAD    1
#define PT_DYNAMIC 2
#define PT_INTERP  3
#define PT_NOTE    4
#define PT_SHLIB   5
#define PT_PHDR    6
#define PT_TLS     7
#define PT_LOOS    0x60000000
#define PT_HIOS    0x6fffffff
#define PT_LOPROC  0x70000000
#define PT_HIPROC  0x7FFFFFFF

// Segment flags
#define PF_X        1          // Execute
#define PF_W        2          // Write
#define PF_R        4          // Read
#define PF_MASKOS   0x0ff00000 // Unspecified
#define PF_MASKPROC 0xf0000000 // Unspecified

// Dynamic Array Tags
#define DT_NULL            0
#define DT_NEEDED          1
#define DT_PLTRELSZ        2
#define DT_PLTGOT          3
#define DT_HASH            4
#define DT_STRTAB          5
#define DT_SYMTAB          6
#define DT_RELA            7
#define DT_RELASZ          8
#define DT_RELAENT         9
#define DT_STRSZ           10
#define DT_SYMENT          11
#define DT_INIT            12
#define DT_FINI            13
#define DT_SONAME          14
#define DT_RPATH           15
#define DT_SYMBOLIC        16
#define DT_REL             17
#define DT_RELSZ           18
#define DT_RELENT          19
#define DT_PLTREL          20
#define DT_DEBUG           21
#define DT_TEXTREL         22
#define DT_JMPREL          23
#define DT_BIND_NOW        24
#define DT_INIT_ARRAY      25
#define DT_FINI_ARRAY      26
#define DT_INIT_ARRAYSZ    27
#define DT_FINI_ARRAYSZ    28
#define DT_RUNPATH         29
#define DT_FLAGS           30
#define DT_ENCODING        32
#define DT_PREINIT_ARRAY   32
#define DT_PREINIT_ARRAYSZ 33
#define DT_MAXPOSTAGS      34
#define DT_LOOS            0x6000000D
#define DT_HIOS            0x6ffff000
#define DT_LOPROC          0x70000000
#define DT_HIPROC          0x7FFFFFFF

// DT_FLAGS values
#define DF_ORIGIN     0x1
#define DF_SYMBOLIC   0x2
#define DF_TEXTREL    0x4
#define DF_BIND_NOW   0x8
#define DF_STATIC_TLS 0x10

// ELF file header
struct Elf32_Ehdr
{
	unsigned char e_ident[EI_NIDENT];
	Elf_Half      e_type;
	Elf_Half      e_machine;
	Elf_Word      e_version;
	Elf32_Addr    e_entry;
	Elf32_Off     e_phoff;
	Elf32_Off     e_shoff;
	Elf_Word      e_flags;
	Elf_Half      e_ehsize;
	Elf_Half      e_phentsize;
	Elf_Half      e_phnum;
	Elf_Half      e_shentsize;
	Elf_Half      e_shnum;
	Elf_Half      e_shstrndx;
};

struct Elf64_Ehdr
{
	unsigned char e_ident[EI_NIDENT];
	Elf_Half      e_type;
	Elf_Half      e_machine;
	Elf_Word      e_version;
	Elf64_Addr    e_entry;
	Elf64_Off     e_phoff;
	Elf64_Off     e_shoff;
	Elf_Word      e_flags;
	Elf_Half      e_ehsize;
	Elf_Half      e_phentsize;
	Elf_Half      e_phnum;
	Elf_Half      e_shentsize;
	Elf_Half      e_shnum;
	Elf_Half      e_shstrndx;
};

// Section header
struct Elf32_Shdr
{
	Elf_Word   sh_name;
	Elf_Word   sh_type;
	Elf_Word   sh_flags;
	Elf32_Addr sh_addr;
	Elf32_Off  sh_offset;
	Elf_Word   sh_size;
	Elf_Word   sh_link;
	Elf_Word   sh_info;
	Elf_Word   sh_addralign;
	Elf_Word   sh_entsize;
};

struct Elf64_Shdr
{
	Elf_Word   sh_name;
	Elf_Word   sh_type;
	Elf_Xword  sh_flags;
	Elf64_Addr sh_addr;
	Elf64_Off  sh_offset;
	Elf_Xword  sh_size;
	Elf_Word   sh_link;
	Elf_Word   sh_info;
	Elf_Xword  sh_addralign;
	Elf_Xword  sh_entsize;
};

// Segment header
struct Elf32_Phdr
{
	Elf_Word   p_type;
	Elf32_Off  p_offset;
	Elf32_Addr p_vaddr;
	Elf32_Addr p_paddr;
	Elf_Word   p_filesz;
	Elf_Word   p_memsz;
	Elf_Word   p_flags;
	Elf_Word   p_align;
};

struct Elf64_Phdr
{
	Elf_Word   p_type;
	Elf_Word   p_flags;
	Elf64_Off  p_offset;
	Elf64_Addr p_vaddr;
	Elf64_Addr p_paddr;
	Elf_Xword  p_filesz;
	Elf_Xword  p_memsz;
	Elf_Xword  p_align;
};

// Symbol table entry
struct Elf32_Sym
{
	Elf_Word      st_name;
	Elf32_Addr    st_value;
	Elf_Word      st_size;
	unsigned char st_info;
	unsigned char st_other;
	Elf_Half      st_shndx;
};

struct Elf64_Sym
{
	Elf_Word      st_name;
	unsigned char st_info;
	unsigned char st_other;
	Elf_Half      st_shndx;
	Elf64_Addr    st_value;
	Elf_Xword     st_size;
};

#define ELF_ST_BIND( i )    ( ( i ) >> 4 )
#define ELF_ST_TYPE( i )    ( (i)&0xf )
#define ELF_ST_INFO( b, t ) ( ( ( b ) << 4 ) + ( (t)&0xf ) )

#define ELF_ST_VISIBILITY( o ) ( (o)&0x3 )

// Relocation entries
struct Elf32_Rel
{
	Elf32_Addr r_offset;
	Elf_Word   r_info;
};

struct Elf32_Rela
{
	Elf32_Addr r_offset;
	Elf_Word   r_info;
	Elf_Sword  r_addend;
};

struct Elf64_Rel
{
	Elf64_Addr r_offset;
	Elf_Xword  r_info;
};

struct Elf64_Rela
{
	Elf64_Addr r_offset;
	Elf_Xword  r_info;
	Elf_Sxword r_addend;
};

#define ELF32_R_SYM( i )     ( ( i ) >> 8 )
#define ELF32_R_TYPE( i )    ( (unsigned char)( i ) )
#define ELF32_R_INFO( s, t ) ( ( ( s ) << 8 ) + (unsigned char)( t ) )

#define ELF64_R_SYM( i )  ( ( i ) >> 32 )
#define ELF64_R_TYPE( i ) ( (i)&0xffffffffL )
#define ELF64_R_INFO( s, t ) \
	( ( ( ( int64_t )( s ) ) << 32 ) + ( (t)&0xffffffffL ) )

// Dynamic structure
struct Elf32_Dyn
{
	Elf_Sword d_tag;
	union {
		Elf_Word   d_val;
		Elf32_Addr d_ptr;
	} d_un;
};

struct Elf64_Dyn
{
	Elf_Sxword d_tag;
	union {
		Elf_Xword  d_val;
		Elf64_Addr d_ptr;
	} d_un;
};

} // namespace ELFIO

#endif // ELFTYPES_H

/*** End of inlined file: elf_types.hpp ***/


/*** Start of inlined file: elfio_version.hpp ***/
#define ELFIO_VERSION "3.8"

/*** End of inlined file: elfio_version.hpp ***/


/*** Start of inlined file: elfio_utils.hpp ***/
#ifndef ELFIO_UTILS_HPP
#define ELFIO_UTILS_HPP

#define ELFIO_GET_ACCESS( TYPE, NAME, FIELD ) \
	TYPE get_##NAME() const { return ( *convertor )( FIELD ); }
#define ELFIO_SET_ACCESS( TYPE, NAME, FIELD ) \
	void set_##NAME( TYPE value )             \
	{                                         \
		FIELD = value;                        \
		FIELD = ( *convertor )( FIELD );      \
	}
#define ELFIO_GET_SET_ACCESS( TYPE, NAME, FIELD )               \
	TYPE get_##NAME() const { return ( *convertor )( FIELD ); } \
	void set_##NAME( TYPE value )                               \
	{                                                           \
		FIELD = value;                                          \
		FIELD = ( *convertor )( FIELD );                        \
	}

#define ELFIO_GET_ACCESS_DECL( TYPE, NAME ) virtual TYPE get_##NAME() const = 0

#define ELFIO_SET_ACCESS_DECL( TYPE, NAME ) \
	virtual void set_##NAME( TYPE value ) = 0

#define ELFIO_GET_SET_ACCESS_DECL( TYPE, NAME ) \
	virtual TYPE get_##NAME() const       = 0;  \
	virtual void set_##NAME( TYPE value ) = 0

namespace ELFIO {

//------------------------------------------------------------------------------
class endianess_convertor
{
  public:
	//------------------------------------------------------------------------------
	endianess_convertor() { need_conversion = false; }

	//------------------------------------------------------------------------------
	void setup( unsigned char elf_file_encoding )
	{
		need_conversion = ( elf_file_encoding != get_host_encoding() );
	}

	//------------------------------------------------------------------------------
	uint64_t operator()( uint64_t value ) const
	{
		if ( !need_conversion ) {
			return value;
		}
		value = ( ( value & 0x00000000000000FFull ) << 56 ) |
				( ( value & 0x000000000000FF00ull ) << 40 ) |
				( ( value & 0x0000000000FF0000ull ) << 24 ) |
				( ( value & 0x00000000FF000000ull ) << 8 ) |
				( ( value & 0x000000FF00000000ull ) >> 8 ) |
				( ( value & 0x0000FF0000000000ull ) >> 24 ) |
				( ( value & 0x00FF000000000000ull ) >> 40 ) |
				( ( value & 0xFF00000000000000ull ) >> 56 );

		return value;
	}

	//------------------------------------------------------------------------------
	int64_t operator()( int64_t value ) const
	{
		if ( !need_conversion ) {
			return value;
		}
		return ( int64_t )( *this )( (uint64_t)value );
	}

	//------------------------------------------------------------------------------
	uint32_t operator()( uint32_t value ) const
	{
		if ( !need_conversion ) {
			return value;
		}
		value =
			( ( value & 0x000000FF ) << 24 ) | ( ( value & 0x0000FF00 ) << 8 ) |
			( ( value & 0x00FF0000 ) >> 8 ) | ( ( value & 0xFF000000 ) >> 24 );

		return value;
	}

	//------------------------------------------------------------------------------
	int32_t operator()( int32_t value ) const
	{
		if ( !need_conversion ) {
			return value;
		}
		return ( int32_t )( *this )( (uint32_t)value );
	}

	//------------------------------------------------------------------------------
	uint16_t operator()( uint16_t value ) const
	{
		if ( !need_conversion ) {
			return value;
		}
		value = ( ( value & 0x00FF ) << 8 ) | ( ( value & 0xFF00 ) >> 8 );

		return value;
	}

	//------------------------------------------------------------------------------
	int16_t operator()( int16_t value ) const
	{
		if ( !need_conversion ) {
			return value;
		}
		return ( int16_t )( *this )( (uint16_t)value );
	}

	//------------------------------------------------------------------------------
	int8_t operator()( int8_t value ) const { return value; }

	//------------------------------------------------------------------------------
	uint8_t operator()( uint8_t value ) const { return value; }

	//------------------------------------------------------------------------------
  private:
	//------------------------------------------------------------------------------
	unsigned char get_host_encoding() const
	{
		static const int tmp = 1;
		if ( 1 == *(const char*)&tmp ) {
			return ELFDATA2LSB;
		}
		else {
			return ELFDATA2MSB;
		}
	}

	//------------------------------------------------------------------------------
  private:
	bool need_conversion;
};

//------------------------------------------------------------------------------
inline uint32_t elf_hash( const unsigned char* name )
{
	uint32_t h = 0, g;
	while ( *name ) {
		h = ( h << 4 ) + *name++;
		g = h & 0xf0000000;
		if ( g != 0 )
			h ^= g >> 24;
		h &= ~g;
	}
	return h;
}

} // namespace ELFIO

#endif // ELFIO_UTILS_HPP

/*** End of inlined file: elfio_utils.hpp ***/


/*** Start of inlined file: elfio_header.hpp ***/
#ifndef ELF_HEADER_HPP
#define ELF_HEADER_HPP

#include <iostream>

namespace ELFIO {

class elf_header
{
  public:
	virtual ~elf_header(){};
	virtual bool load( std::istream& stream )       = 0;
	virtual bool save( std::ostream& stream ) const = 0;

	// ELF header functions
	ELFIO_GET_ACCESS_DECL( unsigned char, class );
	ELFIO_GET_ACCESS_DECL( unsigned char, elf_version );
	ELFIO_GET_ACCESS_DECL( unsigned char, encoding );
	ELFIO_GET_ACCESS_DECL( Elf_Half, header_size );
	ELFIO_GET_ACCESS_DECL( Elf_Half, section_entry_size );
	ELFIO_GET_ACCESS_DECL( Elf_Half, segment_entry_size );

	ELFIO_GET_SET_ACCESS_DECL( Elf_Word, version );
	ELFIO_GET_SET_ACCESS_DECL( unsigned char, os_abi );
	ELFIO_GET_SET_ACCESS_DECL( unsigned char, abi_version );
	ELFIO_GET_SET_ACCESS_DECL( Elf_Half, type );
	ELFIO_GET_SET_ACCESS_DECL( Elf_Half, machine );
	ELFIO_GET_SET_ACCESS_DECL( Elf_Word, flags );
	ELFIO_GET_SET_ACCESS_DECL( Elf64_Addr, entry );
	ELFIO_GET_SET_ACCESS_DECL( Elf_Half, sections_num );
	ELFIO_GET_SET_ACCESS_DECL( Elf64_Off, sections_offset );
	ELFIO_GET_SET_ACCESS_DECL( Elf_Half, segments_num );
	ELFIO_GET_SET_ACCESS_DECL( Elf64_Off, segments_offset );
	ELFIO_GET_SET_ACCESS_DECL( Elf_Half, section_name_str_index );
};

template <class T> struct elf_header_impl_types;
template <> struct elf_header_impl_types<Elf32_Ehdr>
{
	typedef Elf32_Phdr         Phdr_type;
	typedef Elf32_Shdr         Shdr_type;
	static const unsigned char file_class = ELFCLASS32;
};
template <> struct elf_header_impl_types<Elf64_Ehdr>
{
	typedef Elf64_Phdr         Phdr_type;
	typedef Elf64_Shdr         Shdr_type;
	static const unsigned char file_class = ELFCLASS64;
};

template <class T> class elf_header_impl : public elf_header
{
  public:
	//------------------------------------------------------------------------------
	elf_header_impl( endianess_convertor* convertor_, unsigned char encoding )
	{
		convertor = convertor_;

		std::fill_n( reinterpret_cast<char*>( &header ), sizeof( header ),
					 '\0' );

		header.e_ident[EI_MAG0]    = ELFMAG0;
		header.e_ident[EI_MAG1]    = ELFMAG1;
		header.e_ident[EI_MAG2]    = ELFMAG2;
		header.e_ident[EI_MAG3]    = ELFMAG3;
		header.e_ident[EI_CLASS]   = elf_header_impl_types<T>::file_class;
		header.e_ident[EI_DATA]    = encoding;
		header.e_ident[EI_VERSION] = EV_CURRENT;
		header.e_version           = ( *convertor )( (Elf_Word)EV_CURRENT );
		header.e_ehsize            = ( sizeof( header ) );
		header.e_ehsize            = ( *convertor )( header.e_ehsize );
		header.e_shstrndx          = ( *convertor )( (Elf_Half)1 );
		header.e_phentsize =
			sizeof( typename elf_header_impl_types<T>::Phdr_type );
		header.e_shentsize =
			sizeof( typename elf_header_impl_types<T>::Shdr_type );
		header.e_phentsize = ( *convertor )( header.e_phentsize );
		header.e_shentsize = ( *convertor )( header.e_shentsize );
	}

	//------------------------------------------------------------------------------
	bool load( std::istream& stream )
	{
		stream.seekg( 0 );
		stream.read( reinterpret_cast<char*>( &header ), sizeof( header ) );

		return ( stream.gcount() == sizeof( header ) );
	}

	//------------------------------------------------------------------------------
	bool save( std::ostream& stream ) const
	{
		stream.seekp( 0 );
		stream.write( reinterpret_cast<const char*>( &header ),
					  sizeof( header ) );

		return stream.good();
	}

	//------------------------------------------------------------------------------
	// ELF header functions
	ELFIO_GET_ACCESS( unsigned char, class, header.e_ident[EI_CLASS] );
	ELFIO_GET_ACCESS( unsigned char, elf_version, header.e_ident[EI_VERSION] );
	ELFIO_GET_ACCESS( unsigned char, encoding, header.e_ident[EI_DATA] );
	ELFIO_GET_ACCESS( Elf_Half, header_size, header.e_ehsize );
	ELFIO_GET_ACCESS( Elf_Half, section_entry_size, header.e_shentsize );
	ELFIO_GET_ACCESS( Elf_Half, segment_entry_size, header.e_phentsize );

	ELFIO_GET_SET_ACCESS( Elf_Word, version, header.e_version );
	ELFIO_GET_SET_ACCESS( unsigned char, os_abi, header.e_ident[EI_OSABI] );
	ELFIO_GET_SET_ACCESS( unsigned char,
						  abi_version,
						  header.e_ident[EI_ABIVERSION] );
	ELFIO_GET_SET_ACCESS( Elf_Half, type, header.e_type );
	ELFIO_GET_SET_ACCESS( Elf_Half, machine, header.e_machine );
	ELFIO_GET_SET_ACCESS( Elf_Word, flags, header.e_flags );
	ELFIO_GET_SET_ACCESS( Elf_Half, section_name_str_index, header.e_shstrndx );
	ELFIO_GET_SET_ACCESS( Elf64_Addr, entry, header.e_entry );
	ELFIO_GET_SET_ACCESS( Elf_Half, sections_num, header.e_shnum );
	ELFIO_GET_SET_ACCESS( Elf64_Off, sections_offset, header.e_shoff );
	ELFIO_GET_SET_ACCESS( Elf_Half, segments_num, header.e_phnum );
	ELFIO_GET_SET_ACCESS( Elf64_Off, segments_offset, header.e_phoff );

  private:
	T                    header;
	endianess_convertor* convertor;
};

} // namespace ELFIO

#endif // ELF_HEADER_HPP

/*** End of inlined file: elfio_header.hpp ***/


/*** Start of inlined file: elfio_section.hpp ***/
#ifndef ELFIO_SECTION_HPP
#define ELFIO_SECTION_HPP

#include <string>
#include <iostream>
#include <new>

namespace ELFIO {

class section
{
	friend class elfio;

  public:
	virtual ~section(){};

	ELFIO_GET_ACCESS_DECL( Elf_Half, index );
	ELFIO_GET_SET_ACCESS_DECL( std::string, name );
	ELFIO_GET_SET_ACCESS_DECL( Elf_Word, type );
	ELFIO_GET_SET_ACCESS_DECL( Elf_Xword, flags );
	ELFIO_GET_SET_ACCESS_DECL( Elf_Word, info );
	ELFIO_GET_SET_ACCESS_DECL( Elf_Word, link );
	ELFIO_GET_SET_ACCESS_DECL( Elf_Xword, addr_align );
	ELFIO_GET_SET_ACCESS_DECL( Elf_Xword, entry_size );
	ELFIO_GET_SET_ACCESS_DECL( Elf64_Addr, address );
	ELFIO_GET_SET_ACCESS_DECL( Elf_Xword, size );
	ELFIO_GET_SET_ACCESS_DECL( Elf_Word, name_string_offset );
	ELFIO_GET_ACCESS_DECL( Elf64_Off, offset );

	virtual const char* get_data() const                                = 0;
	virtual void        set_data( const char* pData, Elf_Word size )    = 0;
	virtual void        set_data( const std::string& data )             = 0;
	virtual void        append_data( const char* pData, Elf_Word size ) = 0;
	virtual void        append_data( const std::string& data )          = 0;
	virtual size_t      get_stream_size() const                         = 0;
	virtual void        set_stream_size( size_t value )                 = 0;

  protected:
	ELFIO_SET_ACCESS_DECL( Elf64_Off, offset );
	ELFIO_SET_ACCESS_DECL( Elf_Half, index );

	virtual void load( std::istream& stream, std::streampos header_offset ) = 0;
	virtual void save( std::ostream&  stream,
					   std::streampos header_offset,
					   std::streampos data_offset )                         = 0;
	virtual bool is_address_initialized() const                             = 0;
};

template <class T> class section_impl : public section
{
  public:
	//------------------------------------------------------------------------------
	section_impl( const endianess_convertor* convertor_ )
		: convertor( convertor_ )
	{
		std::fill_n( reinterpret_cast<char*>( &header ), sizeof( header ),
					 '\0' );
		is_address_set = false;
		data           = 0;
		data_size      = 0;
		index          = 0;
		stream_size    = 0;
	}

	//------------------------------------------------------------------------------
	~section_impl() { delete[] data; }

	//------------------------------------------------------------------------------
	// Section info functions
	ELFIO_GET_SET_ACCESS( Elf_Word, type, header.sh_type );
	ELFIO_GET_SET_ACCESS( Elf_Xword, flags, header.sh_flags );
	ELFIO_GET_SET_ACCESS( Elf_Xword, size, header.sh_size );
	ELFIO_GET_SET_ACCESS( Elf_Word, link, header.sh_link );
	ELFIO_GET_SET_ACCESS( Elf_Word, info, header.sh_info );
	ELFIO_GET_SET_ACCESS( Elf_Xword, addr_align, header.sh_addralign );
	ELFIO_GET_SET_ACCESS( Elf_Xword, entry_size, header.sh_entsize );
	ELFIO_GET_SET_ACCESS( Elf_Word, name_string_offset, header.sh_name );
	ELFIO_GET_ACCESS( Elf64_Addr, address, header.sh_addr );

	//------------------------------------------------------------------------------
	Elf_Half get_index() const { return index; }

	//------------------------------------------------------------------------------
	std::string get_name() const { return name; }

	//------------------------------------------------------------------------------
	void set_name( std::string name_ ) { name = name_; }

	//------------------------------------------------------------------------------
	void set_address( Elf64_Addr value )
	{
		header.sh_addr = value;
		header.sh_addr = ( *convertor )( header.sh_addr );
		is_address_set = true;
	}

	//------------------------------------------------------------------------------
	bool is_address_initialized() const { return is_address_set; }

	//------------------------------------------------------------------------------
	const char* get_data() const { return data; }

	//------------------------------------------------------------------------------
	void set_data( const char* raw_data, Elf_Word size )
	{
		if ( get_type() != SHT_NOBITS ) {
			delete[] data;
			data = new ( std::nothrow ) char[size];
			if ( 0 != data && 0 != raw_data ) {
				data_size = size;
				std::copy( raw_data, raw_data + size, data );
			}
			else {
				data_size = 0;
			}
		}

		set_size( data_size );
	}

	//------------------------------------------------------------------------------
	void set_data( const std::string& str_data )
	{
		return set_data( str_data.c_str(), (Elf_Word)str_data.size() );
	}

	//------------------------------------------------------------------------------
	void append_data( const char* raw_data, Elf_Word size )
	{
		if ( get_type() != SHT_NOBITS ) {
			if ( get_size() + size < data_size ) {
				std::copy( raw_data, raw_data + size, data + get_size() );
			}
			else {
				data_size      = 2 * ( data_size + size );
				char* new_data = new ( std::nothrow ) char[data_size];

				if ( 0 != new_data ) {
					std::copy( data, data + get_size(), new_data );
					std::copy( raw_data, raw_data + size,
							   new_data + get_size() );
					delete[] data;
					data = new_data;
				}
				else {
					size = 0;
				}
			}
			set_size( get_size() + size );
		}
	}

	//------------------------------------------------------------------------------
	void append_data( const std::string& str_data )
	{
		return append_data( str_data.c_str(), (Elf_Word)str_data.size() );
	}

	//------------------------------------------------------------------------------
  protected:
	//------------------------------------------------------------------------------
	ELFIO_GET_SET_ACCESS( Elf64_Off, offset, header.sh_offset );

	//------------------------------------------------------------------------------
	void set_index( Elf_Half value ) { index = value; }

	//------------------------------------------------------------------------------
	void load( std::istream& stream, std::streampos header_offset )
	{
		std::fill_n( reinterpret_cast<char*>( &header ), sizeof( header ),
					 '\0' );

		stream.seekg( 0, stream.end );
		set_stream_size( stream.tellg() );

		stream.seekg( header_offset );
		stream.read( reinterpret_cast<char*>( &header ), sizeof( header ) );

		Elf_Xword size = get_size();
		if ( 0 == data && SHT_NULL != get_type() && SHT_NOBITS != get_type() &&
			 size < get_stream_size() ) {
			data = new ( std::nothrow ) char[size + 1];

			if ( ( 0 != size ) && ( 0 != data ) ) {
				stream.seekg( ( *convertor )( header.sh_offset ) );
				stream.read( data, size );
				data[size] = 0; // Ensure data is ended with 0 to avoid oob read
				data_size  = size;
			}
			else {
				data_size = 0;
			}
		}
	}

	//------------------------------------------------------------------------------
	void save( std::ostream&  stream,
			   std::streampos header_offset,
			   std::streampos data_offset )
	{
		if ( 0 != get_index() ) {
			header.sh_offset = data_offset;
			header.sh_offset = ( *convertor )( header.sh_offset );
		}

		save_header( stream, header_offset );
		if ( get_type() != SHT_NOBITS && get_type() != SHT_NULL &&
			 get_size() != 0 && data != 0 ) {
			save_data( stream, data_offset );
		}
	}

	//------------------------------------------------------------------------------
  private:
	//------------------------------------------------------------------------------
	void save_header( std::ostream& stream, std::streampos header_offset ) const
	{
		stream.seekp( header_offset );
		stream.write( reinterpret_cast<const char*>( &header ),
					  sizeof( header ) );
	}

	//------------------------------------------------------------------------------
	void save_data( std::ostream& stream, std::streampos data_offset ) const
	{
		stream.seekp( data_offset );
		stream.write( get_data(), get_size() );
	}

	//------------------------------------------------------------------------------
	size_t get_stream_size() const { return stream_size; }

	//------------------------------------------------------------------------------
	void set_stream_size( size_t value ) { stream_size = value; }

	//------------------------------------------------------------------------------
  private:
	T                          header;
	Elf_Half                   index;
	std::string                name;
	char*                      data;
	Elf_Word                   data_size;
	const endianess_convertor* convertor;
	bool                       is_address_set;
	size_t                     stream_size;
};

} // namespace ELFIO

#endif // ELFIO_SECTION_HPP

/*** End of inlined file: elfio_section.hpp ***/


/*** Start of inlined file: elfio_segment.hpp ***/
#ifndef ELFIO_SEGMENT_HPP
#define ELFIO_SEGMENT_HPP

#include <iostream>
#include <vector>
#include <new>

namespace ELFIO {

class segment
{
	friend class elfio;

  public:
	virtual ~segment(){};

	ELFIO_GET_ACCESS_DECL( Elf_Half, index );
	ELFIO_GET_SET_ACCESS_DECL( Elf_Word, type );
	ELFIO_GET_SET_ACCESS_DECL( Elf_Word, flags );
	ELFIO_GET_SET_ACCESS_DECL( Elf_Xword, align );
	ELFIO_GET_SET_ACCESS_DECL( Elf64_Addr, virtual_address );
	ELFIO_GET_SET_ACCESS_DECL( Elf64_Addr, physical_address );
	ELFIO_GET_SET_ACCESS_DECL( Elf_Xword, file_size );
	ELFIO_GET_SET_ACCESS_DECL( Elf_Xword, memory_size );
	ELFIO_GET_ACCESS_DECL( Elf64_Off, offset );

	virtual const char* get_data() const = 0;

	virtual Elf_Half add_section_index( Elf_Half  index,
										Elf_Xword addr_align )  = 0;
	virtual Elf_Half get_sections_num() const                   = 0;
	virtual Elf_Half get_section_index_at( Elf_Half num ) const = 0;
	virtual bool     is_offset_initialized() const              = 0;

  protected:
	ELFIO_SET_ACCESS_DECL( Elf64_Off, offset );
	ELFIO_SET_ACCESS_DECL( Elf_Half, index );

	virtual const std::vector<Elf_Half>& get_sections() const               = 0;
	virtual void load( std::istream& stream, std::streampos header_offset ) = 0;
	virtual void save( std::ostream&  stream,
					   std::streampos header_offset,
					   std::streampos data_offset )                         = 0;
};

//------------------------------------------------------------------------------
template <class T> class segment_impl : public segment
{
  public:
	//------------------------------------------------------------------------------
	segment_impl( endianess_convertor* convertor_ )
		: stream_size( 0 ), index( 0 ), data( 0 ), convertor( convertor_ )
	{
		is_offset_set = false;
		std::fill_n( reinterpret_cast<char*>( &ph ), sizeof( ph ), '\0' );
	}

	//------------------------------------------------------------------------------
	virtual ~segment_impl() { delete[] data; }

	//------------------------------------------------------------------------------
	// Section info functions
	ELFIO_GET_SET_ACCESS( Elf_Word, type, ph.p_type );
	ELFIO_GET_SET_ACCESS( Elf_Word, flags, ph.p_flags );
	ELFIO_GET_SET_ACCESS( Elf_Xword, align, ph.p_align );
	ELFIO_GET_SET_ACCESS( Elf64_Addr, virtual_address, ph.p_vaddr );
	ELFIO_GET_SET_ACCESS( Elf64_Addr, physical_address, ph.p_paddr );
	ELFIO_GET_SET_ACCESS( Elf_Xword, file_size, ph.p_filesz );
	ELFIO_GET_SET_ACCESS( Elf_Xword, memory_size, ph.p_memsz );
	ELFIO_GET_ACCESS( Elf64_Off, offset, ph.p_offset );
	size_t stream_size;

	//------------------------------------------------------------------------------
	size_t get_stream_size() const { return stream_size; }

	//------------------------------------------------------------------------------
	void set_stream_size( size_t value ) { stream_size = value; }

	//------------------------------------------------------------------------------
	Elf_Half get_index() const { return index; }

	//------------------------------------------------------------------------------
	const char* get_data() const { return data; }

	//------------------------------------------------------------------------------
	Elf_Half add_section_index( Elf_Half sec_index, Elf_Xword addr_align )
	{
		sections.push_back( sec_index );
		if ( addr_align > get_align() ) {
			set_align( addr_align );
		}

		return (Elf_Half)sections.size();
	}

	//------------------------------------------------------------------------------
	Elf_Half get_sections_num() const { return (Elf_Half)sections.size(); }

	//------------------------------------------------------------------------------
	Elf_Half get_section_index_at( Elf_Half num ) const
	{
		if ( num < sections.size() ) {
			return sections[num];
		}

		return Elf_Half( -1 );
	}

	//------------------------------------------------------------------------------
  protected:
	//------------------------------------------------------------------------------

	//------------------------------------------------------------------------------
	void set_offset( Elf64_Off value )
	{
		ph.p_offset   = value;
		ph.p_offset   = ( *convertor )( ph.p_offset );
		is_offset_set = true;
	}

	//------------------------------------------------------------------------------
	bool is_offset_initialized() const { return is_offset_set; }

	//------------------------------------------------------------------------------
	const std::vector<Elf_Half>& get_sections() const { return sections; }

	//------------------------------------------------------------------------------
	void set_index( Elf_Half value ) { index = value; }

	//------------------------------------------------------------------------------
	void load( std::istream& stream, std::streampos header_offset )
	{

		stream.seekg( 0, stream.end );
		set_stream_size( stream.tellg() );

		stream.seekg( header_offset );
		stream.read( reinterpret_cast<char*>( &ph ), sizeof( ph ) );
		is_offset_set = true;

		if ( PT_NULL != get_type() && 0 != get_file_size() ) {
			stream.seekg( ( *convertor )( ph.p_offset ) );
			Elf_Xword size = get_file_size();

			if ( size > get_stream_size() ) {
				data = 0;
			}
			else {
				data = new (std::nothrow) char[size + 1];

				if ( 0 != data ) {
					stream.read( data, size );
					data[size] = 0;
				}
			}
		}
	}

	//------------------------------------------------------------------------------
	void save( std::ostream&  stream,
			   std::streampos header_offset,
			   std::streampos data_offset )
	{
		ph.p_offset = data_offset;
		ph.p_offset = ( *convertor )( ph.p_offset );
		stream.seekp( header_offset );
		stream.write( reinterpret_cast<const char*>( &ph ), sizeof( ph ) );
	}

	//------------------------------------------------------------------------------
  private:
	T                     ph;
	Elf_Half              index;
	char*                 data;
	std::vector<Elf_Half> sections;
	endianess_convertor*  convertor;
	bool                  is_offset_set;
};

} // namespace ELFIO

#endif // ELFIO_SEGMENT_HPP

/*** End of inlined file: elfio_segment.hpp ***/


/*** Start of inlined file: elfio_strings.hpp ***/
#ifndef ELFIO_STRINGS_HPP
#define ELFIO_STRINGS_HPP

#include <cstdlib>
#include <cstring>
#include <string>

namespace ELFIO {

//------------------------------------------------------------------------------
template <class S> class string_section_accessor_template
{
  public:
	//------------------------------------------------------------------------------
	string_section_accessor_template( S* section_ ) : string_section( section_ )
	{
	}

	//------------------------------------------------------------------------------
	const char* get_string( Elf_Word index ) const
	{
		if ( string_section ) {
			if ( index < string_section->get_size() ) {
				const char* data = string_section->get_data();
				if ( 0 != data ) {
					return data + index;
				}
			}
		}

		return 0;
	}

	//------------------------------------------------------------------------------
	Elf_Word add_string( const char* str )
	{
		Elf_Word current_position = 0;

		if ( string_section ) {
			// Strings are addeded to the end of the current section data
			current_position = (Elf_Word)string_section->get_size();

			if ( current_position == 0 ) {
				char empty_string = '\0';
				string_section->append_data( &empty_string, 1 );
				current_position++;
			}
			string_section->append_data( str,
										 (Elf_Word)std::strlen( str ) + 1 );
		}

		return current_position;
	}

	//------------------------------------------------------------------------------
	Elf_Word add_string( const std::string& str )
	{
		return add_string( str.c_str() );
	}

	//------------------------------------------------------------------------------
  private:
	S* string_section;
};

using string_section_accessor = string_section_accessor_template<section>;
using const_string_section_accessor =
	string_section_accessor_template<const section>;

} // namespace ELFIO

#endif // ELFIO_STRINGS_HPP

/*** End of inlined file: elfio_strings.hpp ***/

#define ELFIO_HEADER_ACCESS_GET( TYPE, FNAME ) \
	TYPE get_##FNAME() const { return header ? ( header->get_##FNAME() ) : 0; }

#define ELFIO_HEADER_ACCESS_GET_SET( TYPE, FNAME )     \
	TYPE get_##FNAME() const                           \
	{                                                  \
		return header ? ( header->get_##FNAME() ) : 0; \
	}                                                  \
	void set_##FNAME( TYPE val )                       \
	{                                                  \
		if ( header ) {                                \
			header->set_##FNAME( val );                \
		}                                              \
	}

namespace ELFIO {

//------------------------------------------------------------------------------
class elfio
{
  public:
	//------------------------------------------------------------------------------
	elfio() : sections( this ), segments( this )
	{
		header           = 0;
		current_file_pos = 0;
		create( ELFCLASS32, ELFDATA2LSB );
	}

	//------------------------------------------------------------------------------
	~elfio() { clean(); }

	//------------------------------------------------------------------------------
	void create( unsigned char file_class, unsigned char encoding )
	{
		clean();
		convertor.setup( encoding );
		header = create_header( file_class, encoding );
		create_mandatory_sections();
	}

	//------------------------------------------------------------------------------
	bool load( const std::string& file_name )
	{
		std::ifstream stream;
		stream.open( file_name.c_str(), std::ios::in | std::ios::binary );
		if ( !stream ) {
			return false;
		}

		return load( stream );
	}

	//------------------------------------------------------------------------------
	bool load( std::istream& stream )
	{
		clean();

		unsigned char e_ident[EI_NIDENT];
		// Read ELF file signature
		stream.read( reinterpret_cast<char*>( &e_ident ), sizeof( e_ident ) );

		// Is it ELF file?
		if ( stream.gcount() != sizeof( e_ident ) ||
			 e_ident[EI_MAG0] != ELFMAG0 || e_ident[EI_MAG1] != ELFMAG1 ||
			 e_ident[EI_MAG2] != ELFMAG2 || e_ident[EI_MAG3] != ELFMAG3 ) {
			return false;
		}

		if ( ( e_ident[EI_CLASS] != ELFCLASS64 ) &&
			 ( e_ident[EI_CLASS] != ELFCLASS32 ) ) {
			return false;
		}

		convertor.setup( e_ident[EI_DATA] );
		header = create_header( e_ident[EI_CLASS], e_ident[EI_DATA] );
		if ( 0 == header ) {
			return false;
		}
		if ( !header->load( stream ) ) {
			return false;
		}

		load_sections( stream );
		bool is_still_good = load_segments( stream );
		return is_still_good;
	}

	//------------------------------------------------------------------------------
	bool save( const std::string& file_name )
	{
		std::ofstream stream;
		stream.open( file_name.c_str(), std::ios::out | std::ios::binary );
		if ( !stream ) {
			return false;
		}

		return save( stream );
	}

	//------------------------------------------------------------------------------
	bool save( std::ostream& stream )
	{
		if ( !stream || !header ) {
			return false;
		}

		bool is_still_good = true;
		// Define layout specific header fields
		// The position of the segment table is fixed after the header.
		// The position of the section table is variable and needs to be fixed
		// before saving.
		header->set_segments_num( segments.size() );
		header->set_segments_offset( segments.size() ? header->get_header_size()
													 : 0 );
		header->set_sections_num( sections.size() );
		header->set_sections_offset( 0 );

		// Layout the first section right after the segment table
		current_file_pos = header->get_header_size() +
						   header->get_segment_entry_size() *
							   (Elf_Xword)header->get_segments_num();

		calc_segment_alignment();

		is_still_good = layout_segments_and_their_sections();
		is_still_good = is_still_good && layout_sections_without_segments();
		is_still_good = is_still_good && layout_section_table();

		is_still_good = is_still_good && save_header( stream );
		is_still_good = is_still_good && save_sections( stream );
		is_still_good = is_still_good && save_segments( stream );

		return is_still_good;
	}

	//------------------------------------------------------------------------------
	// ELF header access functions
	ELFIO_HEADER_ACCESS_GET( unsigned char, class );
	ELFIO_HEADER_ACCESS_GET( unsigned char, elf_version );
	ELFIO_HEADER_ACCESS_GET( unsigned char, encoding );
	ELFIO_HEADER_ACCESS_GET( Elf_Word, version );
	ELFIO_HEADER_ACCESS_GET( Elf_Half, header_size );
	ELFIO_HEADER_ACCESS_GET( Elf_Half, section_entry_size );
	ELFIO_HEADER_ACCESS_GET( Elf_Half, segment_entry_size );

	ELFIO_HEADER_ACCESS_GET_SET( unsigned char, os_abi );
	ELFIO_HEADER_ACCESS_GET_SET( unsigned char, abi_version );
	ELFIO_HEADER_ACCESS_GET_SET( Elf_Half, type );
	ELFIO_HEADER_ACCESS_GET_SET( Elf_Half, machine );
	ELFIO_HEADER_ACCESS_GET_SET( Elf_Word, flags );
	ELFIO_HEADER_ACCESS_GET_SET( Elf64_Addr, entry );
	ELFIO_HEADER_ACCESS_GET_SET( Elf64_Off, sections_offset );
	ELFIO_HEADER_ACCESS_GET_SET( Elf64_Off, segments_offset );
	ELFIO_HEADER_ACCESS_GET_SET( Elf_Half, section_name_str_index );

	//------------------------------------------------------------------------------
	const endianess_convertor& get_convertor() const { return convertor; }

	//------------------------------------------------------------------------------
	Elf_Xword get_default_entry_size( Elf_Word section_type ) const
	{
		switch ( section_type ) {
		case SHT_RELA:
			if ( header->get_class() == ELFCLASS64 ) {
				return sizeof( Elf64_Rela );
			}
			else {
				return sizeof( Elf32_Rela );
			}
		case SHT_REL:
			if ( header->get_class() == ELFCLASS64 ) {
				return sizeof( Elf64_Rel );
			}
			else {
				return sizeof( Elf32_Rel );
			}
		case SHT_SYMTAB:
			if ( header->get_class() == ELFCLASS64 ) {
				return sizeof( Elf64_Sym );
			}
			else {
				return sizeof( Elf32_Sym );
			}
		case SHT_DYNAMIC:
			if ( header->get_class() == ELFCLASS64 ) {
				return sizeof( Elf64_Dyn );
			}
			else {
				return sizeof( Elf32_Dyn );
			}
		default:
			return 0;
		}
	}

	//------------------------------------------------------------------------------
  private:
	bool is_offset_in_section( Elf64_Off offset, const section* sec ) const
	{
		return ( offset >= sec->get_offset() ) &&
			   ( offset < ( sec->get_offset() + sec->get_size() ) );
	}

	//------------------------------------------------------------------------------
  public:
	//! returns an empty string if no problems are detected,
	//! or a string containing an error message if problems are found
	std::string validate() const
	{

		// check for overlapping sections in the file
		for ( int i = 0; i < sections.size(); ++i ) {
			for ( int j = i + 1; j < sections.size(); ++j ) {
				const section* a = sections[i];
				const section* b = sections[j];
				if ( !( a->get_type() & SHT_NOBITS ) &&
					 !( b->get_type() & SHT_NOBITS ) && ( a->get_size() > 0 ) &&
					 ( b->get_size() > 0 ) && ( a->get_offset() > 0 ) &&
					 ( b->get_offset() > 0 ) ) {
					if ( is_offset_in_section( a->get_offset(), b ) ||
						 is_offset_in_section(
							 a->get_offset() + a->get_size() - 1, b ) ||
						 is_offset_in_section( b->get_offset(), a ) ||
						 is_offset_in_section(
							 b->get_offset() + b->get_size() - 1, a ) ) {
						return "Sections " + a->get_name() + " and " +
							   b->get_name() + " overlap in file";
					}
				}
			}
		}

		// more checks to be added here...

		return "";
	}

	//------------------------------------------------------------------------------
  private:
	//------------------------------------------------------------------------------
	void clean()
	{
		delete header;
		header = 0;

		std::vector<section*>::const_iterator it;
		for ( it = sections_.begin(); it != sections_.end(); ++it ) {
			delete *it;
		}
		sections_.clear();

		std::vector<segment*>::const_iterator it1;
		for ( it1 = segments_.begin(); it1 != segments_.end(); ++it1 ) {
			delete *it1;
		}
		segments_.clear();
	}

	//------------------------------------------------------------------------------
	elf_header* create_header( unsigned char file_class,
							   unsigned char encoding )
	{
		elf_header* new_header = 0;

		if ( file_class == ELFCLASS64 ) {
			new_header =
				new elf_header_impl<Elf64_Ehdr>( &convertor, encoding );
		}
		else if ( file_class == ELFCLASS32 ) {
			new_header =
				new elf_header_impl<Elf32_Ehdr>( &convertor, encoding );
		}
		else {
			return 0;
		}

		return new_header;
	}

	//------------------------------------------------------------------------------
	section* create_section()
	{
		section*      new_section;
		unsigned char file_class = get_class();

		if ( file_class == ELFCLASS64 ) {
			new_section = new section_impl<Elf64_Shdr>( &convertor );
		}
		else if ( file_class == ELFCLASS32 ) {
			new_section = new section_impl<Elf32_Shdr>( &convertor );
		}
		else {
			return 0;
		}

		new_section->set_index( (Elf_Half)sections_.size() );
		sections_.push_back( new_section );

		return new_section;
	}

	//------------------------------------------------------------------------------
	segment* create_segment()
	{
		segment*      new_segment;
		unsigned char file_class = header->get_class();

		if ( file_class == ELFCLASS64 ) {
			new_segment = new segment_impl<Elf64_Phdr>( &convertor );
		}
		else if ( file_class == ELFCLASS32 ) {
			new_segment = new segment_impl<Elf32_Phdr>( &convertor );
		}
		else {
			return 0;
		}

		new_segment->set_index( (Elf_Half)segments_.size() );
		segments_.push_back( new_segment );

		return new_segment;
	}

	//------------------------------------------------------------------------------
	void create_mandatory_sections()
	{
		// Create null section without calling to 'add_section' as no string
		// section containing section names exists yet
		section* sec0 = create_section();
		sec0->set_index( 0 );
		sec0->set_name( "" );
		sec0->set_name_string_offset( 0 );

		set_section_name_str_index( 1 );
		section* shstrtab = sections.add( ".shstrtab" );
		shstrtab->set_type( SHT_STRTAB );
		shstrtab->set_addr_align( 1 );
	}

	//------------------------------------------------------------------------------
	Elf_Half load_sections( std::istream& stream )
	{
		Elf_Half  entry_size = header->get_section_entry_size();
		Elf_Half  num        = header->get_sections_num();
		Elf64_Off offset     = header->get_sections_offset();

		for ( Elf_Half i = 0; i < num; ++i ) {
			section* sec = create_section();
			sec->load( stream, (std::streamoff)offset +
								   (std::streampos)i * entry_size );
			sec->set_index( i );
			// To mark that the section is not permitted to reassign address
			// during layout calculation
			sec->set_address( sec->get_address() );
		}

		Elf_Half shstrndx = get_section_name_str_index();

		if ( SHN_UNDEF != shstrndx ) {
			string_section_accessor str_reader( sections[shstrndx] );
			for ( Elf_Half i = 0; i < num; ++i ) {
				Elf_Word section_offset = sections[i]->get_name_string_offset();
				const char* p = str_reader.get_string( section_offset );
				if ( p != 0 ) {
					sections[i]->set_name( p );
				}
			}
		}

		return num;
	}

	//------------------------------------------------------------------------------
	//! Checks whether the addresses of the section entirely fall within the given segment.
	//! It doesn't matter if the addresses are memory addresses, or file offsets,
	//!  they just need to be in the same address space
	bool is_sect_in_seg( Elf64_Off sect_begin,
						 Elf_Xword sect_size,
						 Elf64_Off seg_begin,
						 Elf64_Off seg_end )
	{
		return ( seg_begin <= sect_begin ) &&
			   ( sect_begin + sect_size <= seg_end ) &&
			   ( sect_begin <
				 seg_end ); // this is important criteria when sect_size == 0
		// Example:  seg_begin=10, seg_end=12 (-> covering the bytes 10 and 11)
		//           sect_begin=12, sect_size=0  -> shall return false!
	}

	//------------------------------------------------------------------------------
	bool load_segments( std::istream& stream )
	{
		Elf_Half  entry_size = header->get_segment_entry_size();
		Elf_Half  num        = header->get_segments_num();
		Elf64_Off offset     = header->get_segments_offset();

		for ( Elf_Half i = 0; i < num; ++i ) {
			segment*      seg;
			unsigned char file_class = header->get_class();

			if ( file_class == ELFCLASS64 ) {
				seg = new segment_impl<Elf64_Phdr>( &convertor );
			}
			else if ( file_class == ELFCLASS32 ) {
				seg = new segment_impl<Elf32_Phdr>( &convertor );
			}
			else {
				return false;
			}

			seg->load( stream, (std::streamoff)offset +
								   (std::streampos)i * entry_size );
			seg->set_index( i );

			// Add sections to the segments (similar to readelfs algorithm)
			Elf64_Off segBaseOffset = seg->get_offset();
			Elf64_Off segEndOffset  = segBaseOffset + seg->get_file_size();
			Elf64_Off segVBaseAddr  = seg->get_virtual_address();
			Elf64_Off segVEndAddr   = segVBaseAddr + seg->get_memory_size();
			for ( Elf_Half j = 0; j < sections.size(); ++j ) {
				const section* psec = sections[j];

				// SHF_ALLOC sections are matched based on the virtual address
				// otherwise the file offset is matched
				if ( ( psec->get_flags() & SHF_ALLOC )
						 ? is_sect_in_seg( psec->get_address(),
										   psec->get_size(), segVBaseAddr,
										   segVEndAddr )
						 : is_sect_in_seg( psec->get_offset(), psec->get_size(),
										   segBaseOffset, segEndOffset ) ) {
					// Alignment of segment shall not be updated, to preserve original value
					// It will be re-calculated on saving.
					seg->add_section_index( psec->get_index(), 0 );
				}
			}

			// Add section into the segments' container
			segments_.push_back( seg );
		}

		return true;
	}

	//------------------------------------------------------------------------------
	bool save_header( std::ostream& stream ) { return header->save( stream ); }

	//------------------------------------------------------------------------------
	bool save_sections( std::ostream& stream )
	{
		for ( unsigned int i = 0; i < sections_.size(); ++i ) {
			section* sec = sections_.at( i );

			std::streampos headerPosition =
				(std::streamoff)header->get_sections_offset() +
				(std::streampos)header->get_section_entry_size() *
					sec->get_index();

			sec->save( stream, headerPosition, sec->get_offset() );
		}
		return true;
	}

	//------------------------------------------------------------------------------
	bool save_segments( std::ostream& stream )
	{
		for ( unsigned int i = 0; i < segments_.size(); ++i ) {
			segment* seg = segments_.at( i );

			std::streampos headerPosition =
				header->get_segments_offset() +
				(std::streampos)header->get_segment_entry_size() *
					seg->get_index();

			seg->save( stream, headerPosition, seg->get_offset() );
		}
		return true;
	}

	//------------------------------------------------------------------------------
	bool is_section_without_segment( unsigned int section_index )
	{
		bool found = false;

		for ( unsigned int j = 0; !found && ( j < segments.size() ); ++j ) {
			for ( unsigned int k = 0;
				  !found && ( k < segments[j]->get_sections_num() ); ++k ) {
				found = segments[j]->get_section_index_at( k ) == section_index;
			}
		}

		return !found;
	}

	//------------------------------------------------------------------------------
	bool is_subsequence_of( segment* seg1, segment* seg2 )
	{
		// Return 'true' if sections of seg1 are a subset of sections in seg2
		const std::vector<Elf_Half>& sections1 = seg1->get_sections();
		const std::vector<Elf_Half>& sections2 = seg2->get_sections();

		bool found = false;
		if ( sections1.size() < sections2.size() ) {
			found = std::includes( sections2.begin(), sections2.end(),
								   sections1.begin(), sections1.end() );
		}

		return found;
	}

	//------------------------------------------------------------------------------
	std::vector<segment*> get_ordered_segments()
	{
		std::vector<segment*> res;
		std::deque<segment*>  worklist;

		res.reserve( segments.size() );
		std::copy( segments_.begin(), segments_.end(),
				   std::back_inserter( worklist ) );

		// Bring the segments which start at address 0 to the front
		size_t nextSlot = 0;
		for ( size_t i = 0; i < worklist.size(); ++i ) {
			if ( i != nextSlot && worklist[i]->is_offset_initialized() &&
				 worklist[i]->get_offset() == 0 ) {
				if ( worklist[nextSlot]->get_offset() == 0 ) {
					++nextSlot;
				}
				std::swap( worklist[i], worklist[nextSlot] );
				++nextSlot;
			}
		}

		while ( !worklist.empty() ) {
			segment* seg = worklist.front();
			worklist.pop_front();

			size_t i = 0;
			for ( ; i < worklist.size(); ++i ) {
				if ( is_subsequence_of( seg, worklist[i] ) ) {
					break;
				}
			}

			if ( i < worklist.size() )
				worklist.push_back( seg );
			else
				res.push_back( seg );
		}

		return res;
	}

	//------------------------------------------------------------------------------
	bool layout_sections_without_segments()
	{
		for ( unsigned int i = 0; i < sections_.size(); ++i ) {
			if ( is_section_without_segment( i ) ) {
				section* sec = sections_[i];

				Elf_Xword section_align = sec->get_addr_align();
				if ( section_align > 1 &&
					 current_file_pos % section_align != 0 ) {
					current_file_pos +=
						section_align - current_file_pos % section_align;
				}

				if ( 0 != sec->get_index() )
					sec->set_offset( current_file_pos );

				if ( SHT_NOBITS != sec->get_type() &&
					 SHT_NULL != sec->get_type() ) {
					current_file_pos += sec->get_size();
				}
			}
		}

		return true;
	}

	//------------------------------------------------------------------------------
	void calc_segment_alignment()
	{
		for ( std::vector<segment*>::iterator s = segments_.begin();
			  s != segments_.end(); ++s ) {
			segment* seg = *s;
			for ( int i = 0; i < seg->get_sections_num(); ++i ) {
				section* sect = sections_[seg->get_section_index_at( i )];
				if ( sect->get_addr_align() > seg->get_align() ) {
					seg->set_align( sect->get_addr_align() );
				}
			}
		}
	}

	//------------------------------------------------------------------------------
	bool layout_segments_and_their_sections()
	{
		std::vector<segment*> worklist;
		std::vector<bool>     section_generated( sections.size(), false );

		// Get segments in a order in where segments which contain a
		// sub sequence of other segments are located at the end
		worklist = get_ordered_segments();

		for ( unsigned int i = 0; i < worklist.size(); ++i ) {
			Elf_Xword segment_memory   = 0;
			Elf_Xword segment_filesize = 0;
			Elf_Xword seg_start_pos    = current_file_pos;
			segment*  seg              = worklist[i];

			// Special case: PHDR segment
			// This segment contains the program headers but no sections
			if ( seg->get_type() == PT_PHDR && seg->get_sections_num() == 0 ) {
				seg_start_pos  = header->get_segments_offset();
				segment_memory = segment_filesize =
					header->get_segment_entry_size() *
					(Elf_Xword)header->get_segments_num();
			}
			// Special case:
			else if ( seg->is_offset_initialized() && seg->get_offset() == 0 ) {
				seg_start_pos = 0;
				if ( seg->get_sections_num() ) {
					segment_memory = segment_filesize = current_file_pos;
				}
			}
			// New segments with not generated sections
			// have to be aligned
			else if ( seg->get_sections_num() &&
					  !section_generated[seg->get_section_index_at( 0 )] ) {
				Elf_Xword align = seg->get_align() > 0 ? seg->get_align() : 1;
				Elf64_Off cur_page_alignment = current_file_pos % align;
				Elf64_Off req_page_alignment =
					seg->get_virtual_address() % align;
				Elf64_Off error = req_page_alignment - cur_page_alignment;

				current_file_pos += ( seg->get_align() + error ) % align;
				seg_start_pos = current_file_pos;
			}
			else if ( seg->get_sections_num() ) {
				seg_start_pos =
					sections[seg->get_section_index_at( 0 )]->get_offset();
			}

			// Write segment's data
			for ( unsigned int j = 0; j < seg->get_sections_num(); ++j ) {
				Elf_Half index = seg->get_section_index_at( j );

				section* sec = sections[index];

				// The NULL section is always generated
				if ( SHT_NULL == sec->get_type() ) {
					section_generated[index] = true;
					continue;
				}

				Elf_Xword secAlign = 0;
				// Fix up the alignment
				if ( !section_generated[index] &&
					 sec->is_address_initialized() &&
					 SHT_NOBITS != sec->get_type() &&
					 SHT_NULL != sec->get_type() && 0 != sec->get_size() ) {
					// Align the sections based on the virtual addresses
					// when possible (this is what matters for execution)
					Elf64_Off req_offset =
						sec->get_address() - seg->get_virtual_address();
					Elf64_Off cur_offset = current_file_pos - seg_start_pos;
					if ( req_offset < cur_offset ) {
						// something has gone awfully wrong, abort!
						// secAlign would turn out negative, seeking backwards and overwriting previous data
						return false;
					}
					secAlign = req_offset - cur_offset;
				}
				else if ( !section_generated[index] &&
						  !sec->is_address_initialized() ) {
					// If no address has been specified then only the section
					// alignment constraint has to be matched
					Elf_Xword align = sec->get_addr_align();
					if ( align == 0 ) {
						align = 1;
					}
					Elf64_Off error = current_file_pos % align;
					secAlign        = ( align - error ) % align;
				}
				else if ( section_generated[index] ) {
					// Alignment for already generated sections
					secAlign =
						sec->get_offset() - seg_start_pos - segment_filesize;
				}

				// Determine the segment file and memory sizes
				// Special case .tbss section (NOBITS) in non TLS segment
				if ( ( sec->get_flags() & SHF_ALLOC ) &&
					 !( ( sec->get_flags() & SHF_TLS ) &&
						( seg->get_type() != PT_TLS ) &&
						( SHT_NOBITS == sec->get_type() ) ) )
					segment_memory += sec->get_size() + secAlign;

				if ( SHT_NOBITS != sec->get_type() )
					segment_filesize += sec->get_size() + secAlign;

				// Nothing to be done when generating nested segments
				if ( section_generated[index] ) {
					continue;
				}

				current_file_pos += secAlign;

				// Set the section addresses when missing
				if ( !sec->is_address_initialized() )
					sec->set_address( seg->get_virtual_address() +
									  current_file_pos - seg_start_pos );

				if ( 0 != sec->get_index() )
					sec->set_offset( current_file_pos );

				if ( SHT_NOBITS != sec->get_type() )
					current_file_pos += sec->get_size();

				section_generated[index] = true;
			}

			seg->set_file_size( segment_filesize );

			// If we already have a memory size from loading an elf file (value > 0),
			// it must not shrink!
			// Memory size may be bigger than file size and it is the loader's job to do something
			// with the surplus bytes in memory, like initializing them with a defined value.
			if ( seg->get_memory_size() < segment_memory ) {
				seg->set_memory_size( segment_memory );
			}

			seg->set_offset( seg_start_pos );
		}

		return true;
	}

	//------------------------------------------------------------------------------
	bool layout_section_table()
	{
		// Simply place the section table at the end for now
		Elf64_Off alignmentError = current_file_pos % 4;
		current_file_pos += ( 4 - alignmentError ) % 4;
		header->set_sections_offset( current_file_pos );
		return true;
	}

	//------------------------------------------------------------------------------
  public:
	friend class Sections;
	class Sections
	{
	  public:
		//------------------------------------------------------------------------------
		Sections( elfio* parent_ ) : parent( parent_ ) {}

		//------------------------------------------------------------------------------
		Elf_Half size() const { return (Elf_Half)parent->sections_.size(); }

		//------------------------------------------------------------------------------
		section* operator[]( unsigned int index ) const
		{
			section* sec = 0;

			if ( index < parent->sections_.size() ) {
				sec = parent->sections_[index];
			}

			return sec;
		}

		//------------------------------------------------------------------------------
		section* operator[]( const std::string& name ) const
		{
			section* sec = 0;

			std::vector<section*>::const_iterator it;
			for ( it = parent->sections_.begin(); it != parent->sections_.end();
				  ++it ) {
				if ( ( *it )->get_name() == name ) {
					sec = *it;
					break;
				}
			}

			return sec;
		}

		//------------------------------------------------------------------------------
		section* add( const std::string& name )
		{
			section* new_section = parent->create_section();
			new_section->set_name( name );

			Elf_Half str_index = parent->get_section_name_str_index();
			section* string_table( parent->sections_[str_index] );
			string_section_accessor str_writer( string_table );
			Elf_Word                pos = str_writer.add_string( name );
			new_section->set_name_string_offset( pos );

			return new_section;
		}

		//------------------------------------------------------------------------------
		std::vector<section*>::iterator begin()
		{
			return parent->sections_.begin();
		}

		//------------------------------------------------------------------------------
		std::vector<section*>::iterator end()
		{
			return parent->sections_.end();
		}

		//------------------------------------------------------------------------------
		std::vector<section*>::const_iterator begin() const
		{
			return parent->sections_.cbegin();
		}

		//------------------------------------------------------------------------------
		std::vector<section*>::const_iterator end() const
		{
			return parent->sections_.cend();
		}

		//------------------------------------------------------------------------------
	  private:
		elfio* parent;
	} sections;

	//------------------------------------------------------------------------------
  public:
	friend class Segments;
	class Segments
	{
	  public:
		//------------------------------------------------------------------------------
		Segments( elfio* parent_ ) : parent( parent_ ) {}

		//------------------------------------------------------------------------------
		Elf_Half size() const { return (Elf_Half)parent->segments_.size(); }

		//------------------------------------------------------------------------------
		segment* operator[]( unsigned int index ) const
		{
			return parent->segments_[index];
		}

		//------------------------------------------------------------------------------
		segment* add() { return parent->create_segment(); }

		//------------------------------------------------------------------------------
		std::vector<segment*>::iterator begin()
		{
			return parent->segments_.begin();
		}

		//------------------------------------------------------------------------------
		std::vector<segment*>::iterator end()
		{
			return parent->segments_.end();
		}

		//------------------------------------------------------------------------------
		std::vector<segment*>::const_iterator begin() const
		{
			return parent->segments_.cbegin();
		}

		//------------------------------------------------------------------------------
		std::vector<segment*>::const_iterator end() const
		{
			return parent->segments_.cend();
		}

		//------------------------------------------------------------------------------
	  private:
		elfio* parent;
	} segments;

	//------------------------------------------------------------------------------
  private:
	elf_header*           header;
	std::vector<section*> sections_;
	std::vector<segment*> segments_;
	endianess_convertor   convertor;

	Elf_Xword current_file_pos;
};

} // namespace ELFIO


/*** Start of inlined file: elfio_symbols.hpp ***/
#ifndef ELFIO_SYMBOLS_HPP
#define ELFIO_SYMBOLS_HPP

namespace ELFIO {

//------------------------------------------------------------------------------
template <class S> class symbol_section_accessor_template
{
  public:
	//------------------------------------------------------------------------------
	symbol_section_accessor_template( const elfio& elf_file_,
									  S*           symbol_section_ )
		: elf_file( elf_file_ ), symbol_section( symbol_section_ )
	{
		find_hash_section();
	}

	//------------------------------------------------------------------------------
	Elf_Xword get_symbols_num() const
	{
		Elf_Xword nRet = 0;
		if ( 0 != symbol_section->get_entry_size() ) {
			nRet =
				symbol_section->get_size() / symbol_section->get_entry_size();
		}

		return nRet;
	}

	//------------------------------------------------------------------------------
	bool get_symbol( Elf_Xword      index,
					 std::string&   name,
					 Elf64_Addr&    value,
					 Elf_Xword&     size,
					 unsigned char& bind,
					 unsigned char& type,
					 Elf_Half&      section_index,
					 unsigned char& other ) const
	{
		bool ret = false;

		if ( elf_file.get_class() == ELFCLASS32 ) {
			ret = generic_get_symbol<Elf32_Sym>( index, name, value, size, bind,
												 type, section_index, other );
		}
		else {
			ret = generic_get_symbol<Elf64_Sym>( index, name, value, size, bind,
												 type, section_index, other );
		}

		return ret;
	}

	//------------------------------------------------------------------------------
	bool get_symbol( const std::string& name,
					 Elf64_Addr&        value,
					 Elf_Xword&         size,
					 unsigned char&     bind,
					 unsigned char&     type,
					 Elf_Half&          section_index,
					 unsigned char&     other ) const
	{
		bool ret = false;

		if ( 0 != get_hash_table_index() ) {
			Elf_Word    nbucket = *(const Elf_Word*)hash_section->get_data();
			Elf_Word    nchain  = *(const Elf_Word*)( hash_section->get_data() +
												  sizeof( Elf_Word ) );
			Elf_Word    val = elf_hash( (const unsigned char*)name.c_str() );
			Elf_Word    y   = *(const Elf_Word*)( hash_section->get_data() +
											 ( 2 + val % nbucket ) *
												 sizeof( Elf_Word ) );
			std::string str;
			get_symbol( y, str, value, size, bind, type, section_index, other );
			while ( str != name && STN_UNDEF != y && y < nchain ) {
				y = *(const Elf_Word*)( hash_section->get_data() +
										( 2 + nbucket + y ) *
											sizeof( Elf_Word ) );
				get_symbol( y, str, value, size, bind, type, section_index,
							other );
			}
			if ( str == name ) {
				ret = true;
			}
		}
		else {
			for ( Elf_Xword i = 0; i < get_symbols_num() && !ret; i++ ) {
				std::string symbol_name;
				if ( get_symbol( i, symbol_name, value, size, bind, type,
								 section_index, other ) ) {
					if ( symbol_name == name ) {
						ret = true;
					}
				}
			}
		}

		return ret;
	}

	//------------------------------------------------------------------------------
	bool get_symbol( const Elf64_Addr& value,
					 std::string&      name,
					 Elf_Xword&        size,
					 unsigned char&    bind,
					 unsigned char&    type,
					 Elf_Half&         section_index,
					 unsigned char&    other ) const
	{

		const endianess_convertor& convertor = elf_file.get_convertor();

		Elf_Xword  idx   = 0;
		bool       match = false;
		Elf64_Addr v     = 0;

		if ( elf_file.get_class() == ELFCLASS32 ) {
			match = generic_search_symbols<Elf32_Sym>(
				[&]( const Elf32_Sym* sym ) {
					return convertor( sym->st_value ) == value;
				},
				idx );
		}
		else {
			match = generic_search_symbols<Elf64_Sym>(
				[&]( const Elf64_Sym* sym ) {
					return convertor( sym->st_value ) == value;
				},
				idx );
		}

		if ( match ) {
			return get_symbol( idx, name, v, size, bind, type, section_index,
							   other );
		}

		return false;
	}

	//------------------------------------------------------------------------------
	Elf_Word add_symbol( Elf_Word      name,
						 Elf64_Addr    value,
						 Elf_Xword     size,
						 unsigned char info,
						 unsigned char other,
						 Elf_Half      shndx )
	{
		Elf_Word nRet;

		if ( symbol_section->get_size() == 0 ) {
			if ( elf_file.get_class() == ELFCLASS32 ) {
				nRet = generic_add_symbol<Elf32_Sym>( 0, 0, 0, 0, 0, 0 );
			}
			else {
				nRet = generic_add_symbol<Elf64_Sym>( 0, 0, 0, 0, 0, 0 );
			}
		}

		if ( elf_file.get_class() == ELFCLASS32 ) {
			nRet = generic_add_symbol<Elf32_Sym>( name, value, size, info,
												  other, shndx );
		}
		else {
			nRet = generic_add_symbol<Elf64_Sym>( name, value, size, info,
												  other, shndx );
		}

		return nRet;
	}

	//------------------------------------------------------------------------------
	Elf_Word add_symbol( Elf_Word      name,
						 Elf64_Addr    value,
						 Elf_Xword     size,
						 unsigned char bind,
						 unsigned char type,
						 unsigned char other,
						 Elf_Half      shndx )
	{
		return add_symbol( name, value, size, ELF_ST_INFO( bind, type ), other,
						   shndx );
	}

	//------------------------------------------------------------------------------
	Elf_Word add_symbol( string_section_accessor& pStrWriter,
						 const char*              str,
						 Elf64_Addr               value,
						 Elf_Xword                size,
						 unsigned char            info,
						 unsigned char            other,
						 Elf_Half                 shndx )
	{
		Elf_Word index = pStrWriter.add_string( str );
		return add_symbol( index, value, size, info, other, shndx );
	}

	//------------------------------------------------------------------------------
	Elf_Word add_symbol( string_section_accessor& pStrWriter,
						 const char*              str,
						 Elf64_Addr               value,
						 Elf_Xword                size,
						 unsigned char            bind,
						 unsigned char            type,
						 unsigned char            other,
						 Elf_Half                 shndx )
	{
		return add_symbol( pStrWriter, str, value, size,
						   ELF_ST_INFO( bind, type ), other, shndx );
	}

	//------------------------------------------------------------------------------
	Elf_Xword arrange_local_symbols(
		std::function<void( Elf_Xword first, Elf_Xword second )> func =
			nullptr )
	{
		int nRet = 0;

		if ( elf_file.get_class() == ELFCLASS32 ) {
			nRet = generic_arrange_local_symbols<Elf32_Sym>( func );
		}
		else {
			nRet = generic_arrange_local_symbols<Elf64_Sym>( func );
		}

		return nRet;
	}

	//------------------------------------------------------------------------------
  private:
	//------------------------------------------------------------------------------
	void find_hash_section()
	{
		hash_section       = 0;
		hash_section_index = 0;
		Elf_Half nSecNo    = elf_file.sections.size();
		for ( Elf_Half i = 0; i < nSecNo && 0 == hash_section_index; ++i ) {
			const section* sec = elf_file.sections[i];
			if ( sec->get_link() == symbol_section->get_index() ) {
				hash_section       = sec;
				hash_section_index = i;
			}
		}
	}

	//------------------------------------------------------------------------------
	Elf_Half get_string_table_index() const
	{
		return (Elf_Half)symbol_section->get_link();
	}

	//------------------------------------------------------------------------------
	Elf_Half get_hash_table_index() const { return hash_section_index; }

	//------------------------------------------------------------------------------
	template <class T> const T* generic_get_symbol_ptr( Elf_Xword index ) const
	{
		if ( 0 != symbol_section->get_data() && index < get_symbols_num() ) {
			const T* pSym = reinterpret_cast<const T*>(
				symbol_section->get_data() +
				index * symbol_section->get_entry_size() );

			return pSym;
		}

		return nullptr;
	}

	//------------------------------------------------------------------------------
	template <class T>
	bool generic_search_symbols( std::function<bool( const T* )> match,
								 Elf_Xword&                      idx ) const
	{
		for ( Elf_Xword i = 0; i < get_symbols_num(); i++ ) {
			const T* symPtr = generic_get_symbol_ptr<T>( i );

			if ( symPtr == nullptr )
				return false;

			if ( match( symPtr ) ) {
				idx = i;
				return true;
			}
		}

		return false;
	}

	//------------------------------------------------------------------------------
	template <class T>
	bool generic_get_symbol( Elf_Xword      index,
							 std::string&   name,
							 Elf64_Addr&    value,
							 Elf_Xword&     size,
							 unsigned char& bind,
							 unsigned char& type,
							 Elf_Half&      section_index,
							 unsigned char& other ) const
	{
		bool ret = false;

		if ( 0 != symbol_section->get_data() && index < get_symbols_num() ) {
			const T* pSym = reinterpret_cast<const T*>(
				symbol_section->get_data() +
				index * symbol_section->get_entry_size() );

			const endianess_convertor& convertor = elf_file.get_convertor();

			section* string_section =
				elf_file.sections[get_string_table_index()];
			string_section_accessor str_reader( string_section );
			const char*             pStr =
				str_reader.get_string( convertor( pSym->st_name ) );
			if ( 0 != pStr ) {
				name = pStr;
			}
			value         = convertor( pSym->st_value );
			size          = convertor( pSym->st_size );
			bind          = ELF_ST_BIND( pSym->st_info );
			type          = ELF_ST_TYPE( pSym->st_info );
			section_index = convertor( pSym->st_shndx );
			other         = pSym->st_other;

			ret = true;
		}

		return ret;
	}

	//------------------------------------------------------------------------------
	template <class T>
	Elf_Word generic_add_symbol( Elf_Word      name,
								 Elf64_Addr    value,
								 Elf_Xword     size,
								 unsigned char info,
								 unsigned char other,
								 Elf_Half      shndx )
	{
		const endianess_convertor& convertor = elf_file.get_convertor();

		T entry;
		entry.st_name  = convertor( name );
		entry.st_value = value;
		entry.st_value = convertor( entry.st_value );
		entry.st_size  = size;
		entry.st_size  = convertor( entry.st_size );
		entry.st_info  = convertor( info );
		entry.st_other = convertor( other );
		entry.st_shndx = convertor( shndx );

		symbol_section->append_data( reinterpret_cast<char*>( &entry ),
									 sizeof( entry ) );

		Elf_Word nRet = symbol_section->get_size() / sizeof( entry ) - 1;

		return nRet;
	}

	//------------------------------------------------------------------------------
	template <class T>
	Elf_Xword generic_arrange_local_symbols(
		std::function<void( Elf_Xword first, Elf_Xword second )> func )
	{
		const endianess_convertor& convertor = elf_file.get_convertor();
		const Elf_Xword            size      = symbol_section->get_entry_size();

		Elf_Xword first_not_local =
			1; // Skip the first entry. It is always NOTYPE
		Elf_Xword current = 0;
		Elf_Xword count   = get_symbols_num();

		while ( true ) {
			T* p1 = nullptr;
			T* p2 = nullptr;

			while ( first_not_local < count ) {
				p1 = const_cast<T*>(
					generic_get_symbol_ptr<T>( first_not_local ) );
				if ( ELF_ST_BIND( convertor( p1->st_info ) ) != STB_LOCAL )
					break;
				++first_not_local;
			}

			current = first_not_local + 1;
			while ( current < count ) {
				p2 = const_cast<T*>( generic_get_symbol_ptr<T>( current ) );
				if ( ELF_ST_BIND( convertor( p2->st_info ) ) == STB_LOCAL )
					break;
				++current;
			}

			if ( first_not_local < count && current < count ) {
				if ( func )
					func( first_not_local, current );

				// Swap the symbols
				T tmp;
				std::copy( p1, p1 + 1, &tmp );
				std::copy( p2, p2 + 1, p1 );
				std::copy( &tmp, &tmp + 1, p2 );
			}
			else {
				// Update 'info' field of the section
				symbol_section->set_info( first_not_local );
				break;
			}
		}

		// Elf_Word nRet = symbol_section->get_size() / sizeof(entry) - 1;

		return first_not_local;
	}

	//------------------------------------------------------------------------------
  private:
	const elfio&   elf_file;
	S*             symbol_section;
	Elf_Half       hash_section_index;
	const section* hash_section;
};

using symbol_section_accessor = symbol_section_accessor_template<section>;
using const_symbol_section_accessor =
	symbol_section_accessor_template<const section>;

} // namespace ELFIO

#endif // ELFIO_SYMBOLS_HPP

/*** End of inlined file: elfio_symbols.hpp ***/


/*** Start of inlined file: elfio_note.hpp ***/
#ifndef ELFIO_NOTE_HPP
#define ELFIO_NOTE_HPP

namespace ELFIO {

//------------------------------------------------------------------------------
// There are discrepancies in documentations. SCO documentation
// (http://www.sco.com/developers/gabi/latest/ch5.pheader.html#note_section)
// requires 8 byte entries alignment for 64-bit ELF file,
// but Oracle's definition uses the same structure
// for 32-bit and 64-bit formats.
// (https://docs.oracle.com/cd/E23824_01/html/819-0690/chapter6-18048.html)
//
// It looks like EM_X86_64 Linux implementation is similar to Oracle's
// definition. Therefore, the same alignment works for both formats
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
template <class S> class note_section_accessor_template
{
  public:
	//------------------------------------------------------------------------------
	note_section_accessor_template( const elfio& elf_file_, S* section_ )
		: elf_file( elf_file_ ), note_section( section_ )
	{
		process_section();
	}

	//------------------------------------------------------------------------------
	Elf_Word get_notes_num() const
	{
		return (Elf_Word)note_start_positions.size();
	}

	//------------------------------------------------------------------------------
	bool get_note( Elf_Word     index,
				   Elf_Word&    type,
				   std::string& name,
				   void*&       desc,
				   Elf_Word&    descSize ) const
	{
		if ( index >= note_section->get_size() ) {
			return false;
		}

		const char* pData =
			note_section->get_data() + note_start_positions[index];
		int align = sizeof( Elf_Word );

		const endianess_convertor& convertor = elf_file.get_convertor();
		type            = convertor( *(const Elf_Word*)( pData + 2 * align ) );
		Elf_Word namesz = convertor( *(const Elf_Word*)( pData ) );
		descSize = convertor( *(const Elf_Word*)( pData + sizeof( namesz ) ) );

		Elf_Xword max_name_size =
			note_section->get_size() - note_start_positions[index];
		if ( namesz < 1 || namesz > max_name_size ||
			 (Elf_Xword)namesz + descSize > max_name_size ) {
			return false;
		}
		name.assign( pData + 3 * align, namesz - 1 );
		if ( 0 == descSize ) {
			desc = 0;
		}
		else {
			desc =
				const_cast<char*>( pData + 3 * align +
								   ( ( namesz + align - 1 ) / align ) * align );
		}

		return true;
	}

	//------------------------------------------------------------------------------
	void add_note( Elf_Word           type,
				   const std::string& name,
				   const void*        desc,
				   Elf_Word           descSize )
	{
		const endianess_convertor& convertor = elf_file.get_convertor();

		int         align       = sizeof( Elf_Word );
		Elf_Word    nameLen     = (Elf_Word)name.size() + 1;
		Elf_Word    nameLenConv = convertor( nameLen );
		std::string buffer( reinterpret_cast<char*>( &nameLenConv ), align );
		Elf_Word    descSizeConv = convertor( descSize );

		buffer.append( reinterpret_cast<char*>( &descSizeConv ), align );
		type = convertor( type );
		buffer.append( reinterpret_cast<char*>( &type ), align );
		buffer.append( name );
		buffer.append( 1, '\x00' );
		const char pad[] = { '\0', '\0', '\0', '\0' };
		if ( nameLen % align != 0 ) {
			buffer.append( pad, align - nameLen % align );
		}
		if ( desc != 0 && descSize != 0 ) {
			buffer.append( reinterpret_cast<const char*>( desc ), descSize );
			if ( descSize % align != 0 ) {
				buffer.append( pad, align - descSize % align );
			}
		}

		note_start_positions.push_back( note_section->get_size() );
		note_section->append_data( buffer );
	}

  private:
	//------------------------------------------------------------------------------
	void process_section()
	{
		const endianess_convertor& convertor = elf_file.get_convertor();
		const char*                data      = note_section->get_data();
		Elf_Xword                  size      = note_section->get_size();
		Elf_Xword                  current   = 0;

		note_start_positions.clear();

		// Is it empty?
		if ( 0 == data || 0 == size ) {
			return;
		}

		Elf_Word align = sizeof( Elf_Word );
		while ( current + (Elf_Xword)3 * align <= size ) {
			note_start_positions.push_back( current );
			Elf_Word namesz = convertor( *(const Elf_Word*)( data + current ) );
			Elf_Word descsz = convertor(
				*(const Elf_Word*)( data + current + sizeof( namesz ) ) );

			current += (Elf_Xword)3 * sizeof( Elf_Word ) +
					   ( ( namesz + align - 1 ) / align ) * (Elf_Xword)align +
					   ( ( descsz + align - 1 ) / align ) * (Elf_Xword)align;
		}
	}

	//------------------------------------------------------------------------------
  private:
	const elfio&           elf_file;
	S*                     note_section;
	std::vector<Elf_Xword> note_start_positions;
};

using note_section_accessor = note_section_accessor_template<section>;
using const_note_section_accessor =
	note_section_accessor_template<const section>;

} // namespace ELFIO

#endif // ELFIO_NOTE_HPP

/*** End of inlined file: elfio_note.hpp ***/


/*** Start of inlined file: elfio_relocation.hpp ***/
#ifndef ELFIO_RELOCATION_HPP
#define ELFIO_RELOCATION_HPP

namespace ELFIO {

template <typename T> struct get_sym_and_type;
template <> struct get_sym_and_type<Elf32_Rel>
{
	static int get_r_sym( Elf_Xword info )
	{
		return ELF32_R_SYM( (Elf_Word)info );
	}
	static int get_r_type( Elf_Xword info )
	{
		return ELF32_R_TYPE( (Elf_Word)info );
	}
};
template <> struct get_sym_and_type<Elf32_Rela>
{
	static int get_r_sym( Elf_Xword info )
	{
		return ELF32_R_SYM( (Elf_Word)info );
	}
	static int get_r_type( Elf_Xword info )
	{
		return ELF32_R_TYPE( (Elf_Word)info );
	}
};
template <> struct get_sym_and_type<Elf64_Rel>
{
	static int get_r_sym( Elf_Xword info ) { return ELF64_R_SYM( info ); }
	static int get_r_type( Elf_Xword info ) { return ELF64_R_TYPE( info ); }
};
template <> struct get_sym_and_type<Elf64_Rela>
{
	static int get_r_sym( Elf_Xword info ) { return ELF64_R_SYM( info ); }
	static int get_r_type( Elf_Xword info ) { return ELF64_R_TYPE( info ); }
};

//------------------------------------------------------------------------------
template <class S> class relocation_section_accessor_template
{
  public:
	//------------------------------------------------------------------------------
	relocation_section_accessor_template( const elfio& elf_file_, S* section_ )
		: elf_file( elf_file_ ), relocation_section( section_ )
	{
	}

	//------------------------------------------------------------------------------
	Elf_Xword get_entries_num() const
	{
		Elf_Xword nRet = 0;

		if ( 0 != relocation_section->get_entry_size() ) {
			nRet = relocation_section->get_size() /
				   relocation_section->get_entry_size();
		}

		return nRet;
	}

	//------------------------------------------------------------------------------
	bool get_entry( Elf_Xword   index,
					Elf64_Addr& offset,
					Elf_Word&   symbol,
					Elf_Word&   type,
					Elf_Sxword& addend ) const
	{
		if ( index >= get_entries_num() ) { // Is index valid
			return false;
		}

		if ( elf_file.get_class() == ELFCLASS32 ) {
			if ( SHT_REL == relocation_section->get_type() ) {
				generic_get_entry_rel<Elf32_Rel>( index, offset, symbol, type,
												  addend );
			}
			else if ( SHT_RELA == relocation_section->get_type() ) {
				generic_get_entry_rela<Elf32_Rela>( index, offset, symbol, type,
													addend );
			}
		}
		else {
			if ( SHT_REL == relocation_section->get_type() ) {
				generic_get_entry_rel<Elf64_Rel>( index, offset, symbol, type,
												  addend );
			}
			else if ( SHT_RELA == relocation_section->get_type() ) {
				generic_get_entry_rela<Elf64_Rela>( index, offset, symbol, type,
													addend );
			}
		}

		return true;
	}

	//------------------------------------------------------------------------------
	bool get_entry( Elf_Xword    index,
					Elf64_Addr&  offset,
					Elf64_Addr&  symbolValue,
					std::string& symbolName,
					Elf_Word&    type,
					Elf_Sxword&  addend,
					Elf_Sxword&  calcValue ) const
	{
		// Do regular job
		Elf_Word symbol;
		bool     ret = get_entry( index, offset, symbol, type, addend );

		// Find the symbol
		Elf_Xword     size;
		unsigned char bind;
		unsigned char symbolType;
		Elf_Half      section;
		unsigned char other;

		symbol_section_accessor symbols(
			elf_file, elf_file.sections[get_symbol_table_index()] );
		ret = ret && symbols.get_symbol( symbol, symbolName, symbolValue, size,
										 bind, symbolType, section, other );

		if ( ret ) { // Was it successful?
			switch ( type ) {
			case R_386_NONE: // none
				calcValue = 0;
				break;
			case R_386_32: // S + A
				calcValue = symbolValue + addend;
				break;
			case R_386_PC32: // S + A - P
				calcValue = symbolValue + addend - offset;
				break;
			case R_386_GOT32: // G + A - P
				calcValue = 0;
				break;
			case R_386_PLT32: // L + A - P
				calcValue = 0;
				break;
			case R_386_COPY: // none
				calcValue = 0;
				break;
			case R_386_GLOB_DAT: // S
			case R_386_JMP_SLOT: // S
				calcValue = symbolValue;
				break;
			case R_386_RELATIVE: // B + A
				calcValue = addend;
				break;
			case R_386_GOTOFF: // S + A - GOT
				calcValue = 0;
				break;
			case R_386_GOTPC: // GOT + A - P
				calcValue = 0;
				break;
			default: // Not recognized symbol!
				calcValue = 0;
				break;
			}
		}

		return ret;
	}

	//------------------------------------------------------------------------------
	bool set_entry( Elf_Xword  index,
					Elf64_Addr offset,
					Elf_Word   symbol,
					Elf_Word   type,
					Elf_Sxword addend )
	{
		if ( index >= get_entries_num() ) { // Is index valid
			return false;
		}

		if ( elf_file.get_class() == ELFCLASS32 ) {
			if ( SHT_REL == relocation_section->get_type() ) {
				generic_set_entry_rel<Elf32_Rel>( index, offset, symbol, type,
												  addend );
			}
			else if ( SHT_RELA == relocation_section->get_type() ) {
				generic_set_entry_rela<Elf32_Rela>( index, offset, symbol, type,
													addend );
			}
		}
		else {
			if ( SHT_REL == relocation_section->get_type() ) {
				generic_set_entry_rel<Elf64_Rel>( index, offset, symbol, type,
												  addend );
			}
			else if ( SHT_RELA == relocation_section->get_type() ) {
				generic_set_entry_rela<Elf64_Rela>( index, offset, symbol, type,
													addend );
			}
		}

		return true;
	}

	//------------------------------------------------------------------------------
	void add_entry( Elf64_Addr offset, Elf_Xword info )
	{
		if ( elf_file.get_class() == ELFCLASS32 ) {
			generic_add_entry<Elf32_Rel>( offset, info );
		}
		else {
			generic_add_entry<Elf64_Rel>( offset, info );
		}
	}

	//------------------------------------------------------------------------------
	void add_entry( Elf64_Addr offset, Elf_Word symbol, unsigned char type )
	{
		Elf_Xword info;
		if ( elf_file.get_class() == ELFCLASS32 ) {
			info = ELF32_R_INFO( (Elf_Xword)symbol, type );
		}
		else {
			info = ELF64_R_INFO( (Elf_Xword)symbol, type );
		}

		add_entry( offset, info );
	}

	//------------------------------------------------------------------------------
	void add_entry( Elf64_Addr offset, Elf_Xword info, Elf_Sxword addend )
	{
		if ( elf_file.get_class() == ELFCLASS32 ) {
			generic_add_entry<Elf32_Rela>( offset, info, addend );
		}
		else {
			generic_add_entry<Elf64_Rela>( offset, info, addend );
		}
	}

	//------------------------------------------------------------------------------
	void add_entry( Elf64_Addr    offset,
					Elf_Word      symbol,
					unsigned char type,
					Elf_Sxword    addend )
	{
		Elf_Xword info;
		if ( elf_file.get_class() == ELFCLASS32 ) {
			info = ELF32_R_INFO( (Elf_Xword)symbol, type );
		}
		else {
			info = ELF64_R_INFO( (Elf_Xword)symbol, type );
		}

		add_entry( offset, info, addend );
	}

	//------------------------------------------------------------------------------
	void add_entry( string_section_accessor str_writer,
					const char*             str,
					symbol_section_accessor sym_writer,
					Elf64_Addr              value,
					Elf_Word                size,
					unsigned char           sym_info,
					unsigned char           other,
					Elf_Half                shndx,
					Elf64_Addr              offset,
					unsigned char           type )
	{
		Elf_Word str_index = str_writer.add_string( str );
		Elf_Word sym_index = sym_writer.add_symbol( str_index, value, size,
													sym_info, other, shndx );
		add_entry( offset, sym_index, type );
	}

	//------------------------------------------------------------------------------
	void swap_symbols( Elf_Xword first, Elf_Xword second )
	{
		Elf64_Addr offset;
		Elf_Word   symbol;
		Elf_Word   rtype;
		Elf_Sxword addend;
		for ( Elf_Word i = 0; i < get_entries_num(); i++ ) {
			get_entry( i, offset, symbol, rtype, addend );
			if ( symbol == first ) {
				set_entry( i, offset, (Elf_Word)second, rtype, addend );
			}
			if ( symbol == second ) {
				set_entry( i, offset, (Elf_Word)first, rtype, addend );
			}
		}
	}

	//------------------------------------------------------------------------------
  private:
	//------------------------------------------------------------------------------
	Elf_Half get_symbol_table_index() const
	{
		return (Elf_Half)relocation_section->get_link();
	}

	//------------------------------------------------------------------------------
	template <class T>
	void generic_get_entry_rel( Elf_Xword   index,
								Elf64_Addr& offset,
								Elf_Word&   symbol,
								Elf_Word&   type,
								Elf_Sxword& addend ) const
	{
		const endianess_convertor& convertor = elf_file.get_convertor();

		const T* pEntry = reinterpret_cast<const T*>(
			relocation_section->get_data() +
			index * relocation_section->get_entry_size() );
		offset        = convertor( pEntry->r_offset );
		Elf_Xword tmp = convertor( pEntry->r_info );
		symbol        = get_sym_and_type<T>::get_r_sym( tmp );
		type          = get_sym_and_type<T>::get_r_type( tmp );
		addend        = 0;
	}

	//------------------------------------------------------------------------------
	template <class T>
	void generic_get_entry_rela( Elf_Xword   index,
								 Elf64_Addr& offset,
								 Elf_Word&   symbol,
								 Elf_Word&   type,
								 Elf_Sxword& addend ) const
	{
		const endianess_convertor& convertor = elf_file.get_convertor();

		const T* pEntry = reinterpret_cast<const T*>(
			relocation_section->get_data() +
			index * relocation_section->get_entry_size() );
		offset        = convertor( pEntry->r_offset );
		Elf_Xword tmp = convertor( pEntry->r_info );
		symbol        = get_sym_and_type<T>::get_r_sym( tmp );
		type          = get_sym_and_type<T>::get_r_type( tmp );
		addend        = convertor( pEntry->r_addend );
	}

	//------------------------------------------------------------------------------
	template <class T>
	void generic_set_entry_rel( Elf_Xword  index,
								Elf64_Addr offset,
								Elf_Word   symbol,
								Elf_Word   type,
								Elf_Sxword )
	{
		const endianess_convertor& convertor = elf_file.get_convertor();

		T* pEntry = const_cast<T*>( reinterpret_cast<const T*>(
			relocation_section->get_data() +
			index * relocation_section->get_entry_size() ) );

		if ( elf_file.get_class() == ELFCLASS32 ) {
			pEntry->r_info = ELF32_R_INFO( (Elf_Xword)symbol, type );
		}
		else {
			pEntry->r_info = ELF64_R_INFO( (Elf_Xword)symbol, type );
		}
		pEntry->r_offset = offset;
		pEntry->r_offset = convertor( pEntry->r_offset );
		pEntry->r_info   = convertor( pEntry->r_info );
	}

	//------------------------------------------------------------------------------
	template <class T>
	void generic_set_entry_rela( Elf_Xword  index,
								 Elf64_Addr offset,
								 Elf_Word   symbol,
								 Elf_Word   type,
								 Elf_Sxword addend )
	{
		const endianess_convertor& convertor = elf_file.get_convertor();

		T* pEntry = const_cast<T*>( reinterpret_cast<const T*>(
			relocation_section->get_data() +
			index * relocation_section->get_entry_size() ) );

		if ( elf_file.get_class() == ELFCLASS32 ) {
			pEntry->r_info = ELF32_R_INFO( (Elf_Xword)symbol, type );
		}
		else {
			pEntry->r_info = ELF64_R_INFO( (Elf_Xword)symbol, type );
		}
		pEntry->r_offset = offset;
		pEntry->r_addend = addend;
		pEntry->r_offset = convertor( pEntry->r_offset );
		pEntry->r_info   = convertor( pEntry->r_info );
		pEntry->r_addend = convertor( pEntry->r_addend );
	}

	//------------------------------------------------------------------------------
	template <class T>
	void generic_add_entry( Elf64_Addr offset, Elf_Xword info )
	{
		const endianess_convertor& convertor = elf_file.get_convertor();

		T entry;
		entry.r_offset = offset;
		entry.r_info   = info;
		entry.r_offset = convertor( entry.r_offset );
		entry.r_info   = convertor( entry.r_info );

		relocation_section->append_data( reinterpret_cast<char*>( &entry ),
										 sizeof( entry ) );
	}

	//------------------------------------------------------------------------------
	template <class T>
	void
	generic_add_entry( Elf64_Addr offset, Elf_Xword info, Elf_Sxword addend )
	{
		const endianess_convertor& convertor = elf_file.get_convertor();

		T entry;
		entry.r_offset = offset;
		entry.r_info   = info;
		entry.r_addend = addend;
		entry.r_offset = convertor( entry.r_offset );
		entry.r_info   = convertor( entry.r_info );
		entry.r_addend = convertor( entry.r_addend );

		relocation_section->append_data( reinterpret_cast<char*>( &entry ),
										 sizeof( entry ) );
	}

	//------------------------------------------------------------------------------
  private:
	const elfio& elf_file;
	S*           relocation_section;
};

using relocation_section_accessor =
	relocation_section_accessor_template<section>;
using const_relocation_section_accessor =
	relocation_section_accessor_template<const section>;

} // namespace ELFIO

#endif // ELFIO_RELOCATION_HPP

/*** End of inlined file: elfio_relocation.hpp ***/


/*** Start of inlined file: elfio_dynamic.hpp ***/
#ifndef ELFIO_DYNAMIC_HPP
#define ELFIO_DYNAMIC_HPP

namespace ELFIO {

//------------------------------------------------------------------------------
template <class S> class dynamic_section_accessor_template
{
  public:
	//------------------------------------------------------------------------------
	dynamic_section_accessor_template( const elfio& elf_file_, S* section_ )
		: elf_file( elf_file_ ), dynamic_section( section_ )
	{
	}

	//------------------------------------------------------------------------------
	Elf_Xword get_entries_num() const
	{
		Elf_Xword nRet = 0;

		if ( 0 != dynamic_section->get_entry_size() ) {
			nRet =
				dynamic_section->get_size() / dynamic_section->get_entry_size();
		}

		return nRet;
	}

	//------------------------------------------------------------------------------
	bool get_entry( Elf_Xword    index,
					Elf_Xword&   tag,
					Elf_Xword&   value,
					std::string& str ) const
	{
		if ( index >= get_entries_num() ) { // Is index valid
			return false;
		}

		if ( elf_file.get_class() == ELFCLASS32 ) {
			generic_get_entry_dyn<Elf32_Dyn>( index, tag, value );
		}
		else {
			generic_get_entry_dyn<Elf64_Dyn>( index, tag, value );
		}

		// If the tag may have a string table reference, prepare the string
		if ( tag == DT_NEEDED || tag == DT_SONAME || tag == DT_RPATH ||
			 tag == DT_RUNPATH ) {
			string_section_accessor strsec =
				elf_file.sections[get_string_table_index()];
			const char* result = strsec.get_string( value );
			if ( 0 == result ) {
				str.clear();
				return false;
			}
			str = result;
		}
		else {
			str.clear();
		}

		return true;
	}

	//------------------------------------------------------------------------------
	void add_entry( Elf_Xword tag, Elf_Xword value )
	{
		if ( elf_file.get_class() == ELFCLASS32 ) {
			generic_add_entry<Elf32_Dyn>( tag, value );
		}
		else {
			generic_add_entry<Elf64_Dyn>( tag, value );
		}
	}

	//------------------------------------------------------------------------------
	void add_entry( Elf_Xword tag, const std::string& str )
	{
		string_section_accessor strsec =
			elf_file.sections[get_string_table_index()];
		Elf_Xword value = strsec.add_string( str );
		add_entry( tag, value );
	}

	//------------------------------------------------------------------------------
  private:
	//------------------------------------------------------------------------------
	Elf_Half get_string_table_index() const
	{
		return (Elf_Half)dynamic_section->get_link();
	}

	//------------------------------------------------------------------------------
	template <class T>
	void generic_get_entry_dyn( Elf_Xword  index,
								Elf_Xword& tag,
								Elf_Xword& value ) const
	{
		const endianess_convertor& convertor = elf_file.get_convertor();

		// Check unusual case when dynamic section has no data
		if ( dynamic_section->get_data() == 0 ||
			 ( index + 1 ) * dynamic_section->get_entry_size() >
				 dynamic_section->get_size() ) {
			tag   = DT_NULL;
			value = 0;
			return;
		}

		const T* pEntry = reinterpret_cast<const T*>(
			dynamic_section->get_data() +
			index * dynamic_section->get_entry_size() );
		tag = convertor( pEntry->d_tag );
		switch ( tag ) {
		case DT_NULL:
		case DT_SYMBOLIC:
		case DT_TEXTREL:
		case DT_BIND_NOW:
			value = 0;
			break;
		case DT_NEEDED:
		case DT_PLTRELSZ:
		case DT_RELASZ:
		case DT_RELAENT:
		case DT_STRSZ:
		case DT_SYMENT:
		case DT_SONAME:
		case DT_RPATH:
		case DT_RELSZ:
		case DT_RELENT:
		case DT_PLTREL:
		case DT_INIT_ARRAYSZ:
		case DT_FINI_ARRAYSZ:
		case DT_RUNPATH:
		case DT_FLAGS:
		case DT_PREINIT_ARRAYSZ:
			value = convertor( pEntry->d_un.d_val );
			break;
		case DT_PLTGOT:
		case DT_HASH:
		case DT_STRTAB:
		case DT_SYMTAB:
		case DT_RELA:
		case DT_INIT:
		case DT_FINI:
		case DT_REL:
		case DT_DEBUG:
		case DT_JMPREL:
		case DT_INIT_ARRAY:
		case DT_FINI_ARRAY:
		case DT_PREINIT_ARRAY:
		default:
			value = convertor( pEntry->d_un.d_ptr );
			break;
		}
	}

	//------------------------------------------------------------------------------
	template <class T> void generic_add_entry( Elf_Xword tag, Elf_Xword value )
	{
		const endianess_convertor& convertor = elf_file.get_convertor();

		T entry;

		switch ( tag ) {
		case DT_NULL:
		case DT_SYMBOLIC:
		case DT_TEXTREL:
		case DT_BIND_NOW:
			value = 0;
		case DT_NEEDED:
		case DT_PLTRELSZ:
		case DT_RELASZ:
		case DT_RELAENT:
		case DT_STRSZ:
		case DT_SYMENT:
		case DT_SONAME:
		case DT_RPATH:
		case DT_RELSZ:
		case DT_RELENT:
		case DT_PLTREL:
		case DT_INIT_ARRAYSZ:
		case DT_FINI_ARRAYSZ:
		case DT_RUNPATH:
		case DT_FLAGS:
		case DT_PREINIT_ARRAYSZ:
			entry.d_un.d_val = convertor( value );
			break;
		case DT_PLTGOT:
		case DT_HASH:
		case DT_STRTAB:
		case DT_SYMTAB:
		case DT_RELA:
		case DT_INIT:
		case DT_FINI:
		case DT_REL:
		case DT_DEBUG:
		case DT_JMPREL:
		case DT_INIT_ARRAY:
		case DT_FINI_ARRAY:
		case DT_PREINIT_ARRAY:
		default:
			entry.d_un.d_ptr = convertor( value );
			break;
		}

		entry.d_tag = convertor( tag );

		dynamic_section->append_data( reinterpret_cast<char*>( &entry ),
									  sizeof( entry ) );
	}

	//------------------------------------------------------------------------------
  private:
	const elfio& elf_file;
	S*           dynamic_section;
};

using dynamic_section_accessor = dynamic_section_accessor_template<section>;
using const_dynamic_section_accessor =
	dynamic_section_accessor_template<const section>;

} // namespace ELFIO

#endif // ELFIO_DYNAMIC_HPP

/*** End of inlined file: elfio_dynamic.hpp ***/


/*** Start of inlined file: elfio_modinfo.hpp ***/
#ifndef ELFIO_MODINFO_HPP
#define ELFIO_MODINFO_HPP

#include <string>
#include <vector>

namespace ELFIO {

//------------------------------------------------------------------------------
template <class S> class modinfo_section_accessor_template
{
  public:
	//------------------------------------------------------------------------------
	modinfo_section_accessor_template( S* section_ )
		: modinfo_section( section_ )
	{
		process_section();
	}

	//------------------------------------------------------------------------------
	Elf_Word get_attribute_num() const { return (Elf_Word)content.size(); }

	//------------------------------------------------------------------------------
	bool
	get_attribute( Elf_Word no, std::string& field, std::string& value ) const
	{
		if ( no < content.size() ) {
			field = content[no].first;
			value = content[no].second;
			return true;
		}

		return false;
	}

	//------------------------------------------------------------------------------
	bool get_attribute( std::string field_name, std::string& value ) const
	{
		for ( auto i = content.begin(); i != content.end(); i++ ) {
			if ( field_name == i->first ) {
				value = i->second;
				return true;
			}
		}

		return false;
	}

	//------------------------------------------------------------------------------
	Elf_Word add_attribute( std::string field, std::string value )
	{
		Elf_Word current_position = 0;

		if ( modinfo_section ) {
			// Strings are addeded to the end of the current section data
			current_position = (Elf_Word)modinfo_section->get_size();

			std::string attribute = field + "=" + value;

			modinfo_section->append_data( attribute + '\0' );
			content.push_back(
				std::pair<std::string, std::string>( field, value ) );
		}

		return current_position;
	}

	//------------------------------------------------------------------------------
  private:
	void process_section()
	{
		const char* pdata = modinfo_section->get_data();
		if ( pdata ) {
			ELFIO::Elf_Xword i = 0;
			while ( i < modinfo_section->get_size() ) {
				while ( i < modinfo_section->get_size() && !pdata[i] )
					i++;
				if ( i < modinfo_section->get_size() ) {
					std::string                         info = pdata + i;
					size_t                              loc  = info.find( '=' );
					std::pair<std::string, std::string> attribute(
						info.substr( 0, loc ), info.substr( loc + 1 ) );

					content.push_back( attribute );

					i += info.length();
				}
			}
		}
	}

	//------------------------------------------------------------------------------
  private:
	S*                                               modinfo_section;
	std::vector<std::pair<std::string, std::string>> content;
};

using modinfo_section_accessor = modinfo_section_accessor_template<section>;
using const_modinfo_section_accessor =
	modinfo_section_accessor_template<const section>;

} // namespace ELFIO

#endif // ELFIO_MODINFO_HPP

/*** End of inlined file: elfio_modinfo.hpp ***/

#ifdef _MSC_VER
#pragma warning( pop )
#endif

#endif // ELFIO_HPP

/*** End of inlined file: elfio.hpp ***/


namespace ELFIO {

static struct class_table_t
{
	const char  key;
	const char* str;
} class_table[] = {
	{ ELFCLASS32, "ELF32" },
	{ ELFCLASS64, "ELF64" },
};

static struct endian_table_t
{
	const char  key;
	const char* str;
} endian_table[] = {
	{ ELFDATANONE, "None" },
	{ ELFDATA2LSB, "Little endian" },
	{ ELFDATA2MSB, "Big endian" },
};

static struct version_table_t
{
	const Elf64_Word key;
	const char*      str;
} version_table[] = {
	{ EV_NONE, "None" },
	{ EV_CURRENT, "Current" },
};

static struct type_table_t
{
	const Elf32_Half key;
	const char*      str;
} type_table[] = {
	{ ET_NONE, "No file type" },    { ET_REL, "Relocatable file" },
	{ ET_EXEC, "Executable file" }, { ET_DYN, "Shared object file" },
	{ ET_CORE, "Core file" },
};

static struct machine_table_t
{
	const Elf64_Half key;
	const char*      str;
} machine_table[] = {
	{ EM_NONE, "No machine" },
	{ EM_M32, "AT&T WE 32100" },
	{ EM_SPARC, "SUN SPARC" },
	{ EM_386, "Intel 80386" },
	{ EM_68K, "Motorola m68k family" },
	{ EM_88K, "Motorola m88k family" },
	{ EM_486, "Intel 80486// Reserved for future use" },
	{ EM_860, "Intel 80860" },
	{ EM_MIPS, "MIPS R3000 (officially, big-endian only)" },
	{ EM_S370, "IBM System/370" },
	{ EM_MIPS_RS3_LE,
	  "MIPS R3000 little-endian (Oct 4 1999 Draft) Deprecated" },
	{ EM_res011, "Reserved" },
	{ EM_res012, "Reserved" },
	{ EM_res013, "Reserved" },
	{ EM_res014, "Reserved" },
	{ EM_PARISC, "HPPA" },
	{ EM_res016, "Reserved" },
	{ EM_VPP550, "Fujitsu VPP500" },
	{ EM_SPARC32PLUS, "Sun's v8plus" },
	{ EM_960, "Intel 80960" },
	{ EM_PPC, "PowerPC" },
	{ EM_PPC64, "64-bit PowerPC" },
	{ EM_S390, "IBM S/390" },
	{ EM_SPU, "Sony/Toshiba/IBM SPU" },
	{ EM_res024, "Reserved" },
	{ EM_res025, "Reserved" },
	{ EM_res026, "Reserved" },
	{ EM_res027, "Reserved" },
	{ EM_res028, "Reserved" },
	{ EM_res029, "Reserved" },
	{ EM_res030, "Reserved" },
	{ EM_res031, "Reserved" },
	{ EM_res032, "Reserved" },
	{ EM_res033, "Reserved" },
	{ EM_res034, "Reserved" },
	{ EM_res035, "Reserved" },
	{ EM_V800, "NEC V800 series" },
	{ EM_FR20, "Fujitsu FR20" },
	{ EM_RH32, "TRW RH32" },
	{ EM_MCORE, "Motorola M*Core // May also be taken by Fujitsu MMA" },
	{ EM_RCE, "Old name for MCore" },
	{ EM_ARM, "ARM" },
	{ EM_OLD_ALPHA, "Digital Alpha" },
	{ EM_SH, "Renesas (formerly Hitachi) / SuperH SH" },
	{ EM_SPARCV9, "SPARC v9 64-bit" },
	{ EM_TRICORE, "Siemens Tricore embedded processor" },
	{ EM_ARC, "ARC Cores" },
	{ EM_H8_300, "Renesas (formerly Hitachi) H8/300" },
	{ EM_H8_300H, "Renesas (formerly Hitachi) H8/300H" },
	{ EM_H8S, "Renesas (formerly Hitachi) H8S" },
	{ EM_H8_500, "Renesas (formerly Hitachi) H8/500" },
	{ EM_IA_64, "Intel IA-64 Processor" },
	{ EM_MIPS_X, "Stanford MIPS-X" },
	{ EM_COLDFIRE, "Motorola Coldfire" },
	{ EM_68HC12, "Motorola M68HC12" },
	{ EM_MMA, "Fujitsu Multimedia Accelerator" },
	{ EM_PCP, "Siemens PCP" },
	{ EM_NCPU, "Sony nCPU embedded RISC processor" },
	{ EM_NDR1, "Denso NDR1 microprocesspr" },
	{ EM_STARCORE, "Motorola Star*Core processor" },
	{ EM_ME16, "Toyota ME16 processor" },
	{ EM_ST100, "STMicroelectronics ST100 processor" },
	{ EM_TINYJ, "Advanced Logic Corp. TinyJ embedded processor" },
	{ EM_X86_64, "Advanced Micro Devices X86-64 processor" },
	{ EM_PDSP, "Sony DSP Processor" },
	{ EM_PDP10, "Digital Equipment Corp. PDP-10" },
	{ EM_PDP11, "Digital Equipment Corp. PDP-11" },
	{ EM_FX66, "Siemens FX66 microcontroller" },
	{ EM_ST9PLUS, "STMicroelectronics ST9+ 8/16 bit microcontroller" },
	{ EM_ST7, "STMicroelectronics ST7 8-bit microcontroller" },
	{ EM_68HC16, "Motorola MC68HC16 Microcontroller" },
	{ EM_68HC11, "Motorola MC68HC11 Microcontroller" },
	{ EM_68HC08, "Motorola MC68HC08 Microcontroller" },
	{ EM_68HC05, "Motorola MC68HC05 Microcontroller" },
	{ EM_SVX, "Silicon Graphics SVx" },
	{ EM_ST19, "STMicroelectronics ST19 8-bit cpu" },
	{ EM_VAX, "Digital VAX" },
	{ EM_CRIS, "Axis Communications 32-bit embedded processor" },
	{ EM_JAVELIN, "Infineon Technologies 32-bit embedded cpu" },
	{ EM_FIREPATH, "Element 14 64-bit DSP processor" },
	{ EM_ZSP, "LSI Logic's 16-bit DSP processor" },
	{ EM_MMIX, "Donald Knuth's educational 64-bit processor" },
	{ EM_HUANY, "Harvard's machine-independent format" },
	{ EM_PRISM, "SiTera Prism" },
	{ EM_AVR, "Atmel AVR 8-bit microcontroller" },
	{ EM_FR30, "Fujitsu FR30" },
	{ EM_D10V, "Mitsubishi D10V" },
	{ EM_D30V, "Mitsubishi D30V" },
	{ EM_V850, "NEC v850" },
	{ EM_M32R, "Renesas M32R (formerly Mitsubishi M32R)" },
	{ EM_MN10300, "Matsushita MN10300" },
	{ EM_MN10200, "Matsushita MN10200" },
	{ EM_PJ, "picoJava" },
	{ EM_OPENRISC, "OpenRISC 32-bit embedded processor" },
	{ EM_ARC_A5, "ARC Cores Tangent-A5" },
	{ EM_XTENSA, "Tensilica Xtensa Architecture" },
	{ EM_VIDEOCORE, "Alphamosaic VideoCore processor" },
	{ EM_TMM_GPP, "Thompson Multimedia General Purpose Processor" },
	{ EM_NS32K, "National Semiconductor 32000 series" },
	{ EM_TPC, "Tenor Network TPC processor" },
	{ EM_SNP1K, "Trebia SNP 1000 processor" },
	{ EM_ST200, "STMicroelectronics ST200 microcontroller" },
	{ EM_IP2K, "Ubicom IP2022 micro controller" },
	{ EM_MAX, "MAX Processor" },
	{ EM_CR, "National Semiconductor CompactRISC" },
	{ EM_F2MC16, "Fujitsu F2MC16" },
	{ EM_MSP430, "TI msp430 micro controller" },
	{ EM_BLACKFIN, "ADI Blackfin" },
	{ EM_SE_C33, "S1C33 Family of Seiko Epson processors" },
	{ EM_SEP, "Sharp embedded microprocessor" },
	{ EM_ARCA, "Arca RISC Microprocessor" },
	{ EM_UNICORE, "Microprocessor series from PKU-Unity Ltd. and MPRC of "
				  "Peking University" },
	{ EM_EXCESS, "eXcess: 16/32/64-bit configurable embedded CPU" },
	{ EM_DXP, "Icera Semiconductor Inc. Deep Execution Processor" },
	{ EM_ALTERA_NIOS2, "Altera Nios II soft-core processor" },
	{ EM_CRX, "National Semiconductor CRX" },
	{ EM_XGATE, "Motorola XGATE embedded processor" },
	{ EM_C166, "Infineon C16x/XC16x processor" },
	{ EM_M16C, "Renesas M16C series microprocessors" },
	{ EM_DSPIC30F, "Microchip Technology dsPIC30F Digital Signal Controller" },
	{ EM_CE, "Freescale Communication Engine RISC core" },
	{ EM_M32C, "Renesas M32C series microprocessors" },
	{ EM_res121, "Reserved" },
	{ EM_res122, "Reserved" },
	{ EM_res123, "Reserved" },
	{ EM_res124, "Reserved" },
	{ EM_res125, "Reserved" },
	{ EM_res126, "Reserved" },
	{ EM_res127, "Reserved" },
	{ EM_res128, "Reserved" },
	{ EM_res129, "Reserved" },
	{ EM_res130, "Reserved" },
	{ EM_TSK3000, "Altium TSK3000 core" },
	{ EM_RS08, "Freescale RS08 embedded processor" },
	{ EM_res133, "Reserved" },
	{ EM_ECOG2, "Cyan Technology eCOG2 microprocessor" },
	{ EM_SCORE, "Sunplus Score" },
	{ EM_SCORE7, "Sunplus S+core7 RISC processor" },
	{ EM_DSP24, "New Japan Radio (NJR) 24-bit DSP Processor" },
	{ EM_VIDEOCORE3, "Broadcom VideoCore III processor" },
	{ EM_LATTICEMICO32, "RISC processor for Lattice FPGA architecture" },
	{ EM_SE_C17, "Seiko Epson C17 family" },
	{ EM_TI_C6000, "Texas Instruments TMS320C6000 DSP family" },
	{ EM_TI_C2000, "Texas Instruments TMS320C2000 DSP family" },
	{ EM_TI_C5500, "Texas Instruments TMS320C55x DSP family" },
	{ EM_res143, "Reserved" },
	{ EM_res144, "Reserved" },
	{ EM_res145, "Reserved" },
	{ EM_res146, "Reserved" },
	{ EM_res147, "Reserved" },
	{ EM_res148, "Reserved" },
	{ EM_res149, "Reserved" },
	{ EM_res150, "Reserved" },
	{ EM_res151, "Reserved" },
	{ EM_res152, "Reserved" },
	{ EM_res153, "Reserved" },
	{ EM_res154, "Reserved" },
	{ EM_res155, "Reserved" },
	{ EM_res156, "Reserved" },
	{ EM_res157, "Reserved" },
	{ EM_res158, "Reserved" },
	{ EM_res159, "Reserved" },
	{ EM_MMDSP_PLUS, "STMicroelectronics 64bit VLIW Data Signal Processor" },
	{ EM_CYPRESS_M8C, "Cypress M8C microprocessor" },
	{ EM_R32C, "Renesas R32C series microprocessors" },
	{ EM_TRIMEDIA, "NXP Semiconductors TriMedia architecture family" },
	{ EM_QDSP6, "QUALCOMM DSP6 Processor" },
	{ EM_8051, "Intel 8051 and variants" },
	{ EM_STXP7X, "STMicroelectronics STxP7x family" },
	{ EM_NDS32,
	  "Andes Technology compact code size embedded RISC processor family" },
	{ EM_ECOG1, "Cyan Technology eCOG1X family" },
	{ EM_ECOG1X, "Cyan Technology eCOG1X family" },
	{ EM_MAXQ30, "Dallas Semiconductor MAXQ30 Core Micro-controllers" },
	{ EM_XIMO16, "New Japan Radio (NJR) 16-bit DSP Processor" },
	{ EM_MANIK, "M2000 Reconfigurable RISC Microprocessor" },
	{ EM_CRAYNV2, "Cray Inc. NV2 vector architecture" },
	{ EM_RX, "Renesas RX family" },
	{ EM_METAG, "Imagination Technologies META processor architecture" },
	{ EM_MCST_ELBRUS, "MCST Elbrus general purpose hardware architecture" },
	{ EM_ECOG16, "Cyan Technology eCOG16 family" },
	{ EM_CR16, "National Semiconductor CompactRISC 16-bit processor" },
	{ EM_ETPU, "Freescale Extended Time Processing Unit" },
	{ EM_SLE9X, "Infineon Technologies SLE9X core" },
	{ EM_L1OM, "Intel L1OM" },
	{ EM_INTEL181, "Reserved by Intel" },
	{ EM_INTEL182, "Reserved by Intel" },
	{ EM_res183, "Reserved by ARM" },
	{ EM_res184, "Reserved by ARM" },
	{ EM_AVR32, "Atmel Corporation 32-bit microprocessor family" },
	{ EM_STM8, "STMicroeletronics STM8 8-bit microcontroller" },
	{ EM_TILE64, "Tilera TILE64 multicore architecture family" },
	{ EM_TILEPRO, "Tilera TILEPro multicore architecture family" },
	{ EM_MICROBLAZE, "Xilinx MicroBlaze 32-bit RISC soft processor core" },
	{ EM_CUDA, "NVIDIA CUDA architecture " },
};

static struct section_type_table_t
{
	const Elf64_Half key;
	const char*      str;
} section_type_table[] = {
	{ SHT_NULL, "NULL" },
	{ SHT_PROGBITS, "PROGBITS" },
	{ SHT_SYMTAB, "SYMTAB" },
	{ SHT_STRTAB, "STRTAB" },
	{ SHT_RELA, "RELA" },
	{ SHT_HASH, "HASH" },
	{ SHT_DYNAMIC, "DYNAMIC" },
	{ SHT_NOTE, "NOTE" },
	{ SHT_NOBITS, "NOBITS" },
	{ SHT_REL, "REL" },
	{ SHT_SHLIB, "SHLIB" },
	{ SHT_DYNSYM, "DYNSYM" },
	{ SHT_INIT_ARRAY, "INIT_ARRAY" },
	{ SHT_FINI_ARRAY, "FINI_ARRAY" },
	{ SHT_PREINIT_ARRAY, "PREINIT_ARRAY" },
	{ SHT_GROUP, "GROUP" },
	{ SHT_SYMTAB_SHNDX, "SYMTAB_SHNDX " },
};

static struct segment_type_table_t
{
	const Elf_Word key;
	const char*    str;
} segment_type_table[] = {
	{ PT_NULL, "NULL" },     { PT_LOAD, "LOAD" }, { PT_DYNAMIC, "DYNAMIC" },
	{ PT_INTERP, "INTERP" }, { PT_NOTE, "NOTE" }, { PT_SHLIB, "SHLIB" },
	{ PT_PHDR, "PHDR" },     { PT_TLS, "TLS" },
};

static struct segment_flag_table_t
{
	const Elf_Word key;
	const char*    str;
} segment_flag_table[] = {
	{ 0, "" },  { 1, "X" },  { 2, "W" },  { 3, "WX" },
	{ 4, "R" }, { 5, "RX" }, { 6, "RW" }, { 7, "RWX" },
};

static struct symbol_bind_t
{
	const Elf_Word key;
	const char*    str;
} symbol_bind_table[] = {
	{ STB_LOCAL, "LOCAL" },   { STB_GLOBAL, "GLOBAL" },
	{ STB_WEAK, "WEAK" },     { STB_LOOS, "LOOS" },
	{ STB_HIOS, "HIOS" },     { STB_MULTIDEF, "MULTIDEF" },
	{ STB_LOPROC, "LOPROC" }, { STB_HIPROC, "HIPROC" },
};

static struct symbol_type_t
{
	const Elf_Word key;
	const char*    str;
} symbol_type_table[] = {
	{ STT_NOTYPE, "NOTYPE" }, { STT_OBJECT, "OBJECT" },
	{ STT_FUNC, "FUNC" },     { STT_SECTION, "SECTION" },
	{ STT_FILE, "FILE" },     { STT_COMMON, "COMMON" },
	{ STT_TLS, "TLS" },       { STT_LOOS, "LOOS" },
	{ STT_HIOS, "HIOS" },     { STT_LOPROC, "LOPROC" },
	{ STT_HIPROC, "HIPROC" },
};

static struct dynamic_tag_t
{
	const Elf_Word key;
	const char*    str;
} dynamic_tag_table[] = {
	{ DT_NULL, "NULL" },
	{ DT_NEEDED, "NEEDED" },
	{ DT_PLTRELSZ, "PLTRELSZ" },
	{ DT_PLTGOT, "PLTGOT" },
	{ DT_HASH, "HASH" },
	{ DT_STRTAB, "STRTAB" },
	{ DT_SYMTAB, "SYMTAB" },
	{ DT_RELA, "RELA" },
	{ DT_RELASZ, "RELASZ" },
	{ DT_RELAENT, "RELAENT" },
	{ DT_STRSZ, "STRSZ" },
	{ DT_SYMENT, "SYMENT" },
	{ DT_INIT, "INIT" },
	{ DT_FINI, "FINI" },
	{ DT_SONAME, "SONAME" },
	{ DT_RPATH, "RPATH" },
	{ DT_SYMBOLIC, "SYMBOLIC" },
	{ DT_REL, "REL" },
	{ DT_RELSZ, "RELSZ" },
	{ DT_RELENT, "RELENT" },
	{ DT_PLTREL, "PLTREL" },
	{ DT_DEBUG, "DEBUG" },
	{ DT_TEXTREL, "TEXTREL" },
	{ DT_JMPREL, "JMPREL" },
	{ DT_BIND_NOW, "BIND_NOW" },
	{ DT_INIT_ARRAY, "INIT_ARRAY" },
	{ DT_FINI_ARRAY, "FINI_ARRAY" },
	{ DT_INIT_ARRAYSZ, "INIT_ARRAYSZ" },
	{ DT_FINI_ARRAYSZ, "FINI_ARRAYSZ" },
	{ DT_RUNPATH, "RUNPATH" },
	{ DT_FLAGS, "FLAGS" },
	{ DT_ENCODING, "ENCODING" },
	{ DT_PREINIT_ARRAY, "PREINIT_ARRAY" },
	{ DT_PREINIT_ARRAYSZ, "PREINIT_ARRAYSZ" },
	{ DT_MAXPOSTAGS, "MAXPOSTAGS" },
};

static const ELFIO::Elf_Xword MAX_DATA_ENTRIES = 64;

//------------------------------------------------------------------------------
class dump
{
#define DUMP_DEC_FORMAT( width ) \
	std::setw( width ) << std::setfill( ' ' ) << std::dec << std::right
#define DUMP_HEX_FORMAT( width ) \
	std::setw( width ) << std::setfill( '0' ) << std::hex << std::right
#define DUMP_STR_FORMAT( width ) \
	std::setw( width ) << std::setfill( ' ' ) << std::hex << std::left

  public:
	//------------------------------------------------------------------------------
	static void header( std::ostream& out, const elfio& reader )
	{
		if ( !reader.get_header_size() ) {
			return;
		}
		out << "ELF Header" << std::endl
			<< std::endl
			<< "  Class:      " << str_class( reader.get_class() ) << std::endl
			<< "  Encoding:   " << str_endian( reader.get_encoding() )
			<< std::endl
			<< "  ELFVersion: " << str_version( reader.get_elf_version() )
			<< std::endl
			<< "  Type:       " << str_type( reader.get_type() ) << std::endl
			<< "  Machine:    " << str_machine( reader.get_machine() )
			<< std::endl
			<< "  Version:    " << str_version( reader.get_version() )
			<< std::endl
			<< "  Entry:      "
			<< "0x" << std::hex << reader.get_entry() << std::endl
			<< "  Flags:      "
			<< "0x" << std::hex << reader.get_flags() << std::endl
			<< std::endl;
	}

	//------------------------------------------------------------------------------
	static void section_headers( std::ostream& out, const elfio& reader )
	{
		Elf_Half n = reader.sections.size();

		if ( n == 0 ) {
			return;
		}

		out << "Section Headers:" << std::endl;
		if ( reader.get_class() == ELFCLASS32 ) { // Output for 32-bit
			out << "[  Nr ] Type              Addr     Size     ES Flg Lk Inf "
				   "Al Name"
				<< std::endl;
		}
		else { // Output for 64-bit
			out << "[  Nr ] Type              Addr             Size            "
				   " ES   Flg"
				<< std::endl
				<< "        Lk   Inf  Al      Name" << std::endl;
		}

		for ( Elf_Half i = 0; i < n; ++i ) { // For all sections
			section* sec = reader.sections[i];
			section_header( out, i, sec, reader.get_class() );
		}

		out << "Key to Flags: W (write), A (alloc), X (execute)\n\n"
			<< std::endl;
	}

	//------------------------------------------------------------------------------
	static void section_header( std::ostream&  out,
								Elf_Half       no,
								const section* sec,
								unsigned char  elf_class )
	{
		std::ios_base::fmtflags original_flags = out.flags();

		if ( elf_class == ELFCLASS32 ) { // Output for 32-bit
			out << "[" << DUMP_DEC_FORMAT( 5 ) << no << "] "
				<< DUMP_STR_FORMAT( 17 ) << str_section_type( sec->get_type() )
				<< " " << DUMP_HEX_FORMAT( 8 ) << sec->get_address() << " "
				<< DUMP_HEX_FORMAT( 8 ) << sec->get_size() << " "
				<< DUMP_HEX_FORMAT( 2 ) << sec->get_entry_size() << " "
				<< DUMP_STR_FORMAT( 3 ) << section_flags( sec->get_flags() )
				<< " " << DUMP_HEX_FORMAT( 2 ) << sec->get_link() << " "
				<< DUMP_HEX_FORMAT( 3 ) << sec->get_info() << " "
				<< DUMP_HEX_FORMAT( 2 ) << sec->get_addr_align() << " "
				<< DUMP_STR_FORMAT( 17 ) << sec->get_name() << " " << std::endl;
		}
		else { // Output for 64-bit
			out << "[" << DUMP_DEC_FORMAT( 5 ) << no << "] "
				<< DUMP_STR_FORMAT( 17 ) << str_section_type( sec->get_type() )
				<< " " << DUMP_HEX_FORMAT( 16 ) << sec->get_address() << " "
				<< DUMP_HEX_FORMAT( 16 ) << sec->get_size() << " "
				<< DUMP_HEX_FORMAT( 4 ) << sec->get_entry_size() << " "
				<< DUMP_STR_FORMAT( 3 ) << section_flags( sec->get_flags() )
				<< " " << std::endl
				<< "        " << DUMP_HEX_FORMAT( 4 ) << sec->get_link() << " "
				<< DUMP_HEX_FORMAT( 4 ) << sec->get_info() << " "
				<< DUMP_HEX_FORMAT( 4 ) << sec->get_addr_align() << "    "
				<< DUMP_STR_FORMAT( 17 ) << sec->get_name() << " " << std::endl;
		}

		out.flags( original_flags );

		return;
	}

	//------------------------------------------------------------------------------
	static void segment_headers( std::ostream& out, const elfio& reader )
	{
		Elf_Half n = reader.segments.size();
		if ( n == 0 ) {
			return;
		}

		out << "Segment headers:" << std::endl;
		if ( reader.get_class() == ELFCLASS32 ) { // Output for 32-bit
			out << "[  Nr ] Type           VirtAddr PhysAddr FileSize Mem.Size "
				   "Flags    Align"
				<< std::endl;
		}
		else { // Output for 64-bit
			out << "[  Nr ] Type           VirtAddr         PhysAddr         "
				   "Flags"
				<< std::endl
				<< "                       FileSize         Mem.Size         "
				   "Align"
				<< std::endl;
		}

		for ( Elf_Half i = 0; i < n; ++i ) {
			segment* seg = reader.segments[i];
			segment_header( out, i, seg, reader.get_class() );
		}

		out << std::endl;
	}

	//------------------------------------------------------------------------------
	static void segment_header( std::ostream&  out,
								Elf_Half       no,
								const segment* seg,
								unsigned int   elf_class )
	{
		std::ios_base::fmtflags original_flags = out.flags();

		if ( elf_class == ELFCLASS32 ) { // Output for 32-bit
			out << "[" << DUMP_DEC_FORMAT( 5 ) << no << "] "
				<< DUMP_STR_FORMAT( 14 ) << str_segment_type( seg->get_type() )
				<< " " << DUMP_HEX_FORMAT( 8 ) << seg->get_virtual_address()
				<< " " << DUMP_HEX_FORMAT( 8 ) << seg->get_physical_address()
				<< " " << DUMP_HEX_FORMAT( 8 ) << seg->get_file_size() << " "
				<< DUMP_HEX_FORMAT( 8 ) << seg->get_memory_size() << " "
				<< DUMP_STR_FORMAT( 8 ) << str_segment_flag( seg->get_flags() )
				<< " " << DUMP_HEX_FORMAT( 8 ) << seg->get_align() << " "
				<< std::endl;
		}
		else { // Output for 64-bit
			out << "[" << DUMP_DEC_FORMAT( 5 ) << no << "] "
				<< DUMP_STR_FORMAT( 14 ) << str_segment_type( seg->get_type() )
				<< " " << DUMP_HEX_FORMAT( 16 ) << seg->get_virtual_address()
				<< " " << DUMP_HEX_FORMAT( 16 ) << seg->get_physical_address()
				<< " " << DUMP_STR_FORMAT( 16 )
				<< str_segment_flag( seg->get_flags() ) << " " << std::endl
				<< "                       " << DUMP_HEX_FORMAT( 16 )
				<< seg->get_file_size() << " " << DUMP_HEX_FORMAT( 16 )
				<< seg->get_memory_size() << " " << DUMP_HEX_FORMAT( 16 )
				<< seg->get_align() << " " << std::endl;
		}

		out.flags( original_flags );
	}

	//------------------------------------------------------------------------------
	static void symbol_tables( std::ostream& out, const elfio& reader )
	{
		Elf_Half n = reader.sections.size();
		for ( Elf_Half i = 0; i < n; ++i ) { // For all sections
			section* sec = reader.sections[i];
			if ( SHT_SYMTAB == sec->get_type() ||
				 SHT_DYNSYM == sec->get_type() ) {
				symbol_section_accessor symbols( reader, sec );

				Elf_Xword sym_no = symbols.get_symbols_num();
				if ( sym_no > 0 ) {
					out << "Symbol table (" << sec->get_name() << ")"
						<< std::endl;
					if ( reader.get_class() ==
						 ELFCLASS32 ) { // Output for 32-bit
						out << "[  Nr ] Value    Size     Type    Bind      "
							   "Sect Name"
							<< std::endl;
					}
					else { // Output for 64-bit
						out << "[  Nr ] Value            Size             Type "
							   "   Bind      Sect"
							<< std::endl
							<< "        Name" << std::endl;
					}
					for ( Elf_Xword i = 0; i < sym_no; ++i ) {
						std::string   name;
						Elf64_Addr    value   = 0;
						Elf_Xword     size    = 0;
						unsigned char bind    = 0;
						unsigned char type    = 0;
						Elf_Half      section = 0;
						unsigned char other   = 0;
						symbols.get_symbol( i, name, value, size, bind, type,
											section, other );
						symbol_table( out, i, name, value, size, bind, type,
									  section, reader.get_class() );
					}

					out << std::endl;
				}
			}
		}
	}

	//------------------------------------------------------------------------------
	static void symbol_table( std::ostream& out,
							  Elf_Xword     no,
							  std::string&  name,
							  Elf64_Addr    value,
							  Elf_Xword     size,
							  unsigned char bind,
							  unsigned char type,
							  Elf_Half      section,
							  unsigned int  elf_class )
	{
		std::ios_base::fmtflags original_flags = out.flags();

		if ( elf_class == ELFCLASS32 ) { // Output for 32-bit
			out << "[" << DUMP_DEC_FORMAT( 5 ) << no << "] "
				<< DUMP_HEX_FORMAT( 8 ) << value << " " << DUMP_HEX_FORMAT( 8 )
				<< size << " " << DUMP_STR_FORMAT( 7 )
				<< str_symbol_type( type ) << " " << DUMP_STR_FORMAT( 8 )
				<< str_symbol_bind( bind ) << " " << DUMP_DEC_FORMAT( 5 )
				<< section << " " << DUMP_STR_FORMAT( 1 ) << name << " "
				<< std::endl;
		}
		else { // Output for 64-bit
			out << "[" << DUMP_DEC_FORMAT( 5 ) << no << "] "
				<< DUMP_HEX_FORMAT( 16 ) << value << " "
				<< DUMP_HEX_FORMAT( 16 ) << size << " " << DUMP_STR_FORMAT( 7 )
				<< str_symbol_type( type ) << " " << DUMP_STR_FORMAT( 8 )
				<< str_symbol_bind( bind ) << " " << DUMP_DEC_FORMAT( 5 )
				<< section << " " << std::endl
				<< "        " << DUMP_STR_FORMAT( 1 ) << name << " "
				<< std::endl;
		}

		out.flags( original_flags );
	}

	//------------------------------------------------------------------------------
	static void notes( std::ostream& out, const elfio& reader )
	{
		Elf_Half no = reader.sections.size();
		for ( Elf_Half i = 0; i < no; ++i ) { // For all sections
			section* sec = reader.sections[i];
			if ( SHT_NOTE == sec->get_type() ) { // Look at notes
				note_section_accessor notes( reader, sec );
				Elf_Word              no_notes = notes.get_notes_num();
				if ( no > 0 ) {
					out << "Note section (" << sec->get_name() << ")"
						<< std::endl
						<< "    No Type     Name" << std::endl;
					for ( Elf_Word j = 0; j < no_notes; ++j ) { // For all notes
						Elf_Word    type;
						std::string name;
						void*       desc;
						Elf_Word    descsz;

						if ( notes.get_note( j, type, name, desc, descsz ) ) {
							// 'name' usually contains \0 at the end. Try to fix it
							name = name.c_str();
							note( out, j, type, name );
						}
					}

					out << std::endl;
				}
			}
		}
	}

	//------------------------------------------------------------------------------
	static void modinfo( std::ostream& out, const elfio& reader )
	{
		Elf_Half no = reader.sections.size();
		for ( Elf_Half i = 0; i < no; ++i ) { // For all sections
			section* sec = reader.sections[i];
			if ( ".modinfo" == sec->get_name() ) { // Look for the section
				out << "Section .modinfo" << std::endl;

				const_modinfo_section_accessor modinfo( sec );
				for ( Elf_Word i = 0; i < modinfo.get_attribute_num(); i++ ) {
					std::string field;
					std::string value;
					if ( modinfo.get_attribute( i, field, value ) ) {
						out << "  " << std::setw( 20 ) << field
							<< std::setw( 0 ) <<  " = " << value << std::endl;
					}
				}

				out << std::endl;
				break;
			}
		}
	}

	//------------------------------------------------------------------------------
	static void
	note( std::ostream& out, int no, Elf_Word type, const std::string& name )
	{
		out << "  [" << DUMP_DEC_FORMAT( 2 ) << no << "] "
			<< DUMP_HEX_FORMAT( 8 ) << type << " " << DUMP_STR_FORMAT( 1 )
			<< name << std::endl;
	}

	//------------------------------------------------------------------------------
	static void dynamic_tags( std::ostream& out, const elfio& reader )
	{
		Elf_Half n = reader.sections.size();
		for ( Elf_Half i = 0; i < n; ++i ) { // For all sections
			section* sec = reader.sections[i];
			if ( SHT_DYNAMIC == sec->get_type() ) {
				dynamic_section_accessor dynamic( reader, sec );

				Elf_Xword dyn_no = dynamic.get_entries_num();
				if ( dyn_no > 0 ) {
					out << "Dynamic section (" << sec->get_name() << ")"
						<< std::endl;
					out << "[  Nr ] Tag              Name/Value" << std::endl;
					for ( Elf_Xword i = 0; i < dyn_no; ++i ) {
						Elf_Xword   tag   = 0;
						Elf_Xword   value = 0;
						std::string str;
						dynamic.get_entry( i, tag, value, str );
						dynamic_tag( out, i, tag, value, str,
									 reader.get_class() );
						if ( DT_NULL == tag ) {
							break;
						}
					}

					out << std::endl;
				}
			}
		}
	}

	//------------------------------------------------------------------------------
	static void dynamic_tag( std::ostream& out,
							 Elf_Xword     no,
							 Elf_Xword     tag,
							 Elf_Xword     value,
							 std::string   str,
							 unsigned int /*elf_class*/ )
	{
		out << "[" << DUMP_DEC_FORMAT( 5 ) << no << "] "
			<< DUMP_STR_FORMAT( 16 ) << str_dynamic_tag( tag ) << " ";
		if ( str.empty() ) {
			out << DUMP_HEX_FORMAT( 16 ) << value << " ";
		}
		else {
			out << DUMP_STR_FORMAT( 32 ) << str << " ";
		}
		out << std::endl;
	}

	//------------------------------------------------------------------------------
	static void section_data( std::ostream& out, const section* sec )
	{
		std::ios_base::fmtflags original_flags = out.flags();

		out << sec->get_name() << std::endl;
		const char* pdata = sec->get_data();
		if ( pdata ) {
			ELFIO::Elf_Xword i;
			for ( i = 0; i < std::min( sec->get_size(), MAX_DATA_ENTRIES );
				  ++i ) {
				if ( i % 16 == 0 ) {
					out << "[" << DUMP_HEX_FORMAT( 8 ) << i << "]";
				}

				out << " " << DUMP_HEX_FORMAT( 2 ) << ( pdata[i] & 0x000000FF );

				if ( i % 16 == 15 ) {
					out << std::endl;
				}
			}
			if ( i % 16 != 0 ) {
				out << std::endl;
			}

			out.flags( original_flags );
		}

		return;
	}

	//------------------------------------------------------------------------------
	static void section_datas( std::ostream& out, const elfio& reader )
	{
		Elf_Half n = reader.sections.size();

		if ( n == 0 ) {
			return;
		}

		out << "Section Data:" << std::endl;

		for ( Elf_Half i = 1; i < n; ++i ) { // For all sections
			section* sec = reader.sections[i];
			if ( sec->get_type() == SHT_NOBITS ) {
				continue;
			}
			section_data( out, sec );
		}

		out << std::endl;
	}

	//------------------------------------------------------------------------------
	static void
	segment_data( std::ostream& out, Elf_Half no, const segment* seg )
	{
		std::ios_base::fmtflags original_flags = out.flags();

		out << "Segment # " << no << std::endl;
		const char* pdata = seg->get_data();
		if ( pdata ) {
			ELFIO::Elf_Xword i;
			for ( i = 0; i < std::min( seg->get_file_size(), MAX_DATA_ENTRIES );
				  ++i ) {
				if ( i % 16 == 0 ) {
					out << "[" << DUMP_HEX_FORMAT( 8 ) << i << "]";
				}

				out << " " << DUMP_HEX_FORMAT( 2 ) << ( pdata[i] & 0x000000FF );

				if ( i % 16 == 15 ) {
					out << std::endl;
				}
			}
			if ( i % 16 != 0 ) {
				out << std::endl;
			}

			out.flags( original_flags );
		}

		return;
	}

	//------------------------------------------------------------------------------
	static void segment_datas( std::ostream& out, const elfio& reader )
	{
		Elf_Half n = reader.segments.size();

		if ( n == 0 ) {
			return;
		}

		out << "Segment Data:" << std::endl;

		for ( Elf_Half i = 0; i < n; ++i ) { // For all sections
			segment* seg = reader.segments[i];
			segment_data( out, i, seg );
		}

		out << std::endl;
	}

  private:
	//------------------------------------------------------------------------------
	template <typename T, typename K>
	std::string static find_value_in_table( const T& table, const K& key )
	{
		std::string res = "?";
		for ( unsigned int i = 0; i < sizeof( table ) / sizeof( table[0] );
			  ++i ) {
			if ( table[i].key == key ) {
				res = table[i].str;
				break;
			}
		}

		return res;
	}

	//------------------------------------------------------------------------------
	template <typename T, typename K>
	static std::string format_assoc( const T& table, const K& key )
	{
		std::string str = find_value_in_table( table, key );
		if ( str == "?" ) {
			std::ostringstream oss;
			oss << str << " (0x" << std::hex << key << ")";
			str = oss.str();
		}

		return str;
	}

	//------------------------------------------------------------------------------
	template <typename T>
	static std::string format_assoc( const T& table, const char key )
	{
		return format_assoc( table, (const int)key );
	}

	//------------------------------------------------------------------------------
	static std::string section_flags( Elf_Xword flags )
	{
		std::string ret = "";
		if ( flags & SHF_WRITE ) {
			ret += "W";
		}
		if ( flags & SHF_ALLOC ) {
			ret += "A";
		}
		if ( flags & SHF_EXECINSTR ) {
			ret += "X";
		}

		return ret;
	}

//------------------------------------------------------------------------------
#define STR_FUNC_TABLE( name )                                         \
	template <typename T> static std::string str_##name( const T key ) \
	{                                                                  \
		return format_assoc( name##_table, key );                      \
	}

	STR_FUNC_TABLE( class )
	STR_FUNC_TABLE( endian )
	STR_FUNC_TABLE( version )
	STR_FUNC_TABLE( type )
	STR_FUNC_TABLE( machine )
	STR_FUNC_TABLE( section_type )
	STR_FUNC_TABLE( segment_type )
	STR_FUNC_TABLE( segment_flag )
	STR_FUNC_TABLE( symbol_bind )
	STR_FUNC_TABLE( symbol_type )
	STR_FUNC_TABLE( dynamic_tag )

#undef STR_FUNC_TABLE
#undef DUMP_DEC_FORMAT
#undef DUMP_HEX_FORMAT
#undef DUMP_STR_FORMAT
}; // class dump

}; // namespace ELFIO

#endif // ELFIO_DUMP_HPP

/*** End of inlined file: elfio_dump.hpp ***/

















#ifndef __ADDR_ANY_H__
#define __ADDR_ANY_H__


//linux
#include <regex.h>
#include <cxxabi.h>
//c
#include <cinttypes>
#include <cstdio>
#include <cstdlib>

//c++
#include <string>
#include <map>




class AddrAny
{
public:
    AddrAny()
    {
        m_init = get_exe_pathname(m_fullname);
        m_baseaddr = 0;
    }
    AddrAny(std::string libname)
    {
        m_init = get_lib_pathname_and_baseaddr(libname, m_fullname, m_baseaddr);
    }

    int get_local_func_addr_symtab(std::string func_name_regex_str, std::map<std::string,void*>& result)
    {
        return get_func_addr(SHT_SYMTAB, STB_LOCAL, func_name_regex_str, result);
    }
    int get_global_func_addr_symtab(std::string func_name_regex_str, std::map<std::string,void*>& result)
    {
        return get_func_addr(SHT_SYMTAB, STB_GLOBAL, func_name_regex_str, result);
    }
    int get_weak_func_addr_symtab(std::string func_name_regex_str, std::map<std::string,void*>& result)
    {
        return get_func_addr(SHT_SYMTAB, STB_WEAK, func_name_regex_str, result);
    }
    
    int get_global_func_addr_dynsym( std::string func_name_regex_str, std::map<std::string,void*>& result)
    {
        return get_func_addr(SHT_DYNSYM, STB_GLOBAL, func_name_regex_str, result);
    }
    int get_weak_func_addr_dynsym(std::string func_name_regex_str, std::map<std::string,void*>& result)
    {
        return get_func_addr(SHT_DYNSYM, STB_WEAK, func_name_regex_str, result);
    }
    
private:
    bool demangle(std::string& s, std::string& name) {
        int status;
        char* pname = abi::__cxa_demangle(s.c_str(), 0, 0, &status);
        if (status != 0)
        {
            switch(status)
            {
                case -1: name = "memory allocation error"; break;
                case -2: name = "invalid name given"; break;
                case -3: name = "internal error: __cxa_demangle: invalid argument"; break;
                default: name = "unknown error occured"; break;
            }
            return false;
        }
        name = pname;
        free(pname);
        return true;
    }
    bool get_exe_pathname( std::string& name)
    {
        char                     line[512];
        FILE                    *fp;
        uintptr_t                base_addr;
        char                     perm[5];
        unsigned long            offset;
        int                      pathname_pos;
        char                    *pathname;
        size_t                   pathname_len;
        int                      match = 0;
        
        if(NULL == (fp = fopen("/proc/self/maps", "r")))
        {
            return false;
        }

        while(fgets(line, sizeof(line), fp))
        {
            if(sscanf(line, "%" PRIxPTR "-%*lx %4s %lx %*x:%*x %*d%n", &base_addr, perm, &offset, &pathname_pos) != 3) continue;

            if(0 != offset) continue;

            //get pathname
            while(isspace(line[pathname_pos]) && pathname_pos < (int)(sizeof(line) - 1))
                pathname_pos += 1;
            if(pathname_pos >= (int)(sizeof(line) - 1)) continue;
            pathname = line + pathname_pos;
            pathname_len = strlen(pathname);
            if(0 == pathname_len) continue;
            if(pathname[pathname_len - 1] == '\n')
            {
                pathname[pathname_len - 1] = '\0';
                pathname_len -= 1;
            }
            if(0 == pathname_len) continue;
            if('[' == pathname[0]) continue;

            name = pathname;
            match = 1;
            break;

        }
        fclose(fp);

        if(0 == match)
        {
            return false;
        }
        else
        {
            return true;
        }

    }

    bool get_lib_pathname_and_baseaddr(std::string pathname_regex_str, std::string& name, unsigned long& addr)
    {
        char                     line[512];
        FILE                    *fp;
        uintptr_t                base_addr;
        char                     perm[5];
        unsigned long            offset;
        int                      pathname_pos;
        char                    *pathname;
        size_t                   pathname_len;
        int                      match;
        regex_t   pathname_regex;

        regcomp(&pathname_regex, pathname_regex_str.c_str(), 0);

        if(NULL == (fp = fopen("/proc/self/maps", "r")))
        {
            return false;
        }

        while(fgets(line, sizeof(line), fp))
        {
            if(sscanf(line, "%" PRIxPTR "-%*lx %4s %lx %*x:%*x %*d%n", &base_addr, perm, &offset, &pathname_pos) != 3) continue;

            //check permission
            if(perm[0] != 'r') continue;
            if(perm[3] != 'p') continue; //do not touch the shared memory

            //check offset
            //
            //We are trying to find ELF header in memory.
            //It can only be found at the beginning of a mapped memory regions
            //whose offset is 0.
            if(0 != offset) continue;

            //get pathname
            while(isspace(line[pathname_pos]) && pathname_pos < (int)(sizeof(line) - 1))
                pathname_pos += 1;
            if(pathname_pos >= (int)(sizeof(line) - 1)) continue;
            pathname = line + pathname_pos;
            pathname_len = strlen(pathname);
            if(0 == pathname_len) continue;
            if(pathname[pathname_len - 1] == '\n')
            {
                pathname[pathname_len - 1] = '\0';
                pathname_len -= 1;
            }
            if(0 == pathname_len) continue;
            if('[' == pathname[0]) continue;

            //check pathname
            //if we need to hook this elf?
            match = 0;
            if(0 == regexec(&pathname_regex, pathname, 0, NULL, 0))
            {
                match = 1;
                name = pathname;
                addr = (unsigned long)base_addr;
                break;
            }
            if(0 == match) continue;

        }
        fclose(fp);
        if(0 == match)
        {
            return false;
        }
        else
        {
            return true;
        }

    }

    int get_func_addr(unsigned int ttype, unsigned int stype, std::string& func_name_regex_str, std::map<std::string,void*>& result)
    {
        // Create an elfio reader
        ELFIO::elfio reader;
        int count = 0;
        regex_t   pathname_regex;

        if(!m_init)
        {
            return -1;
        }

        regcomp(&pathname_regex, func_name_regex_str.c_str(), 0);
        // Load ELF data
        if(!reader.load(m_fullname.c_str()))
        {
            return -1;
        }
        
        ELFIO::Elf_Half sec_num = reader.sections.size();
        for(int i = 0; i < sec_num; ++i)
        {
            ELFIO::section* psec = reader.sections[i];
            // Check section type
            if(psec->get_type() == ttype)
            {
                const ELFIO::symbol_section_accessor symbols( reader, psec );
                for ( unsigned int j = 0; j < symbols.get_symbols_num(); ++j )
                {
                    std::string name;
                    std::string name_mangle;
                    ELFIO::Elf64_Addr value;
                    ELFIO::Elf_Xword size;
                    unsigned char bind;
                    unsigned char type;
                    ELFIO::Elf_Half section_index;
                    unsigned char other;
                    
                    // Read symbol properties
                    symbols.get_symbol( j, name, value, size, bind, type, section_index, other );
                    if(type == STT_FUNC && bind == stype)
                    {
                        bool ret = demangle(name,name_mangle);
                        if(ret == true)
                        {
                            if (0 == regexec(&pathname_regex, name_mangle.c_str(), 0, NULL, 0))
                            {
                                  result.insert ( std::pair<std::string,void *>(name_mangle,(void*)(value + m_baseaddr)));
                                  count++;
                            }
                        }
                        else
                        {
                            if (0 == regexec(&pathname_regex, name.c_str(), 0, NULL, 0))
                            {
                                  result.insert ( std::pair<std::string,void *>(name,(void*)(value + m_baseaddr)));
                                  count++;
                            }
                        }
                    }
                }
                break;
            }
        }
        
        return count;
    }
private:
    bool m_init;
    std::string m_name;
    std::string m_fullname;
    unsigned long m_baseaddr;

};
#endif
