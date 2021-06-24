!  @file   sdc_interface.F90
!  @author Sheng Di (disheng222@gmail.com)
!  @date   Aug., 2014
!  @ Mathematics and Computer Science (MCS)
!  @ Argonne National Laboratory, Lemont, USA.
!  @brief  The key Fortran binding file to connect C language and Fortran (Fortran part)


MODULE RW
	use :: ISO_C_BINDING

	INTERFACE writeData
		MODULE PROCEDURE WriteData_inBinary_d1_INTEGER_K1
		MODULE PROCEDURE WriteData_inBinary_d1_REAL_K4
		MODULE PROCEDURE WriteData_inBinary_d2_REAL_K4
		MODULE PROCEDURE WriteData_inBinary_d3_REAL_K4
		MODULE PROCEDURE WriteData_inBinary_d4_REAL_K4
		MODULE PROCEDURE WriteData_inBinary_d5_REAL_K4
		MODULE PROCEDURE WriteData_inBinary_d1_REAL_K8
		MODULE PROCEDURE WriteData_inBinary_d2_REAL_K8
		MODULE PROCEDURE WriteData_inBinary_d3_REAL_K8
		MODULE PROCEDURE WriteData_inBinary_d4_REAL_K8
		MODULE PROCEDURE WriteData_inBinary_d5_REAL_K8
	END INTERFACE writeData

	INTERFACE readData
		MODULE PROCEDURE readByteData
		MODULE PROCEDURE readFloatData
		MODULE PROCEDURE readDoubleData
	END INTERFACE readData

	CONTAINS

	!Bytes here could be an "allocatable" array, so it requires an extra "byteLength" io indicate the length (can't use size(Bytes))
	SUBROUTINE WriteData_inBinary_d1_INTEGER_K1(Bytes, byteLength, FILE_PATH)
		implicit none
		INTEGER(KIND=1), DIMENSION(:) :: Bytes
		CHARACTER(LEN=*) :: FILE_PATH
		INTEGER(KIND=C_SIZE_T) :: byteLength

		CALL writeByteFile(Bytes, byteLength, FILE_PATH, len(trim(FILE_PATH)))
	END SUBROUTINE WriteData_inBinary_d1_INTEGER_K1

	SUBROUTINE WriteData_inBinary_d1_REAL_K4(VAR, nbEle, FILE_PATH)
		implicit none
		REAL(KIND=4), DIMENSION(:) :: VAR
		CHARACTER(LEN=*) :: FILE_PATH
		INTEGER :: nbEle

		CALL writeFloatFile(VAR, nbEle, FILE_PATH, len(trim(FILE_PATH)))
	END SUBROUTINE WriteData_inBinary_d1_REAL_K4

	SUBROUTINE WriteData_inBinary_d2_REAL_K4(VAR, nbEle, FILE_PATH)
		implicit none
		REAL(KIND=4), DIMENSION(:,:) :: VAR
		CHARACTER(LEN=*) :: FILE_PATH
		INTEGER :: nbEle

		CALL writeFloatFile(RESHAPE(VAR,(/nbEle/)), nbEle, FILE_PATH, len(trim(FILE_PATH)))
	END SUBROUTINE WriteData_inBinary_d2_REAL_K4

	SUBROUTINE WriteData_inBinary_d3_REAL_K4(VAR, nbEle, FILE_PATH)
		implicit none
		REAL(KIND=4), DIMENSION(:,:,:) :: VAR
		CHARACTER(LEN=*) :: FILE_PATH
		INTEGER :: nbEle

		CALL writeFloatFile(RESHAPE(VAR,(/nbEle/)), nbEle, FILE_PATH, len(trim(FILE_PATH)))
	END SUBROUTINE WriteData_inBinary_d3_REAL_K4

	SUBROUTINE WriteData_inBinary_d4_REAL_K4(VAR, nbEle, FILE_PATH)
		implicit none
		REAL(KIND=4), DIMENSION(:,:,:,:) :: VAR
		CHARACTER(LEN=*) :: FILE_PATH
		INTEGER :: nbEle

		CALL writeFloatFile(RESHAPE(VAR,(/nbEle/)), nbEle, FILE_PATH, len(trim(FILE_PATH)))
	END SUBROUTINE WriteData_inBinary_d4_REAL_K4

	SUBROUTINE WriteData_inBinary_d5_REAL_K4(VAR, nbEle, FILE_PATH)
		implicit none
		REAL(KIND=4), DIMENSION(:,:,:,:,:) :: VAR
		CHARACTER(LEN=*) :: FILE_PATH
		INTEGER :: nbEle

		CALL writeFloatFile(RESHAPE(VAR,(/nbEle/)), nbEle, FILE_PATH, len(trim(FILE_PATH)))
	END SUBROUTINE WriteData_inBinary_d5_REAL_K4

!write data in binary for K8 data

	SUBROUTINE WriteData_inBinary_d1_REAL_K8(VAR, nbEle, FILE_PATH)
		implicit none
		REAL(KIND=8), DIMENSION(:) :: VAR
		CHARACTER(LEN=*) :: FILE_PATH
		INTEGER :: nbEle

		CALL writeDoubleFile(VAR, nbEle, FILE_PATH, len(trim(FILE_PATH)))
	END SUBROUTINE WriteData_inBinary_d1_REAL_K8

	SUBROUTINE WriteData_inBinary_d2_REAL_K8(VAR, nbEle, FILE_PATH)
		implicit none
		REAL(KIND=8), DIMENSION(:,:) :: VAR
		CHARACTER(LEN=*) :: FILE_PATH
		INTEGER :: nbEle

		CALL writeDoubleFile(RESHAPE(VAR,(/nbEle/)), nbEle, FILE_PATH, len(trim(FILE_PATH)))
	END SUBROUTINE WriteData_inBinary_d2_REAL_K8

	SUBROUTINE WriteData_inBinary_d3_REAL_K8(VAR, FILE_PATH)
		implicit none
		REAL(KIND=8), DIMENSION(:,:,:) :: VAR
		CHARACTER(LEN=*) :: FILE_PATH
		INTEGER :: nbEle

		CALL writeDoubleFile(RESHAPE(VAR,(/nbEle/)), nbEle, FILE_PATH, len(trim(FILE_PATH)))
	END SUBROUTINE WriteData_inBinary_d3_REAL_K8

	SUBROUTINE WriteData_inBinary_d4_REAL_K8(VAR, nbEle, FILE_PATH)
		implicit none
		REAL(KIND=8), DIMENSION(:,:,:,:) :: VAR
		CHARACTER(LEN=*) :: FILE_PATH
		INTEGER :: nbEle

		CALL writeDoubleFile(RESHAPE(VAR,(/nbEle/)), nbEle, FILE_PATH, len(trim(FILE_PATH)))
	END SUBROUTINE WriteData_inBinary_d4_REAL_K8

	SUBROUTINE WriteData_inBinary_d5_REAL_K8(VAR, nbEle, FILE_PATH)
		implicit none
		REAL(KIND=8), DIMENSION(:,:,:,:,:) :: VAR
		CHARACTER(LEN=*) :: FILE_PATH
		INTEGER :: nbEle

		CALL writeDoubleFile(RESHAPE(VAR,(/nbEle/)), nbEle, FILE_PATH, len(trim(FILE_PATH)))
	END SUBROUTINE WriteData_inBinary_d5_REAL_K8

!Check file size
	SUBROUTINE checkFileSize(FILE_PATH, BYTESIZE)
		implicit none
		CHARACTER(LEN=*) :: FILE_PATH
		INTEGER(kind=C_SIZE_T) :: BYTESIZE

		CALL checkFileSizeC(FILE_PATH, len(trim(FILE_PATH)), BYTESIZE)
	END SUBROUTINE checkFileSize

!Read data
	SUBROUTINE readByteData(FILE_PATH, Bytes, outSize)
		implicit none
		INTEGER(KIND=1), DIMENSION(:), allocatable :: temp
		INTEGER(KIND=1), DIMENSION(:), allocatable :: Bytes
		CHARACTER(LEN=*) :: FILE_PATH
		INTEGER(kind=C_SIZE_T) :: COUNTER
		INTEGER(kind=C_SIZE_T), intent(out) :: outSize !in bytes
		
		CALL checkFileSize(FILE_PATH, outSize)
		allocate(temp(outSize))

		CALL readByteFile(FILE_PATH, len(trim(FILE_PATH)), temp, outSize)
		allocate(Bytes(outSize))
		DO COUNTER=1,outSize,1
			Bytes(COUNTER) = temp(COUNTER)
		END DO
		deallocate(temp)
	END SUBROUTINE readByteData

	SUBROUTINE readFloatData(FILE_PATH, VAR, nbEle)
		implicit none
		REAL(KIND=4), DIMENSION(:), allocatable :: temp
		REAL(KIND=4), DIMENSION(:), allocatable :: VAR
		CHARACTER(LEN=*) :: FILE_PATH
		INTEGER(kind=C_SIZE_T) :: COUNTER, fileSize
		INTEGER(kind=C_SIZE_T), intent(out) :: nbEle

		CALL checkFileSize(FILE_PATH, fileSize)
		nbEle = fileSize/4
		allocate(temp(nbEle))
		
		CALL readFloatFile(FILE_PATH, len(trim(FILE_PATH)), temp, nbEle)
		allocate(VAR(nbEle))
		DO COUNTER=1,fileSize,1
			VAR(COUNTER) = temp(COUNTER)
		END DO		
		deallocate(temp)
	END SUBROUTINE readFloatData

	SUBROUTINE readDoubleData(FILE_PATH, VAR, nbEle)
		implicit none
		REAL(KIND=8), DIMENSION(:), allocatable :: temp
		REAL(KIND=8), DIMENSION(:), allocatable :: VAR
		CHARACTER(LEN=*) :: FILE_PATH
		INTEGER(kind=C_SIZE_T) :: COUNTER, fileSize
		INTEGER(kind=C_SIZE_T), intent(out) :: nbEle

		CALL checkFileSize(FILE_PATH, fileSize)
		nbEle = fileSize/8
		allocate(temp(nbEle))
	
		CALL readDoubleFile(FILE_PATH, len(trim(FILE_PATH)), temp, nbEle)
		allocate(VAR(nbEle))
		DO COUNTER=1,fileSize,1
			VAR(COUNTER) = temp(COUNTER)
		END DO		
		deallocate(temp)		
	END SUBROUTINE readDoubleData

END MODULE RW
