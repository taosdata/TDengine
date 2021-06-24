//make header C++/C inter-operable
#ifdef __cplusplus
extern "C" {
#endif

#ifndef SZ_OPENCL_H
#define SZ_OPENCL_H

#include<stddef.h>

	//opaque pointer for opencl state
  struct sz_opencl_state;

  /**
   * creates an opencl state for multiple uses of the compressor or
   * returns an error code.
   *
   * \post if return code is SZ_NCES, the state object may only be passed to
   * sz_opencl_release or sz_opencl_error_* otherwise it may be used in any
   * sz_opencl_* function.
   *
   * \param[out] state the sz opencl state
   * \return SZ_SCES for success or SZ_NCES on error
   */
  int sz_opencl_init(struct sz_opencl_state** state);

	/**
	 * deinitializes an opencl state
	 *
	 * \param[in] state the sz opencl state
	 * \return SZ_SCES
	 */
  int sz_opencl_release(struct sz_opencl_state** state);

	/**
	 * returns a human readable error message for the last error recieved by state
	 *
	 * \param[in] state the sz opencl state
	 * \return a pointer to a string that describes the error
	 */
	const char* sz_opencl_error_msg(struct sz_opencl_state* state);


	/**
	 * returns a numeric code for the last error recieved by state
	 *
	 * \param[in] state the sz opencl state
	 * \return the numeric error code
	 */
  int sz_opencl_error_code(struct sz_opencl_state* state);

	/**
	 * confirms that the sz opencl state is ready to use by performing a vector addition
	 *
	 * \param[in] state the sz opencl state
	 * \return SZ_SCES if the opencl implementation is functioning
	 */
	int sz_opencl_check(struct sz_opencl_state*);

  unsigned char* sz_compress_float3d_opencl(float* data, size_t r1, size_t r2, size_t r3, double, size_t* out_size);


#endif /* SZ_OPENCL_H */

//make header C++/C inter-operable
#ifdef __cplusplus
}
#endif
