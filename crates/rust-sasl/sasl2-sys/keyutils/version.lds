KEYUTILS_0.3 {

	/* primary syscalls; may be overridden by glibc */
	add_key;
	request_key;
	keyctl;

	/* management functions */
	keyctl_chown;
	keyctl_clear;
	keyctl_describe;
	keyctl_describe_alloc;
	keyctl_get_keyring_ID;
	keyctl_instantiate;
	keyctl_join_session_keyring;
	keyctl_link;
	keyctl_negate;
	keyctl_read;
	keyctl_read_alloc;
	keyctl_revoke;
	keyctl_search;
	keyctl_setperm;
	keyctl_set_reqkey_keyring;
	keyctl_unlink;
	keyctl_update;

};

KEYUTILS_1.0 {
	/* management functions */
	keyctl_assume_authority;
	keyctl_set_timeout;

} KEYUTILS_0.3;

KEYUTILS_1.3 {
	/* management functions */
	keyctl_get_security;
	keyctl_get_security_alloc;
	keyctl_session_to_parent;

} KEYUTILS_1.0;

KEYUTILS_1.4 {
	/* management functions */
	keyctl_reject;
	keyctl_instantiate_iov;
	keyctl_invalidate;

	/* utility functions */
	recursive_key_scan;
	recursive_session_key_scan;

} KEYUTILS_1.3;

KEYUTILS_1.5 {
	/* management functions */
	keyctl_get_persistent;

	/* utility functions */
	find_key_by_type_and_desc;

} KEYUTILS_1.4;

KEYUTILS_1.6 {
	/* management functions */
	keyctl_dh_compute;
	keyctl_dh_compute_alloc;
	keyctl_pkey_query;
	keyctl_pkey_encrypt;
	keyctl_pkey_decrypt;
	keyctl_pkey_sign;
	keyctl_pkey_verify;

} KEYUTILS_1.5;

KEYUTILS_1.7 {
	/* management functions */
	keyctl_restrict;
	keyctl_dh_compute_kdf;
	keyctl_dh_compute_kdf_alloc;

} KEYUTILS_1.6;

KEYUTILS_1.8 {
	/* Public key cryptography functions */
	keyctl_pkey_query;
	keyctl_pkey_encrypt;
	keyctl_pkey_decrypt;
	keyctl_pkey_sign;
	keyctl_pkey_verify;

} KEYUTILS_1.7;

KEYUTILS_1.9 {
	/* Management functions */
	keyctl_move;
	keyctl_capabilities;

} KEYUTILS_1.8;

KEYUTILS_1.10 {
	/* Management functions */
	keyctl_watch_key;

} KEYUTILS_1.9;
