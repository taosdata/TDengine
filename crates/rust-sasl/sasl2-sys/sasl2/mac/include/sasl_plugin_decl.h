/*
 * i guess the unix computer isnt picky about undeclared functions
 * should build with gcc with warn all
 */
#if defined(macintosh) && (!defined(SASL_MONOLITHIC))
#pragma export on
#define SASL_TURN_OFF_PLUGIN_EXPORT
#endif
sasl_server_plug_init_t sasl_server_plug_init;
sasl_client_plug_init_t sasl_client_plug_init;
#ifdef SASL_TURN_OFF_PLUGIN_EXPORT
#pragma export reset
#undef SASL_TURN_OFF_PLUGIN_EXPORT
#endif

#ifdef rubbish
int sasl_server_plug_init(sasl_utils_t *utils __attribute__((unused)),
			  int maxversion,
			  int *out_version,
			  const sasl_server_plug_t **pluglist,
			  int *plugcount);

int sasl_client_plug_init(sasl_utils_t *utils __attribute__((unused)),
			  int maxversion,
			  int *out_version,
			  const sasl_client_plug_t **pluglist,
			  int *plugcount);
#endif