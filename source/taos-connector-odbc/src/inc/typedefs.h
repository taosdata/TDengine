/*
 * MIT License
 *
 * Copyright (c) 2022-2023 freemine <freemine@yeah.net>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#ifndef _typedefs_h_
#define _typedefs_h_

typedef char charset_name_t[64];
typedef struct charset_conv_s           charset_conv_t;
typedef struct charset_conv_mgr_s       charset_conv_mgr_t;
typedef struct charset_convs_s          charset_convs_t;

typedef struct col_bind_map_s           col_bind_map_t;

typedef struct columns_args_s           columns_args_t;
typedef struct columns_s                columns_t;

typedef enum custprod_type_e            custprod_type_e;

typedef struct custprod_item_s          custprod_item_t;

typedef struct conn_cfg_s               conn_cfg_t;

typedef struct conn_parser_param_s      conn_parser_param_t;
typedef struct conn_s                   conn_t;

typedef struct descriptor_s             descriptor_t;
typedef struct desc_s                   desc_t;
typedef struct desc_header_s            desc_header_t;
typedef struct desc_record_s            desc_record_t;

typedef struct ds_conn_base_s           ds_conn_base_t;
typedef struct ds_tsdb_conn_s           ds_tsdb_conn_t;
typedef struct ds_ws_conn_s             ds_ws_conn_t;

typedef struct ds_err_s                 ds_err_t;
typedef struct ds_conn_s                ds_conn_t;
typedef struct ds_res_s                 ds_res_t;
typedef struct ds_fields_s              ds_fields_t;
typedef struct ds_block_s               ds_block_t;
typedef struct ds_stmt_s                ds_stmt_t;

typedef struct env_s                    env_t;

typedef struct err_s                    err_t;
typedef struct errs_s                   errs_t;

typedef struct ext_parser_param_s       ext_parser_param_t;

typedef struct get_data_ctx_s           get_data_ctx_t;

typedef struct insert_eval_s            insert_eval_t;

typedef struct param_bind_map_s         param_bind_map_t;

typedef struct param_bind_meta_s        param_bind_meta_t;
typedef struct params_bind_meta_s       params_bind_meta_t;

typedef struct param_state_s            param_state_t;

typedef struct primarykeys_args_s       primarykeys_args_t;
typedef struct primarykeys_s            primarykeys_t;

typedef struct stmt_s                   stmt_t;
typedef struct stmt_get_data_args_s     stmt_get_data_args_t;

typedef struct stmt_base_s              stmt_base_t;

typedef struct sqlc_tsdb_s              sqlc_tsdb_t;
typedef struct sqlc_data_s              sqlc_data_t;
typedef struct sql_data_s               sql_data_t;

typedef struct sqlc_sql_map_s           sqlc_sql_map_t;

typedef struct sqls_s                   sqls_t;
typedef struct sqls_parser_nterm_s      sqls_parser_nterm_t;
typedef struct sqls_parser_param_s      sqls_parser_param_t;


typedef struct tables_args_s            tables_args_t;
typedef struct tables_s                 tables_t;

typedef struct tls_s                    tls_t;

typedef enum tables_type_e              tables_type_e;

typedef struct topic_s                  topic_t;
typedef struct topic_cfg_s              topic_cfg_t;

typedef struct tsdb_stmt_s              tsdb_stmt_t;
typedef struct tsdb_params_s            tsdb_params_t;
typedef struct tsdb_binds_s             tsdb_binds_t;
typedef struct tsdb_res_s               tsdb_res_t;
typedef struct tsdb_fields_s            tsdb_fields_t;
typedef struct tsdb_rows_block_s        tsdb_rows_block_t;

typedef struct ts_parser_param_s        ts_parser_param_t;

typedef struct typesinfo_s              typesinfo_t;

typedef struct url_s                    url_t;
typedef struct url_parser_param_s       url_parser_param_t;

typedef enum   var_e                    var_e;
typedef enum   var_quote_e              var_quote_e;
typedef struct var_s                    var_t;

#endif // _typedefs_h_

