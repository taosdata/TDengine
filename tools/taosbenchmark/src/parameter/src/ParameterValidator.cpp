class ParameterValidator {
  public:
      // 未识别参数检测
      static void check_unknown_params(
          const std::set<std::string>& input_params,
          const std::map<std::string, ParamDescriptor>& descriptors);
      
      // 条件依赖校验
      static void check_conditional_params(
          const ParameterContext& context,
          const std::string& trigger_param,
          const std::set<std::string>& required_params);
      
      // 类型兼容性检查
      static bool check_type_compatibility(
          const std::string& param_name,
          const ParamValue& actual_value,
          const ParamDescriptor& descriptor);
};

// void validate_filetype_dependent(const ParameterContext& ctx) {
//   if (ctx.get<std::string>("filetype") == "csvfile") {
//       if (!ctx.has("super_tables.csv_ts_format")) {
//           throw ConfigError("csv_ts_format required when filetype=csvfile");
//       }
//   }
// }

