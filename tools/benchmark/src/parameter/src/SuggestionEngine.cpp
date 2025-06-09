
class SuggestionEngine {
  static constexpr size_t MAX_EDIT_DISTANCE = 2;
  
public:
  static std::vector<std::string> suggest_corrections(
      const std::string& input,
      const std::map<std::string, ParamDescriptor>& descriptors)
  {
      std::vector<std::string> candidates;
      for (const auto& [name, _] : descriptors) {
          if (edit_distance(input, name) <= MAX_EDIT_DISTANCE) {
              candidates.push_back(name);
          }
      }
      return candidates;
  }
};
