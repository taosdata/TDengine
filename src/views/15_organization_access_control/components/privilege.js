import store from "@/store";
const typeAliasMap = {
  Organization: "org",
  Instance: "instance",
  All: "common",
  Database: "db",
};

const aliasMap = {
  All: "Grant Privilege",
};

export function getPrivilegeTypeMap() {
  const typeMap = {};
  store.state.privilegeList.forEach(item => {
    const key = aliasMap[item.resourceType] || item.resourceType;
    if (key == "Topic") return;
    if (!typeMap[key]) {
      typeMap[key] = [];
    }
    typeMap[key].push({
      id: item.id,
      label: item.desc || item.name,
      type: typeAliasMap[item.resourceType],
    });
  });
  return typeMap;
}
