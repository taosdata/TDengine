import { IsAliyun } from "@/const";

let user = [
  {
    label: "firstName",
    prop: "firstName",
    width: "150px",
  },
  {
    label: "lastName",
    prop: "lastName",
    width: "150px",
  },
];
if (IsAliyun) {
  user.reverse();
}
user.push({
  label: "email",
  prop: "email",
  "min-width": "100px",
});

export default {
  user,
  group: [
    {
      label: "accessControl.groupName",
      prop: "group_name",
    },
    {
      label: "accessControl.userNum",
      prop: "num",
    },
    {
      label: "status",
      prop: "status",
    },
    {
      label: "createTime",
      prop: "create_time",
    },
  ],
};
