import { requestOffical } from "@/utils/request.js";

export function sendEmail(data) {
  return requestOffical({
    url: "/assets/globalscripts/email.php",
    method: "post",
    data: data,
  });
}
