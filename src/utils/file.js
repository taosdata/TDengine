//Download the file from the Blob response of Axio XHR
export function downloadFileBlob(blob, filePath) {
  if (!blob) {
    return;
  }
  const objUrl = window.URL.createObjectURL(new Blob([blob]));
  const fileLinkEle = document.createElement('a');
  fileLinkEle.href = objUrl;
  let fileName = 'temp.txt';
  if (filePath) {
    const lastInd = filePath.lastIndexOf('/');
    if (lastInd >= 0) {
      fileName = filePath.substr(filePath.lastIndexOf('/') + 1);
    } else {
      fileName = filePath;
    }
  }
  fileLinkEle.setAttribute('download', fileName);
  document.body.appendChild(fileLinkEle);
  fileLinkEle.click();
  document.body.removeChild(fileLinkEle);
  window.URL.revokeObjectURL(objUrl);
}
