module.exports = {
  pre: {
    server: [
      {
        host: "101.201.226.17",
        username: "root",
        password: "Tbase125!",
        dirPath: "/tdcdata",
        fileName: "TDC-frontend",
      },
      {
        host: "101.201.227.153",
        username: "root",
        password: "Tbase125!",
        dirPath: "/tdcdata",
        fileName: "TDC-frontend",
      },
    ],
    zipName: "TDC-frontend",
    localZipDir: "TDC-Hub-fe",
  },
  test: {
    server: { host: "192.168.1.106", username: "root", password: "tbase125!", dirPath: "/data", fileName: "TDC-frontend" },
    zipName: "TDC-frontend",
    localZipDir: "TDC-Hub-fe",
  },
  prd: {
    isUpload: false,
    localZipDir: "TDC-Hub-fe",
    zipName: "TDC-frontend",
  },
};
