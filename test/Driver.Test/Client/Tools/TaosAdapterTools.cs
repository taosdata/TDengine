using System;
using System.Diagnostics;
using System.Net.Http;
using System.Threading.Tasks;

namespace Driver.Test.Client.Tools
{
    public class TaosAdapterTools
    {
        private static readonly HttpClient _httpClient = new HttpClient();

        public static Process NewTaosAdapter(string port)
        {
            string exec;
            if (Environment.OSVersion.Platform == PlatformID.Win32NT)
            {
                exec = "C:\\TDengine\\taosadapter.exe";
            }
            else
            {
                exec = "taosadapter";
            }

            ProcessStartInfo startInfo = new ProcessStartInfo(exec, $"--port {port}");
            Process process = new Process { StartInfo = startInfo };
            return process;
        }

        public static async Task StartTaosAdapter(Process process, string port)
        {
            process.Start();
            await WaitForStart(port);
        }

        public static void StopTaosAdapter(Process process)
        {
            if (process == null || process.Id == 0 || process.HasExited) return;
            process.Kill();
            process.WaitForExit(5000); // 等待进程退出
            process.Close();
        }

        private static async Task WaitForStart(string port)
        {
            string url = $"http://127.0.0.1:{port}/-/ping";
            bool success = await WaitForPingSuccess(_httpClient, url);
            if (!success)
            {
                throw new Exception("Failed to start taosadapter");
            }
        }

        static async Task<bool> WaitForPingSuccess(HttpClient client, string url)
        {
            bool success = false;
            int retryCount = 50;
            int retryDelayMs = 100;

            for (int i = 0; i < retryCount; i++)
            {
                try
                {
                    HttpResponseMessage response = await client.GetAsync(url);
                    if (response.IsSuccessStatusCode)
                    {
                        success = true;
                        break;
                    }
                }
                catch (Exception e)
                {
                    // ignored
                }

                await Task.Delay(retryDelayMs);
            }

            return success;
        }
    }
}