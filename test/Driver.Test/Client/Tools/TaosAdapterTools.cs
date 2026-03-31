using System;
using System.Diagnostics;
using System.Globalization;
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

            ProcessStartInfo startInfo = new ProcessStartInfo(exec, $"--port {port} --instanceId {port}");
            Process process = new Process { StartInfo = startInfo };
            return process;
        }

        public static async Task StartTaosAdapter(Process process, string port)
        {
            process.Start();
            await WaitForStart(port).ConfigureAwait(false);
        }

        public static Task<bool> CanPingHost(string host, string port)
        {
            return WaitForPingSuccess(_httpClient, BuildPingUrl(host, port));
        }

        public static void StopTaosAdapter(Process process)
        {
            if (process == null) return;
            try
            {
                if (!process.HasExited)
                {
                    process.Kill();
                    process.WaitForExit(5000); // 等待进程退出
                }
            }
            catch (InvalidOperationException)
            {
                // process may have never started
            }
            finally
            {
                process.Close();
            }
        }

        private static async Task WaitForStart(string port)
        {
            string url = BuildPingUrl("127.0.0.1", port);
            bool success = await WaitForPingSuccess(_httpClient, url).ConfigureAwait(false);
            if (!success)
            {
                throw new Exception("Failed to start taosadapter");
            }
        }

        private static string BuildPingUrl(string host, string port)
        {
            return new UriBuilder(Uri.UriSchemeHttp, host, int.Parse(port, CultureInfo.InvariantCulture), "/-/ping")
                .Uri
                .ToString();
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
                    using (HttpResponseMessage response = await client.GetAsync(url).ConfigureAwait(false))
                    {
                        if (response.IsSuccessStatusCode)
                        {
                            success = true;
                            break;
                        }
                    }
                }
                catch (Exception)
                {
                    // ignored
                }

                await Task.Delay(retryDelayMs).ConfigureAwait(false);
            }

            return success;
        }
    }
}
