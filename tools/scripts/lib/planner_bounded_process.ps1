# 策划 migration 同 runspace 专用的 Windows 原生进程边界。
#
# CreateProcess(STARTUPINFOEX JOB_LIST + CREATE_SUSPENDED) -> ResumeThread 保证子进程在创建的
# 同一内核操作中就进入 KILL_ON_JOB_CLOSE Job，且在执行用户代码前已完成管道装配。这既
# 避免 Start/Assign 窗口逃出孙进程，也让
# 父 pwsh 被硬杀时由内核关闭 Job handle，回收整棵进程树。
# stdout/stderr 持续 drain 但每条管道只保留有界字节；stdin 写入与子进程等待共用同一个
# Stopwatch 单调 deadline。超时后只有一个额外有界 cleanup 窗口，用于确认内核已终止整树并关闭管道；
# 超时结果永远 fail-closed，不会因 cleanup 期间观察到 exit 0 而改判成功。

Set-StrictMode -Version Latest

if ($null -eq ('Pandora.Planner.Processes.BoundedProcessRunner' -as [type])) {
    Add-Type -TypeDefinition @'
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;

namespace Pandora.Planner.Processes
{
    public sealed class BoundedProcessResult
    {
        public string Name { get; internal set; }
        public int ProcessId { get; internal set; }
        public int ExitCode { get; internal set; }
        public int? ProcessExitCode { get; internal set; }
        public bool TimedOut { get; internal set; }
        public bool DrainCompleted { get; internal set; }
        public bool StandardInputCompleted { get; internal set; }
        public long ElapsedMilliseconds { get; internal set; }
        public string StandardOutput { get; internal set; }
        public string StandardError { get; internal set; }
        public bool StandardOutputTruncated { get; internal set; }
        public bool StandardErrorTruncated { get; internal set; }
        public string Failure { get; internal set; }
    }

    internal sealed class BoundedPipeCapture
    {
        private readonly object gate = new object();
        private readonly MemoryStream retained;
        private readonly int limit;
        private bool truncated;
        private Exception failure;

        internal BoundedPipeCapture(int maximumBytes)
        {
            limit = maximumBytes;
            retained = new MemoryStream(Math.Min(maximumBytes, 8192));
        }

        internal async Task DrainAsync(Stream stream)
        {
            byte[] buffer = new byte[8192];
            try
            {
                while (true)
                {
                    int count = await stream.ReadAsync(buffer, 0, buffer.Length).ConfigureAwait(false);
                    if (count == 0) return;
                    lock (gate)
                    {
                        int remaining = limit - checked((int)retained.Length);
                        int keep = Math.Min(Math.Max(remaining, 0), count);
                        if (keep > 0) retained.Write(buffer, 0, keep);
                        if (keep != count) truncated = true;
                    }
                }
            }
            catch (Exception error)
            {
                lock (gate) { failure = error; }
            }
        }

        internal byte[] SnapshotBytes()
        {
            lock (gate) { return retained.ToArray(); }
        }

        internal bool Truncated
        {
            get { lock (gate) { return truncated; } }
        }

        internal Exception Failure
        {
            get { lock (gate) { return failure; } }
        }
    }

    public static class BoundedProcessRunner
    {
        private const uint CREATE_SUSPENDED = 0x00000004;
        private const uint CREATE_NO_WINDOW = 0x08000000;
        private const uint CREATE_UNICODE_ENVIRONMENT = 0x00000400;
        private const uint EXTENDED_STARTUPINFO_PRESENT = 0x00080000;
        private const int STARTF_USESTDHANDLES = 0x00000100;
        private const uint HANDLE_FLAG_INHERIT = 0x00000001;
        private const uint JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000;
        private const int JobObjectExtendedLimitInformation = 9;
        private static readonly IntPtr PROC_THREAD_ATTRIBUTE_HANDLE_LIST = new IntPtr(0x00020002);
        private static readonly IntPtr PROC_THREAD_ATTRIBUTE_JOB_LIST = new IntPtr(0x0002000D);
        private const uint WAIT_OBJECT_0 = 0;
        private const uint WAIT_TIMEOUT = 258;
        private const uint WAIT_FAILED = 0xFFFFFFFF;
        private const uint STILL_ACTIVE = 259;

        [StructLayout(LayoutKind.Sequential)]
        private struct SECURITY_ATTRIBUTES
        {
            internal int nLength;
            internal IntPtr lpSecurityDescriptor;
            [MarshalAs(UnmanagedType.Bool)] internal bool bInheritHandle;
        }

        [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
        private struct STARTUPINFO
        {
            internal int cb;
            internal string lpReserved;
            internal string lpDesktop;
            internal string lpTitle;
            internal int dwX;
            internal int dwY;
            internal int dwXSize;
            internal int dwYSize;
            internal int dwXCountChars;
            internal int dwYCountChars;
            internal int dwFillAttribute;
            internal int dwFlags;
            internal short wShowWindow;
            internal short cbReserved2;
            internal IntPtr lpReserved2;
            internal IntPtr hStdInput;
            internal IntPtr hStdOutput;
            internal IntPtr hStdError;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct PROCESS_INFORMATION
        {
            internal IntPtr hProcess;
            internal IntPtr hThread;
            internal uint dwProcessId;
            internal uint dwThreadId;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct STARTUPINFOEX
        {
            internal STARTUPINFO StartupInfo;
            internal IntPtr lpAttributeList;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct JOBOBJECT_BASIC_LIMIT_INFORMATION
        {
            internal long PerProcessUserTimeLimit;
            internal long PerJobUserTimeLimit;
            internal uint LimitFlags;
            internal UIntPtr MinimumWorkingSetSize;
            internal UIntPtr MaximumWorkingSetSize;
            internal uint ActiveProcessLimit;
            internal UIntPtr Affinity;
            internal uint PriorityClass;
            internal uint SchedulingClass;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct IO_COUNTERS
        {
            internal ulong ReadOperationCount;
            internal ulong WriteOperationCount;
            internal ulong OtherOperationCount;
            internal ulong ReadTransferCount;
            internal ulong WriteTransferCount;
            internal ulong OtherTransferCount;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct JOBOBJECT_EXTENDED_LIMIT_INFORMATION
        {
            internal JOBOBJECT_BASIC_LIMIT_INFORMATION BasicLimitInformation;
            internal IO_COUNTERS IoInfo;
            internal UIntPtr ProcessMemoryLimit;
            internal UIntPtr JobMemoryLimit;
            internal UIntPtr PeakProcessMemoryUsed;
            internal UIntPtr PeakJobMemoryUsed;
        }

        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        private static extern IntPtr CreateJobObjectW(IntPtr jobAttributes, string name);

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool SetInformationJobObject(
            IntPtr job,
            int informationClass,
            IntPtr information,
            uint informationLength);

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool TerminateJobObject(IntPtr job, uint exitCode);

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool CreatePipe(
            out IntPtr readPipe,
            out IntPtr writePipe,
            ref SECURITY_ATTRIBUTES pipeAttributes,
            uint size);

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool SetHandleInformation(IntPtr handle, uint mask, uint flags);

        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool CreateProcessW(
            string applicationName,
            StringBuilder commandLine,
            IntPtr processAttributes,
            IntPtr threadAttributes,
            [MarshalAs(UnmanagedType.Bool)] bool inheritHandles,
            uint creationFlags,
            IntPtr environment,
            string currentDirectory,
            ref STARTUPINFOEX startupInfo,
            out PROCESS_INFORMATION processInformation);

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool InitializeProcThreadAttributeList(
            IntPtr attributeList,
            int attributeCount,
            uint flags,
            ref IntPtr size);

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool UpdateProcThreadAttribute(
            IntPtr attributeList,
            uint flags,
            IntPtr attribute,
            IntPtr value,
            IntPtr size,
            IntPtr previousValue,
            IntPtr returnSize);

        [DllImport("kernel32.dll")]
        private static extern void DeleteProcThreadAttributeList(IntPtr attributeList);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern uint ResumeThread(IntPtr thread);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern uint WaitForSingleObject(IntPtr handle, uint milliseconds);

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool GetExitCodeProcess(IntPtr process, out uint exitCode);

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool TerminateProcess(IntPtr process, uint exitCode);

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool CloseHandle(IntPtr handle);

        private static void ThrowLastWin32(string operation)
        {
            throw new Win32Exception(Marshal.GetLastWin32Error(), operation);
        }

        private static void CloseNativeHandle(ref IntPtr handle)
        {
            if (handle == IntPtr.Zero || handle == new IntPtr(-1)) return;
            IntPtr ownedHandle = handle;
            handle = IntPtr.Zero;
            if (!CloseHandle(ownedHandle)) ThrowLastWin32("CloseHandle");
        }

        private static void TryCloseNativeHandle(ref IntPtr handle)
        {
            try { CloseNativeHandle(ref handle); }
            catch { }
        }

        private static void TryDisposeFileStream(ref FileStream stream)
        {
            FileStream ownedStream = stream;
            stream = null;
            if (ownedStream == null) return;
            try { ownedStream.Dispose(); }
            catch { }
        }

        private static void TryDispose(IDisposable value)
        {
            if (value == null) return;
            try { value.Dispose(); }
            catch { }
        }

        private static void TryDeleteAttributeList(ref IntPtr attributeList, ref bool initialized)
        {
            bool shouldDelete = initialized;
            initialized = false;
            if (!shouldDelete || attributeList == IntPtr.Zero) return;
            try { DeleteProcThreadAttributeList(attributeList); }
            catch { }
        }

        private static void TryFreeHGlobal(ref IntPtr value)
        {
            IntPtr ownedValue = value;
            value = IntPtr.Zero;
            if (ownedValue == IntPtr.Zero) return;
            try { Marshal.FreeHGlobal(ownedValue); }
            catch { }
        }

        private static void InjectFault(string configuredPoint, string point, int processId)
        {
            if (!String.Equals(configuredPoint, point, StringComparison.Ordinal)) return;
            InvalidOperationException error = new InvalidOperationException(
                "Injected bounded-process fault at " + point + ".");
            error.Data["PandoraFaultPoint"] = point;
            error.Data["PandoraProcessId"] = processId;
            throw error;
        }

        private static void CreateParentReadPipe(out IntPtr parentRead, out IntPtr childWrite)
        {
            SECURITY_ATTRIBUTES attributes = new SECURITY_ATTRIBUTES
            {
                nLength = Marshal.SizeOf(typeof(SECURITY_ATTRIBUTES)),
                bInheritHandle = true
            };
            if (!CreatePipe(out parentRead, out childWrite, ref attributes, 0))
                ThrowLastWin32("CreatePipe(output)");
            if (!SetHandleInformation(parentRead, HANDLE_FLAG_INHERIT, 0))
                ThrowLastWin32("SetHandleInformation(output read)");
        }

        private static void CreateParentWritePipe(out IntPtr childRead, out IntPtr parentWrite)
        {
            SECURITY_ATTRIBUTES attributes = new SECURITY_ATTRIBUTES
            {
                nLength = Marshal.SizeOf(typeof(SECURITY_ATTRIBUTES)),
                bInheritHandle = true
            };
            if (!CreatePipe(out childRead, out parentWrite, ref attributes, 0))
                ThrowLastWin32("CreatePipe(input)");
            if (!SetHandleInformation(parentWrite, HANDLE_FLAG_INHERIT, 0))
                ThrowLastWin32("SetHandleInformation(input write)");
        }

        private static FileStream TakePipeHandle(
            ref IntPtr rawHandle,
            FileAccess access,
            string faultInjectionPoint,
            string transferFaultPoint,
            int processId)
        {
            // SafeFileHandle 构造成功即先清掉 raw slot，之后只有同一个 SafeHandle 对象拥有
            // native handle。这样 FileStream 构造抛错时，finally 也不会拿陈旧整数再次 CloseHandle，
            // 更不会误关已被系统复用成其它资源的 handle。
            SafeFileHandle safeHandle = new SafeFileHandle(rawHandle, true);
            rawHandle = IntPtr.Zero;
            try
            {
                InjectFault(faultInjectionPoint, transferFaultPoint, processId);
                FileStream stream = new FileStream(safeHandle, access, 4096, false);
                safeHandle = null; // FileStream 从这里起持有同一个 SafeFileHandle。
                return stream;
            }
            finally
            {
                // FileStream 构造失败时 SafeHandle 仍是唯一 owner；Dispose 同一 SafeHandle
                // 即使构造器内部已开始清理也是幂等的，不会按陈旧 IntPtr 二次关闭。
                TryDispose(safeHandle);
            }
        }

        private static string QuoteArgument(string value)
        {
            if (value == null) value = String.Empty;
            if (value.Length > 0 && value.IndexOfAny(new[] { ' ', '\t', '\n', '\v', '"' }) < 0)
                return value;

            StringBuilder quoted = new StringBuilder(value.Length + 2);
            quoted.Append('"');
            int slashes = 0;
            foreach (char character in value)
            {
                if (character == '\\')
                {
                    slashes++;
                    continue;
                }
                if (character == '"')
                {
                    quoted.Append('\\', slashes * 2 + 1);
                    quoted.Append('"');
                    slashes = 0;
                    continue;
                }
                if (slashes > 0)
                {
                    quoted.Append('\\', slashes);
                    slashes = 0;
                }
                quoted.Append(character);
            }
            if (slashes > 0) quoted.Append('\\', slashes * 2);
            quoted.Append('"');
            return quoted.ToString();
        }

        private static StringBuilder BuildCommandLine(string executable, string[] arguments)
        {
            StringBuilder commandLine = new StringBuilder(QuoteArgument(executable));
            if (arguments != null)
            {
                foreach (string argument in arguments)
                {
                    commandLine.Append(' ');
                    commandLine.Append(QuoteArgument(argument));
                }
            }
            if (commandLine.Length >= 32767)
                throw new ArgumentException("Windows command line exceeds 32766 UTF-16 characters.", "arguments");
            return commandLine;
        }

        private static IntPtr BuildEnvironmentBlock(IDictionary<string, string> overrides)
        {
            if (overrides == null || overrides.Count == 0) return IntPtr.Zero;
            SortedDictionary<string, string> values =
                new SortedDictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (DictionaryEntry entry in Environment.GetEnvironmentVariables())
                values[(string)entry.Key] = (string)entry.Value;
            foreach (KeyValuePair<string, string> entry in overrides)
            {
                if (String.IsNullOrEmpty(entry.Key) || entry.Key.IndexOf('=') >= 0)
                    throw new ArgumentException("Environment variable names must be non-empty and cannot contain '='.", "overrides");
                if (entry.Value == null) values.Remove(entry.Key);
                else values[entry.Key] = entry.Value;
            }
            StringBuilder block = new StringBuilder();
            foreach (KeyValuePair<string, string> entry in values)
            {
                block.Append(entry.Key).Append('=').Append(entry.Value).Append('\0');
            }
            block.Append('\0');
            return Marshal.StringToHGlobalUni(block.ToString());
        }

        private static int RemainingMilliseconds(long deadlineTimestamp)
        {
            long remainingTicks = deadlineTimestamp - Stopwatch.GetTimestamp();
            if (remainingTicks <= 0) return 0;
            double milliseconds = remainingTicks * 1000.0 / Stopwatch.Frequency;
            return (int)Math.Min(Int32.MaxValue, Math.Max(1.0, Math.Ceiling(milliseconds)));
        }

        private static bool WaitHandleUntil(IntPtr handle, long deadlineTimestamp)
        {
            while (true)
            {
                int remaining = RemainingMilliseconds(deadlineTimestamp);
                if (remaining <= 0) return false;
                uint wait = WaitForSingleObject(handle, (uint)remaining);
                if (wait == WAIT_OBJECT_0) return true;
                if (wait == WAIT_TIMEOUT) return false;
                if (wait == WAIT_FAILED) ThrowLastWin32("WaitForSingleObject");
                throw new InvalidOperationException("Unexpected WaitForSingleObject result: " + wait);
            }
        }

        private static bool WaitTaskUntil(Task task, long deadlineTimestamp)
        {
            if (task == null) return true;
            while (!task.IsCompleted)
            {
                int remaining = RemainingMilliseconds(deadlineTimestamp);
                if (remaining <= 0) return false;
                try { task.Wait(remaining); }
                catch (AggregateException) { return true; }
            }
            return true;
        }

        private static async Task WriteStandardInputAsync(Stream stream, string standardInput)
        {
            try
            {
                if (standardInput != null)
                {
                    byte[] bytes = new UTF8Encoding(false).GetBytes(standardInput);
                    await stream.WriteAsync(bytes, 0, bytes.Length).ConfigureAwait(false);
                    await stream.FlushAsync().ConfigureAwait(false);
                }
            }
            finally
            {
                stream.Dispose();
            }
        }

        private static string DecodeUtf8(byte[] value)
        {
            return new UTF8Encoding(false, false).GetString(value ?? new byte[0]);
        }

        private static string TaskFailure(Task task)
        {
            if (task == null || !task.IsFaulted || task.Exception == null) return null;
            Exception error = task.Exception.GetBaseException();
            return error.GetType().Name + ": " + error.Message;
        }

        public static BoundedProcessResult Run(
            string name,
            string executable,
            string[] arguments,
            string workingDirectory,
            IDictionary<string, string> environmentOverrides,
            string standardInput,
            int timeoutMilliseconds,
            int cleanupTimeoutMilliseconds,
            int maximumOutputBytesPerStream,
            long elapsedBeforeStartMilliseconds,
            string faultInjectionPoint)
        {
            long startedTimestamp = Stopwatch.GetTimestamp();
            if (String.IsNullOrWhiteSpace(name)) throw new ArgumentException("Process name is required.", "name");
            if (String.IsNullOrWhiteSpace(executable) || !Path.IsPathRooted(executable))
                throw new ArgumentException("Executable must be an absolute path.", "executable");
            if (String.IsNullOrWhiteSpace(workingDirectory) || !Path.IsPathRooted(workingDirectory))
                throw new ArgumentException("Working directory must be an absolute path.", "workingDirectory");
            if (timeoutMilliseconds <= 0) throw new ArgumentOutOfRangeException("timeoutMilliseconds");
            if (cleanupTimeoutMilliseconds <= 0) throw new ArgumentOutOfRangeException("cleanupTimeoutMilliseconds");
            if (maximumOutputBytesPerStream <= 0) throw new ArgumentOutOfRangeException("maximumOutputBytesPerStream");
            if (elapsedBeforeStartMilliseconds < 0)
                throw new ArgumentOutOfRangeException("elapsedBeforeStartMilliseconds");

            long operationDeadline = startedTimestamp +
                (long)Math.Ceiling(timeoutMilliseconds * (double)Stopwatch.Frequency / 1000.0);
            IntPtr job = IntPtr.Zero;
            IntPtr environmentBlock = IntPtr.Zero;
            IntPtr attributeList = IntPtr.Zero;
            IntPtr jobListValue = IntPtr.Zero;
            IntPtr handleListValue = IntPtr.Zero;
            bool attributeListInitialized = false;
            IntPtr stdoutRead = IntPtr.Zero;
            IntPtr stdoutWrite = IntPtr.Zero;
            IntPtr stderrRead = IntPtr.Zero;
            IntPtr stderrWrite = IntPtr.Zero;
            IntPtr stdinRead = IntPtr.Zero;
            IntPtr stdinWrite = IntPtr.Zero;
            PROCESS_INFORMATION process = new PROCESS_INFORMATION();
            FileStream stdoutStream = null;
            FileStream stderrStream = null;
            FileStream stdinStream = null;
            Task stdoutTask = null;
            Task stderrTask = null;
            Task stdinTask = null;
            BoundedPipeCapture stdoutCapture = new BoundedPipeCapture(maximumOutputBytesPerStream);
            BoundedPipeCapture stderrCapture = new BoundedPipeCapture(maximumOutputBytesPerStream);
            bool timedOut = false;
            bool rootExited = false;
            bool drainCompleted = false;
            bool inputCompleted = false;
            int processId = 0;
            int? processExitCode = null;
            long processFinishedTimestamp = startedTimestamp;
            List<string> failures = new List<string>();

            try
            {
                job = CreateJobObjectW(IntPtr.Zero, null);
                if (job == IntPtr.Zero) ThrowLastWin32("CreateJobObjectW");
                JOBOBJECT_EXTENDED_LIMIT_INFORMATION limits = new JOBOBJECT_EXTENDED_LIMIT_INFORMATION();
                limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
                int limitsSize = Marshal.SizeOf(typeof(JOBOBJECT_EXTENDED_LIMIT_INFORMATION));
                IntPtr limitsPointer = Marshal.AllocHGlobal(limitsSize);
                try
                {
                    Marshal.StructureToPtr(limits, limitsPointer, false);
                    if (!SetInformationJobObject(job, JobObjectExtendedLimitInformation,
                        limitsPointer, (uint)limitsSize))
                        ThrowLastWin32("SetInformationJobObject(KILL_ON_JOB_CLOSE)");
                }
                finally { TryFreeHGlobal(ref limitsPointer); }

                CreateParentReadPipe(out stdoutRead, out stdoutWrite);
                CreateParentReadPipe(out stderrRead, out stderrWrite);
                CreateParentWritePipe(out stdinRead, out stdinWrite);

                STARTUPINFOEX startup = new STARTUPINFOEX();
                startup.StartupInfo = new STARTUPINFO
                {
                    cb = Marshal.SizeOf(typeof(STARTUPINFOEX)),
                    dwFlags = STARTF_USESTDHANDLES,
                    hStdInput = stdinRead,
                    hStdOutput = stdoutWrite,
                    hStdError = stderrWrite
                };

                // 在 CreateProcess 的同一个内核操作中把 child 加入 Job，消除“创建后、Assign 前”
                // 父 pwsh 被硬杀时留下悬停孤儿的窗口。同时只允许 child 继承三个标准管道
                // handle，不把父 runspace 中其它可继承文件、socket 或 token 泄漏给 migration。
                IntPtr attributeListSize = IntPtr.Zero;
                InitializeProcThreadAttributeList(IntPtr.Zero, 2, 0, ref attributeListSize);
                if (attributeListSize == IntPtr.Zero)
                    ThrowLastWin32("InitializeProcThreadAttributeList(size)");
                attributeList = Marshal.AllocHGlobal(attributeListSize);
                if (!InitializeProcThreadAttributeList(attributeList, 2, 0, ref attributeListSize))
                    ThrowLastWin32("InitializeProcThreadAttributeList");
                attributeListInitialized = true;

                jobListValue = Marshal.AllocHGlobal(IntPtr.Size);
                Marshal.WriteIntPtr(jobListValue, job);
                if (!UpdateProcThreadAttribute(attributeList, 0, PROC_THREAD_ATTRIBUTE_JOB_LIST,
                    jobListValue, new IntPtr(IntPtr.Size), IntPtr.Zero, IntPtr.Zero))
                    ThrowLastWin32("UpdateProcThreadAttribute(JOB_LIST)");

                handleListValue = Marshal.AllocHGlobal(IntPtr.Size * 3);
                Marshal.WriteIntPtr(handleListValue, 0 * IntPtr.Size, stdinRead);
                Marshal.WriteIntPtr(handleListValue, 1 * IntPtr.Size, stdoutWrite);
                Marshal.WriteIntPtr(handleListValue, 2 * IntPtr.Size, stderrWrite);
                if (!UpdateProcThreadAttribute(attributeList, 0, PROC_THREAD_ATTRIBUTE_HANDLE_LIST,
                    handleListValue, new IntPtr(IntPtr.Size * 3), IntPtr.Zero, IntPtr.Zero))
                    ThrowLastWin32("UpdateProcThreadAttribute(HANDLE_LIST)");
                startup.lpAttributeList = attributeList;

                environmentBlock = BuildEnvironmentBlock(environmentOverrides);
                uint creationFlags = CREATE_SUSPENDED | CREATE_NO_WINDOW | EXTENDED_STARTUPINFO_PRESENT;
                if (environmentBlock != IntPtr.Zero) creationFlags |= CREATE_UNICODE_ENVIRONMENT;
                StringBuilder commandLine = BuildCommandLine(executable, arguments);
                if (RemainingMilliseconds(operationDeadline) <= 0)
                    throw new TimeoutException("Bounded process deadline elapsed before CreateProcessW.");
                if (!CreateProcessW(executable, commandLine, IntPtr.Zero, IntPtr.Zero, true,
                    creationFlags, environmentBlock, workingDirectory, ref startup, out process))
                    ThrowLastWin32("CreateProcessW(CREATE_SUSPENDED)");
                processId = checked((int)process.dwProcessId);
                InjectFault(faultInjectionPoint, "AfterCreateProcess", processId);

                // 父进程立即关闭所有 child-side pipe handle；否则自己会让 EOF 永远不到。
                CloseNativeHandle(ref stdoutWrite);
                CloseNativeHandle(ref stderrWrite);
                CloseNativeHandle(ref stdinRead);

                // CreatePipe 生成的是同步 handle；FileStream 不得误标为 overlapped。
                // ReadAsync/WriteAsync 仍会在有界 Task 中持续 drain，避免子进程被管道反压卡住。
                stdoutStream = TakePipeHandle(ref stdoutRead, FileAccess.Read, faultInjectionPoint,
                    "BeforeStdoutFileStream", processId);
                stderrStream = TakePipeHandle(ref stderrRead, FileAccess.Read, faultInjectionPoint,
                    "BeforeStderrFileStream", processId);
                stdinStream = TakePipeHandle(ref stdinWrite, FileAccess.Write, faultInjectionPoint,
                    "BeforeStdinFileStream", processId);
                stdoutTask = stdoutCapture.DrainAsync(stdoutStream);
                stderrTask = stderrCapture.DrainAsync(stderrStream);
                stdinTask = WriteStandardInputAsync(stdinStream, standardInput);
                // stdinTask 会主动关闭；本局仍保留引用，finally 可在任务异常卡住时强制释放。
                if (RemainingMilliseconds(operationDeadline) <= 0)
                {
                    timedOut = true;
                    TerminateJobObject(job, 1);
                }
                else if (ResumeThread(process.hThread) == UInt32.MaxValue)
                {
                    ThrowLastWin32("ResumeThread");
                }

                CloseNativeHandle(ref process.hThread);
                if (!timedOut) rootExited = WaitHandleUntil(process.hProcess, operationDeadline);
                if (!rootExited)
                {
                    timedOut = true;
                    if (!TerminateJobObject(job, 1))
                    {
                        int terminateError = Marshal.GetLastWin32Error();
                        failures.Add("TerminateJobObject: " + new Win32Exception(terminateError).Message);
                    }
                    // 不等 cleanup deadline 耗尽才关 Job。无论 TerminateJobObject 返回什么，
                    // KILL_ON_JOB_CLOSE 都立即作为第二条内核终止路径。
                    CloseNativeHandle(ref job);
                    long cleanupDeadline = Stopwatch.GetTimestamp() +
                        (long)Math.Ceiling(cleanupTimeoutMilliseconds * (double)Stopwatch.Frequency / 1000.0);
                    rootExited = WaitHandleUntil(process.hProcess, cleanupDeadline);
                    processFinishedTimestamp = Stopwatch.GetTimestamp();
                    // Job 已终止，继续在同一 cleanup deadline 内收口三条管道。
                    inputCompleted = WaitTaskUntil(stdinTask, cleanupDeadline);
                    bool stdoutCompleted = WaitTaskUntil(stdoutTask, cleanupDeadline);
                    bool stderrCompleted = WaitTaskUntil(stderrTask, cleanupDeadline);
                    drainCompleted = stdoutCompleted && stderrCompleted;
                }
                else
                {
                    processFinishedTimestamp = Stopwatch.GetTimestamp();
                    // root 退出就关 Job：不允许它遗留脱后台孙进程。
                    CloseNativeHandle(ref job);
                    inputCompleted = WaitTaskUntil(stdinTask, operationDeadline);
                    bool stdoutCompleted = WaitTaskUntil(stdoutTask, operationDeadline);
                    bool stderrCompleted = WaitTaskUntil(stderrTask, operationDeadline);
                    drainCompleted = stdoutCompleted && stderrCompleted;
                }

                if (rootExited)
                {
                    uint nativeExitCode;
                    if (!GetExitCodeProcess(process.hProcess, out nativeExitCode))
                        ThrowLastWin32("GetExitCodeProcess");
                    if (nativeExitCode != STILL_ACTIVE) processExitCode = unchecked((int)nativeExitCode);
                }

                string stdinFailure = TaskFailure(stdinTask);
                if (stdinFailure != null) failures.Add("stdin: " + stdinFailure);
                if (!inputCompleted) failures.Add("stdin did not close within the bounded deadline");
                if (stdoutCapture.Failure != null)
                    failures.Add("stdout: " + stdoutCapture.Failure.GetType().Name + ": " + stdoutCapture.Failure.Message);
                if (stderrCapture.Failure != null)
                    failures.Add("stderr: " + stderrCapture.Failure.GetType().Name + ": " + stderrCapture.Failure.Message);
                if (stdoutCapture.Failure != null || stderrCapture.Failure != null) drainCompleted = false;
                if (!drainCompleted) failures.Add("stdout/stderr did not drain within the bounded deadline");
                if (!rootExited) failures.Add("exact root process did not exit within cleanup deadline");

                bool publicFailure = timedOut || !rootExited || !drainCompleted || !inputCompleted ||
                    stdinFailure != null || stdoutCapture.Failure != null || stderrCapture.Failure != null ||
                    processExitCode == null;
                return new BoundedProcessResult
                {
                    Name = name,
                    ProcessId = processId,
                    ExitCode = publicFailure ? -1 : processExitCode.Value,
                    ProcessExitCode = processExitCode,
                    TimedOut = timedOut,
                    DrainCompleted = drainCompleted,
                    StandardInputCompleted = inputCompleted && stdinFailure == null,
                    ElapsedMilliseconds = elapsedBeforeStartMilliseconds + (long)Math.Max(0.0, Math.Round(
                        (processFinishedTimestamp - startedTimestamp) * 1000.0 / Stopwatch.Frequency)),
                    StandardOutput = DecodeUtf8(stdoutCapture.SnapshotBytes()),
                    StandardError = DecodeUtf8(stderrCapture.SnapshotBytes()),
                    StandardOutputTruncated = stdoutCapture.Truncated,
                    StandardErrorTruncated = stderrCapture.Truncated,
                    Failure = String.Join("; ", failures.ToArray())
                };
            }
            catch
            {
                // 启动 / 装配任何一步失败都先用 Job 回收已创建的进程树再抛出。
                long cleanupDeadline = Stopwatch.GetTimestamp() +
                    (long)Math.Ceiling(cleanupTimeoutMilliseconds * (double)Stopwatch.Frequency / 1000.0);
                if (job != IntPtr.Zero)
                {
                    try { TerminateJobObject(job, 1); }
                    catch { }
                    // TerminateJobObject 失败也立即 Close：KILL_ON_JOB_CLOSE 是独立的内核兜底。
                    TryCloseNativeHandle(ref job);
                }
                else if (process.hProcess != IntPtr.Zero)
                {
                    try { TerminateProcess(process.hProcess, 1); }
                    catch { }
                }
                if (process.hProcess != IntPtr.Zero)
                {
                    try { WaitHandleUntil(process.hProcess, cleanupDeadline); }
                    catch { }
                }
                TryDisposeFileStream(ref stdinStream);
                TryDisposeFileStream(ref stdoutStream);
                TryDisposeFileStream(ref stderrStream);
                try { WaitTaskUntil(stdinTask, cleanupDeadline); }
                catch { }
                try { WaitTaskUntil(stdoutTask, cleanupDeadline); }
                catch { }
                try { WaitTaskUntil(stderrTask, cleanupDeadline); }
                catch { }
                throw;
            }
            finally
            {
                // 每一项都在自身的非抛出 cleanup helper 内隔离。process/job 最先关闭，
                // 后面的任意 Stream/pipe/attribute 清理异常都不可能留下进程树。
                TryCloseNativeHandle(ref process.hThread);
                TryCloseNativeHandle(ref job);
                TryCloseNativeHandle(ref process.hProcess);
                TryDisposeFileStream(ref stdinStream);
                TryDisposeFileStream(ref stdoutStream);
                TryDisposeFileStream(ref stderrStream);
                TryCloseNativeHandle(ref stdinRead);
                TryCloseNativeHandle(ref stdinWrite);
                TryCloseNativeHandle(ref stdoutRead);
                TryCloseNativeHandle(ref stdoutWrite);
                TryCloseNativeHandle(ref stderrRead);
                TryCloseNativeHandle(ref stderrWrite);
                TryDeleteAttributeList(ref attributeList, ref attributeListInitialized);
                TryFreeHGlobal(ref attributeList);
                TryFreeHGlobal(ref jobListValue);
                TryFreeHGlobal(ref handleListValue);
                TryFreeHGlobal(ref environmentBlock);
            }
        }
    }
}
'@ -Language CSharp -ErrorAction Stop
}

function Invoke-PandoraPlannerBoundedProcess {
    [CmdletBinding()]
    param(
        [string]$Name,
        [Parameter(Mandatory)][string]$FilePath,
        [AllowEmptyCollection()][string[]]$ArgumentList = @(),
        [string]$WorkingDirectory,
        [System.Collections.IDictionary]$Environment,
        [AllowNull()][string]$StandardInput = $null,
        [Parameter(Mandatory)][ValidateRange(1, 3600000)][int]$TimeoutMilliseconds,
        [ValidateRange(1, 60000)][int]$CleanupTimeoutMilliseconds = 5000,
        [ValidateRange(1, 16777216)][int]$MaximumOutputBytesPerStream = 1048576,
        [Parameter(DontShow)][scriptblock]$TestGetPreflightElapsedMilliseconds,
        [Parameter(DontShow)][ValidateSet(
            '',
            'AfterCreateProcess',
            'BeforeStdoutFileStream',
            'BeforeStderrFileStream',
            'BeforeStdinFileStream'
        )][string]$TestFaultInjectionPoint = ''
    )

    $invocationWatch = [Diagnostics.Stopwatch]::StartNew()
    Set-StrictMode -Version Latest
    if (-not $IsWindows) { throw 'Invoke-PandoraPlannerBoundedProcess 当前只支持 Windows Job Object。' }

    if ([string]::IsNullOrWhiteSpace($FilePath) -or -not [IO.Path]::IsPathFullyQualified($FilePath)) {
        throw [ArgumentException]::new('FilePath 必须是绝对路径；策划 migration 不做 PATH 搜索。', 'FilePath')
    }
    $resolvedExecutable = [IO.Path]::GetFullPath($FilePath)
    $resolvedWorkingDirectory = if ([string]::IsNullOrWhiteSpace($WorkingDirectory)) {
        [IO.Path]::GetDirectoryName($resolvedExecutable)
    } else {
        if (-not [IO.Path]::IsPathFullyQualified($WorkingDirectory)) {
            throw [ArgumentException]::new(
                'WorkingDirectory 必须是绝对路径；策划 migration 不做当前目录探测。',
                'WorkingDirectory')
        }
        [IO.Path]::GetFullPath($WorkingDirectory)
    }
    if ([string]::IsNullOrWhiteSpace($resolvedWorkingDirectory)) {
        throw [ArgumentException]::new('无法从 FilePath 推导 WorkingDirectory。', 'WorkingDirectory')
    }
    $resolvedName = if ([string]::IsNullOrWhiteSpace($Name)) {
        [IO.Path]::GetFileNameWithoutExtension($resolvedExecutable)
    } else { $Name }

    $environmentOverrides = [Collections.Generic.Dictionary[string, string]]::new(
        [StringComparer]::OrdinalIgnoreCase)
    if ($null -ne $Environment) {
        foreach ($entry in $Environment.GetEnumerator()) {
            $key = [string]$entry.Key
            $value = if ($null -eq $entry.Value) { $null } else { [string]$entry.Value }
            $environmentOverrides[$key] = $value
        }
    }

    # 公开 TimeoutMilliseconds 从进入 wrapper 起计时；路径规范化与环境装配也不能白拿时间。
    # 生产路径只读单调 Stopwatch。隐藏 callback 仅供纯进程契约注入“预检已耗时”，不执行 IO。
    $preflightElapsedMilliseconds = if ($null -ne $TestGetPreflightElapsedMilliseconds) {
        [long](& $TestGetPreflightElapsedMilliseconds)
    } else {
        [long][Math]::Ceiling(
            $invocationWatch.ElapsedTicks * 1000.0 / [Diagnostics.Stopwatch]::Frequency)
    }
    if ($preflightElapsedMilliseconds -lt 0) {
        throw [ArgumentOutOfRangeException]::new(
            'TestGetPreflightElapsedMilliseconds',
            '预检耗时不得为负数。')
    }
    $remainingTimeoutMilliseconds = [long]$TimeoutMilliseconds - $preflightElapsedMilliseconds
    if ($remainingTimeoutMilliseconds -le 0) {
        throw [TimeoutException]::new(
            "原生进程调用 deadline 已在预检阶段耗尽（总计=${TimeoutMilliseconds}ms，预检=${preflightElapsedMilliseconds}ms）；未创建进程。")
    }

    return [Pandora.Planner.Processes.BoundedProcessRunner]::Run(
        $resolvedName,
        $resolvedExecutable,
        [string[]]$ArgumentList,
        $resolvedWorkingDirectory,
        $environmentOverrides,
        $StandardInput,
        [int]$remainingTimeoutMilliseconds,
        $CleanupTimeoutMilliseconds,
        $MaximumOutputBytesPerStream,
        $preflightElapsedMilliseconds,
        $TestFaultInjectionPoint)
}
