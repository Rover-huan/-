import {type FormEvent, type ReactNode, useEffect, useMemo, useState} from 'react';
import {
  Activity,
  AlertCircle,
  AlertTriangle,
  BarChart3,
  ChevronDown,
  CheckCircle2,
  Clock3,
  Copy,
  LogOut,
  RefreshCw,
  Search,
  ShieldCheck,
  SlidersHorizontal,
  UserCog,
  Users,
  XCircle,
} from 'lucide-react';
import {
  adminLogin,
  adminLogout,
  cancelAdminJob,
  getAdminDashboard,
  getAdminJobDetail,
  getAdminJobs,
  getAdminSession,
  getAdminUsers,
  getAdminUserUsage,
  grantViewerAdmin,
  resendAdminUserVerification,
  retryAdminJob,
  UnauthorizedError,
  updateAdminUserQuota,
  updateAdminUserStatus,
} from '../../lib/api';
import type {
  AdminAccountResponse,
  AdminDashboardResponse,
  AdminJobDetailResponse,
  AdminJobRow,
  AdminUserRow,
  AdminUserUsageResponse,
  JobStatus,
} from '../../lib/types';
import {cn} from '../../lib/utils';

type AdminTab = 'dashboard' | 'jobs' | 'users' | 'health';

const EMAIL_VERIFICATION_ACTIONS_ENABLED = import.meta.env.VITE_EMAIL_VERIFICATION_ENABLED === 'true';

function formatDate(value: string | null | undefined) {
  if (!value) {
    return '-';
  }
  return new Date(value).toLocaleString();
}

function formatBytes(value: number) {
  if (value < 1024) {
    return `${value} B`;
  }
  if (value < 1024 * 1024) {
    return `${(value / 1024).toFixed(1)} KB`;
  }
  if (value < 1024 * 1024 * 1024) {
    return `${(value / 1024 / 1024).toFixed(1)} MB`;
  }
  return `${(value / 1024 / 1024 / 1024).toFixed(1)} GB`;
}

const JOB_STATUS_LABELS: Record<string, string> = {
  uploaded: '已上传',
  queued_analysis: '分析排队中',
  running_analysis: '正在分析',
  awaiting_selection: '等待选图',
  queued_render: '报告排队中',
  rendering: '最终报告生成中',
  completed: '已完成',
  failed: '失败',
  expired: '已过期',
};

const JOB_PHASE_LABELS: Record<string, string> = {
  upload: '上传',
  analysis: '分析',
  selection: '选图',
  render: '报告生成',
  complete: '完成',
  failed: '失败',
  expired: '过期',
};

const USER_STATUS_LABELS: Record<string, string> = {
  active: '启用',
  disabled: '禁用',
};

const ROLE_LABELS: Record<string, string> = {
  owner: '所有者',
  viewer: '观察员',
};

const EVENT_TYPE_LABELS: Record<string, string> = {
  'job.created': '任务已创建',
  'job.input_uploaded': '输入文件已上传',
  'job.analysis_queued': '分析已排队',
  'job.analysis_started': '分析已开始',
  'job.task_ready': '候选图已生成',
  'job.awaiting_selection': '等待用户选图',
  'job.analysis_retrying': '分析自动重试中',
  'job.analysis_failed': '分析失败',
  'job.render_queued': '报告生成已排队',
  'job.render_started': '报告生成已开始',
  'job.render_completed': '报告生成完成',
  'job.render_retrying': '报告生成自动重试中',
  'job.render_failed': '报告生成失败',
  'job.expired': '任务已过期',
  'job.deleted': '任务已删除',
  'job.analysis_retry_queued_by_admin': '管理员重试分析',
  'job.render_retry_queued_by_admin': '管理员重试报告生成',
  'job.cancelled_by_admin': '管理员取消任务',
};

const READINESS_LABELS: Record<string, string> = {
  database: '数据库',
  redis: 'Redis / Tair',
  storage: '文件存储',
};

const ADMIN_CARD_CLASS = 'rounded-2xl border border-slate-200/80 bg-white/90 shadow-sm shadow-slate-200/50';
const ADMIN_BUTTON_CLASS =
  'inline-flex items-center justify-center gap-2 rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm font-semibold text-slate-700 shadow-sm shadow-slate-200/30 transition hover:border-indigo-200 hover:bg-indigo-50 hover:text-indigo-700 disabled:cursor-not-allowed disabled:opacity-50';
const ADMIN_PRIMARY_BUTTON_CLASS =
  'inline-flex items-center justify-center gap-2 rounded-xl bg-indigo-600 px-3 py-2 text-sm font-bold text-white shadow-sm shadow-indigo-200 transition hover:bg-indigo-700 disabled:cursor-not-allowed disabled:opacity-50';
const ADMIN_DANGER_BUTTON_CLASS =
  'inline-flex items-center justify-center gap-2 rounded-xl border border-rose-200 bg-white px-3 py-2 text-sm font-semibold text-rose-700 transition hover:bg-rose-50 disabled:cursor-not-allowed disabled:opacity-50';
const ADMIN_INPUT_CLASS =
  'rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm outline-none transition placeholder:text-slate-400 focus:border-indigo-300 focus:ring-4 focus:ring-indigo-100 disabled:bg-slate-50 disabled:text-slate-400';

function getJobStatusTone(status: string) {
  if (status === 'completed') {
    return 'border-emerald-100 bg-emerald-50 text-emerald-700';
  }
  if (status === 'running_analysis' || status === 'rendering') {
    return 'border-sky-100 bg-sky-50 text-sky-700';
  }
  if (status === 'queued_analysis' || status === 'queued_render' || status === 'awaiting_selection') {
    return 'border-indigo-100 bg-indigo-50 text-indigo-700';
  }
  if (status === 'failed') {
    return 'border-rose-100 bg-rose-50 text-rose-700';
  }
  if (status === 'expired') {
    return 'border-slate-200 bg-slate-100 text-slate-500';
  }
  return 'border-slate-200 bg-slate-50 text-slate-600';
}

function getJobProgressTone(status: string) {
  if (status === 'completed') {
    return 'bg-emerald-500';
  }
  if (status === 'running_analysis' || status === 'rendering') {
    return 'bg-sky-500';
  }
  if (status === 'failed') {
    return 'bg-rose-500';
  }
  if (status === 'expired') {
    return 'bg-slate-400';
  }
  return 'bg-indigo-500';
}

function getUserStatusTone(status: string, online?: boolean) {
  if (status === 'disabled') {
    return 'border-slate-200 bg-slate-100 text-slate-500';
  }
  if (online) {
    return 'border-emerald-100 bg-emerald-50 text-emerald-700';
  }
  return 'border-slate-200 bg-slate-50 text-slate-600';
}

function StatusChip({children, className}: {children: ReactNode; className?: string}) {
  return (
    <span className={cn('inline-flex items-center rounded-full border px-2.5 py-1 text-xs font-bold leading-4', className)}>
      {children}
    </span>
  );
}

function PageHeading({title, description}: {title: string; description: string}) {
  return (
    <div>
      <h2 className="text-2xl font-black tracking-tight text-slate-950">{title}</h2>
      <p className="mt-2 max-w-2xl text-sm leading-6 text-slate-500">{description}</p>
    </div>
  );
}

function getErrorMessage(error: unknown) {
  if (error instanceof Error) {
    if (
      error.message.includes('Email verification is enabled') ||
      error.message.includes('Verification email could not be sent')
    ) {
      return '当前未开启邮箱验证，无法发送验证邮件。';
    }
    return error.message;
  }
  return '请求失败，请稍后重试。';
}

function translateEventMessage(message: string) {
  if (message === 'Job created and uploads received.') {
    return '任务已创建，上传文件已接收。';
  }
  if (message.startsWith('Uploaded input file ')) {
    return `已上传输入文件 ${message.replace('Uploaded input file ', '')}`;
  }
  if (message === 'Analysis task queued.') {
    return '分析任务已加入队列。';
  }
  if (message === 'Analysis phase started.') {
    return '分析阶段已开始。';
  }
  if (message.startsWith('Prepared candidate chart for task ')) {
    const taskId = message.replace('Prepared candidate chart for task ', '').replace(/\.$/, '');
    return `已为任务 ${taskId} 生成候选图。`;
  }
  if (message === 'Analysis complete; awaiting user chart selection.') {
    return '分析完成，正在等待用户选择图表。';
  }
  if (message.startsWith('Transient upstream model/network error detected; retrying analysis in ')) {
    return `检测到上游模型或网络临时错误，稍后自动重试分析。`;
  }
  if (message === 'Render task queued after user chart selection.') {
    return '用户选图后，报告生成任务已加入队列。';
  }
  if (message === 'Render phase started.') {
    return '报告生成阶段已开始。';
  }
  if (message === 'Render phase completed and artifacts were uploaded.') {
    return '报告生成完成，产物已上传。';
  }
  if (message.startsWith('Transient upstream model/network error detected; retrying render in ')) {
    return '检测到上游模型或网络临时错误，稍后自动重试报告生成。';
  }
  if (message === 'Expired job resources were deleted.') {
    return '过期任务资源已删除。';
  }
  return message;
}

function buildAdminFailureDetails(job: AdminJobDetailResponse) {
  return JSON.stringify(
    {
      task_id: job.job.id,
      stage: job.job.failure_stage || job.job.phase || null,
      error_code: job.job.error_code || job.job.failure_code || 'unknown',
      error_category: job.job.error_category || 'unknown',
      raw_message: job.job.raw_detail || null,
      recent_events: job.events.slice(-20).map((event) => ({
        created_at: event.created_at,
        level: event.level,
        event_type: event.event_type,
        message: event.message,
        payload_json: job.job.raw_detail ? event.payload_json : null,
      })),
    },
    null,
    2,
  );
}

function AdminFailureCard({job}: {job: AdminJobDetailResponse}) {
  const [expanded, setExpanded] = useState(false);
  const [copied, setCopied] = useState(false);
  const canViewRaw = Boolean(job.job.raw_detail);
  const detailText = useMemo(() => buildAdminFailureDetails(job), [job]);
  const suggestedActions = job.job.suggested_actions?.length
    ? job.job.suggested_actions
    : ['请稍后重试。', '如果多次失败，请联系管理员并提供任务 ID。'];

  async function copyDetails() {
    if (typeof navigator === 'undefined' || !navigator.clipboard) {
      return;
    }
    await navigator.clipboard.writeText(detailText);
    setCopied(true);
    window.setTimeout(() => setCopied(false), 1400);
  }

  return (
    <div className="min-w-0 rounded-2xl border border-rose-100 bg-rose-50/80 p-4 text-rose-800">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
        <div className="min-w-0">
          <div className="font-bold">{job.job.error_title || '分析失败'}</div>
          <p className="mt-2 whitespace-pre-wrap break-words text-sm leading-6">
            {job.job.user_message || job.job.error_summary || '本次任务未能完成。'}
          </p>
          <div className="mt-2 rounded-xl bg-white/70 px-2.5 py-1.5 font-mono text-xs break-all text-rose-500">
            任务编号：{job.job.id.slice(0, 8)}
          </div>
        </div>
        <span className="w-fit rounded-full border border-rose-100 bg-white px-2.5 py-1 text-xs font-bold">
          {job.job.error_code || job.job.failure_code || 'unknown'}
        </span>
      </div>
      <div className="mt-4 grid gap-3 md:grid-cols-2">
        <div className="rounded-2xl bg-white/70 p-3">
          <div className="font-bold">建议操作</div>
          <ul className="mt-2 space-y-1 leading-6">
            {suggestedActions.map((action) => (
              <li className="break-words" key={action}>{action}</li>
            ))}
          </ul>
        </div>
        <div className="rounded-2xl bg-white/70 p-3">
          <div className="font-bold">排查信息</div>
          <div className="mt-2 text-sm leading-6">阶段：{job.job.failure_stage || '未知'}</div>
          <div className="break-all text-sm leading-6">分类：{job.job.error_category || 'unknown'}</div>
        </div>
      </div>
      <div className="mt-4 rounded-2xl border border-rose-100 bg-white">
        <button
          type="button"
          className="flex w-full items-center justify-between gap-3 px-3 py-2 text-left font-bold"
          onClick={() => setExpanded((value) => !value)}
        >
          <span>查看技术详情</span>
          <ChevronDown className={cn('h-4 w-4 shrink-0 transition', expanded ? 'rotate-180' : '')} />
        </button>
        {expanded ? (
          <div className="border-t border-rose-100 p-3">
            {!canViewRaw ? (
              <p className="mb-3 text-sm leading-6 text-slate-600">
                当前观察员账号不显示完整技术错误。请让 Owner 账号查看 raw error。
              </p>
            ) : null}
            <div className="mb-3 flex justify-end">
              <button
                className={cn(ADMIN_BUTTON_CLASS, 'text-xs')}
                type="button"
                onClick={() => void copyDetails()}
              >
                <Copy className="h-3.5 w-3.5" />
                {copied ? '已复制' : '复制详情'}
              </button>
            </div>
            <pre className="max-h-80 overflow-auto whitespace-pre-wrap break-words rounded-lg bg-slate-950 p-3 font-mono text-xs leading-6 text-slate-100">
              <code>{detailText}</code>
            </pre>
          </div>
        ) : null}
      </div>
    </div>
  );
}

function Metric({
  label,
  value,
  helper,
  tone = 'slate',
  icon,
}: {
  label: string;
  value: string | number;
  helper?: string;
  tone?: 'slate' | 'green' | 'amber' | 'red';
  icon?: ReactNode;
}) {
  const toneClass = {
    slate: 'border-slate-200/80 bg-white/90 text-slate-900',
    green: 'border-emerald-100 bg-emerald-50/80 text-emerald-950',
    amber: 'border-amber-100 bg-amber-50/80 text-amber-950',
    red: 'border-rose-100 bg-rose-50/80 text-rose-950',
  }[tone];
  const accentClass = {
    slate: 'bg-indigo-500 text-indigo-600',
    green: 'bg-emerald-500 text-emerald-600',
    amber: 'bg-amber-500 text-amber-600',
    red: 'bg-rose-500 text-rose-600',
  }[tone];
  return (
    <div className={cn('rounded-2xl border p-4 shadow-sm shadow-slate-200/40', toneClass)}>
      <div className="flex items-center justify-between gap-3">
        <div className="text-xs font-bold tracking-[0.18em] text-slate-400">{label}</div>
        <div className={cn('flex h-8 w-8 items-center justify-center rounded-xl bg-white/80', accentClass.replace('bg-', 'text-'))}>
          {icon || <span className={cn('h-2.5 w-2.5 rounded-full', accentClass)} />}
        </div>
      </div>
      <div className="mt-3 text-3xl font-black tracking-tight tabular-nums">{value}</div>
      {helper ? <div className="mt-1 text-xs leading-5 text-slate-500">{helper}</div> : null}
    </div>
  );
}

function AdminLoginView({onLogin}: {onLogin: (admin: AdminAccountResponse) => void}) {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setBusy(true);
    setError(null);
    try {
      const session = await adminLogin({email: email.trim().toLowerCase(), password});
      onLogin(session.admin);
    } catch (err) {
      setError(getErrorMessage(err));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="flex min-h-screen items-center justify-center bg-slate-50 px-6 py-12 text-slate-900">
      <form className="w-full max-w-md rounded-[28px] border border-slate-200/80 bg-white/95 p-8 shadow-xl shadow-slate-200/70" onSubmit={submit}>
        <div className="mb-8">
          <div className="mb-4 flex h-12 w-12 items-center justify-center rounded-2xl bg-indigo-600 text-white shadow-lg shadow-indigo-200">
            <ShieldCheck className="h-6 w-6" />
          </div>
          <h1 className="text-2xl font-bold">SmartAnalyst 后台管理</h1>
          <p className="mt-2 text-sm text-slate-500">用于查看运营状态、任务进度、用户与系统健康。</p>
        </div>
        {error ? (
          <div className="mb-5 rounded-2xl border border-rose-100 bg-rose-50 px-4 py-3 text-sm text-rose-700">
            {error}
          </div>
        ) : null}
        <label className="block space-y-2">
          <span className="text-sm font-semibold text-slate-700">邮箱</span>
          <input
            className={cn(ADMIN_INPUT_CLASS, 'w-full px-4 py-3')}
            type="email"
            value={email}
            onChange={(event) => setEmail(event.target.value)}
            required
          />
        </label>
        <label className="mt-4 block space-y-2">
          <span className="text-sm font-semibold text-slate-700">密码</span>
          <input
            className={cn(ADMIN_INPUT_CLASS, 'w-full px-4 py-3')}
            type="password"
            value={password}
            onChange={(event) => setPassword(event.target.value)}
            required
          />
        </label>
        <button
          className={cn(ADMIN_PRIMARY_BUTTON_CLASS, 'mt-6 w-full px-4 py-3')}
          type="submit"
          disabled={busy}
        >
          {busy ? <RefreshCw className="h-4 w-4 animate-spin" /> : <ShieldCheck className="h-4 w-4" />}
          登录后台
        </button>
      </form>
    </div>
  );
}

function DashboardView({dashboard}: {dashboard: AdminDashboardResponse | null}) {
  if (!dashboard) {
    return <div className="p-8 text-sm text-slate-500">正在加载后台数据...</div>;
  }
  const {overview} = dashboard;
  const budgetText =
    overview.llm_daily_budget_limit > 0
      ? `剩余 ${dashboard.llm_usage.remaining} 次`
      : '未设置每日上限';

  return (
    <div className="space-y-6">
      <PageHeading
        title="系统总览"
        description="查看今日任务、在线用户、模型调用与关键风险指标，帮助快速判断系统当前运行状态。"
      />
      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <Metric label="在线用户" value={dashboard.online_users.length} helper="最近 5 分钟活跃" tone="green" icon={<Users className="h-4 w-4" />} />
        <Metric label="活跃任务" value={overview.active_jobs_total} helper="排队中或运行中" icon={<Activity className="h-4 w-4" />} />
        <Metric label="今日失败" value={overview.failed_today} helper={`风险提示 · 失败率 ${(dashboard.failure_rate_today * 100).toFixed(1)}%`} tone={overview.failed_today ? 'red' : 'slate'} icon={<AlertCircle className="h-4 w-4" />} />
        <Metric label="今日模型调用" value={overview.llm_calls_today} helper={budgetText} tone="amber" icon={<AlertTriangle className="h-4 w-4" />} />
      </div>
      <div className="grid gap-4 md:grid-cols-3">
        <Metric label="总用户数" value={overview.total_users} helper={`${overview.verified_users} 个已验证`} icon={<UserCog className="h-4 w-4" />} />
        <Metric label="今日任务" value={overview.jobs_created_today} helper={`${overview.completed_today} 个已完成`} icon={<BarChart3 className="h-4 w-4" />} />
        <Metric label="今日上传量" value={formatBytes(overview.uploads_bytes_today)} icon={<Clock3 className="h-4 w-4" />} />
      </div>
      <section className={ADMIN_CARD_CLASS}>
        <div className="border-b border-slate-100 px-5 py-4">
          <h2 className="font-bold text-slate-900">在线用户</h2>
          <p className="mt-1 text-xs leading-5 text-slate-500">仅展示当前路径、最近活跃时间与弱化任务编号，避免调试信息抢占主视线。</p>
        </div>
        <div className="divide-y divide-slate-100">
          {dashboard.online_users.length ? (
            dashboard.online_users.map((user) => (
              <div className="flex items-center justify-between gap-4 px-5 py-3.5 text-sm transition hover:bg-indigo-50/40" key={user.user_id}>
                <div className="min-w-0">
                  <div className="font-semibold text-slate-800">{user.email}</div>
                  <div className="mt-1 truncate text-xs text-slate-500">{user.current_path || '-'}</div>
                </div>
                <div className="shrink-0 text-right text-xs text-slate-500">
                  <div className="tabular-nums">{formatDate(user.last_seen_at)}</div>
                  <div className="mt-1 text-slate-400">
                    {user.current_job_id ? `任务编号：${user.current_job_id.slice(0, 8)}` : '未停留在具体任务'}
                  </div>
                </div>
              </div>
            ))
          ) : (
            <div className="px-5 py-8 text-sm text-slate-500">当前没有在线用户。</div>
          )}
        </div>
      </section>
    </div>
  );
}

function JobsView({
  jobs,
  selectedJob,
  canOperate,
  onSelect,
  onRetry,
  onCancel,
}: {
  jobs: AdminJobRow[];
  selectedJob: AdminJobDetailResponse | null;
  canOperate: boolean;
  onSelect: (jobId: string) => void;
  onRetry: (jobId: string) => void;
  onCancel: (jobId: string) => void;
}) {
  const selectedJobId = selectedJob?.job.id || null;

  return (
    <div className="space-y-5">
      <PageHeading
        title="任务管理"
        description="查看任务状态、进度、用户归属和最近事件。技术编号弱化展示，优先呈现用户能理解的任务信息。"
      />
      <div className="grid gap-5 xl:grid-cols-[1.2fr_0.8fr]">
        <section className={ADMIN_CARD_CLASS}>
          <div className="border-b border-slate-100 px-5 py-4 font-bold text-slate-900">任务列表</div>
          <div className="divide-y divide-slate-100">
            {jobs.map((job) => {
              const selected = selectedJobId === job.id;

              return (
                <button
                  className={cn(
                    'block w-full px-5 py-4 text-left transition hover:bg-indigo-50/40',
                    selected && 'bg-indigo-50/70 ring-1 ring-inset ring-indigo-100',
                  )}
                  key={job.id}
                  type="button"
                  onClick={() => onSelect(job.id)}
                >
                  <div className="flex items-start justify-between gap-4">
                    <div className="min-w-0">
                      <div className="truncate font-semibold text-slate-900">{job.report_title || '数据分析任务'}</div>
                      <div className="mt-1 truncate text-xs text-slate-500">{job.user_email}</div>
                      <div className="mt-1 break-all text-[11px] leading-4 text-slate-400">任务 ID：{job.id}</div>
                    </div>
                    <StatusChip className={getJobStatusTone(job.status)}>{JOB_STATUS_LABELS[job.status] || job.status}</StatusChip>
                  </div>
                  <div className="mt-3 h-1.5 rounded-full bg-slate-100">
                    <div className={cn('h-1.5 rounded-full', getJobProgressTone(job.status))} style={{width: `${job.progress_percent}%`}} />
                  </div>
                  <div className="mt-2 flex justify-between text-xs text-slate-500">
                    <span>{JOB_PHASE_LABELS[job.phase] || job.phase}</span>
                    <span className="tabular-nums">{formatDate(job.created_at)}</span>
                  </div>
                </button>
              );
            })}
          </div>
        </section>
        <section className={ADMIN_CARD_CLASS}>
          <div className="border-b border-slate-100 px-5 py-4">
            <div className="font-bold text-slate-900">任务详情</div>
            <p className="mt-1 text-xs leading-5 text-slate-500">按基础信息、操作和事件时间线组织，便于快速排查。</p>
          </div>
          {selectedJob ? (
            <div className="space-y-4 p-5 text-sm">
              <div className="rounded-2xl border border-slate-100 bg-slate-50/70 p-4">
                <div className="mb-3 text-xs font-bold tracking-[0.18em] text-slate-400">基础信息</div>
                <div className="space-y-3">
                  <div>
                    <div className="text-xs text-slate-500">报告标题</div>
                    <div className="mt-1 font-semibold text-slate-900">{selectedJob.job.report_title || '数据分析任务'}</div>
                  </div>
                  <div>
                    <div className="text-xs text-slate-500">用户</div>
                    <div className="mt-1 font-semibold text-slate-900">{selectedJob.user.email}</div>
                  </div>
                  <div>
                    <div className="text-xs text-slate-500">任务 ID</div>
                    <div className="mt-1 break-all font-mono text-[11px] leading-5 text-slate-400">{selectedJob.job.id}</div>
                  </div>
                  <div className="flex flex-wrap items-center gap-2 pt-1">
                    <StatusChip className={getJobStatusTone(selectedJob.job.status)}>{JOB_STATUS_LABELS[selectedJob.job.status] || selectedJob.job.status}</StatusChip>
                    <span className="text-xs font-semibold text-slate-500">{JOB_PHASE_LABELS[selectedJob.job.phase] || selectedJob.job.phase}</span>
                  </div>
                </div>
              </div>

              {selectedJob.job.error_summary || selectedJob.job.user_message ? (
                <AdminFailureCard job={selectedJob} />
              ) : null}

              <div className="rounded-2xl border border-slate-100 bg-white p-4">
                <div className="mb-3 text-xs font-bold tracking-[0.18em] text-slate-400">操作</div>
                <div className="flex flex-wrap gap-2">
                  <button
                    className={ADMIN_BUTTON_CLASS}
                    type="button"
                    disabled={!canOperate || selectedJob.job.status !== 'failed'}
                    onClick={() => onRetry(selectedJob.job.id)}
                  >
                    重试失败任务
                  </button>
                  <button
                    className={ADMIN_DANGER_BUTTON_CLASS}
                    type="button"
                    disabled={!canOperate || !['queued_analysis', 'queued_render'].includes(selectedJob.job.status)}
                    onClick={() => onCancel(selectedJob.job.id)}
                  >
                    取消排队任务
                  </button>
                </div>
              </div>

              <div className="rounded-2xl border border-slate-100 bg-white p-4">
                <div className="mb-4 text-xs font-bold tracking-[0.18em] text-slate-400">最近事件</div>
                <div className="max-h-80 space-y-0 overflow-auto">
                  {selectedJob.events.slice(-20).map((event, index, events) => (
                    <div className="relative grid grid-cols-[1rem_1fr] gap-3 pb-4 last:pb-0" key={event.id}>
                      <div className="relative flex justify-center">
                        {index < events.length - 1 ? <div className="absolute top-4 h-full w-px bg-slate-200" /> : null}
                        <span className="relative mt-1 h-2.5 w-2.5 rounded-full border-2 border-white bg-indigo-500 shadow-sm shadow-indigo-200" />
                      </div>
                      <div className="rounded-2xl bg-slate-50/80 px-3 py-2.5">
                        <div className="flex flex-wrap items-center justify-between gap-2">
                          <span className="font-semibold text-slate-800">{EVENT_TYPE_LABELS[event.event_type] || event.event_type}</span>
                          <span className="text-xs tabular-nums text-slate-400">{formatDate(event.created_at)}</span>
                        </div>
                        <div className="mt-1 whitespace-pre-wrap break-words text-slate-600">{translateEventMessage(event.message)}</div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          ) : (
            <div className="p-8 text-sm text-slate-500">请选择一个任务查看详情。</div>
          )}
        </section>
      </div>
    </div>
  );
}

function UsersView({
  users,
  selectedUser,
  canOperate,
  grantEmail,
  onGrantEmailChange,
  onGrantViewer,
  onSelect,
  onToggleStatus,
  onResendVerification,
  onSaveQuota,
  emailVerificationActionsEnabled,
}: {
  users: AdminUserRow[];
  selectedUser: AdminUserUsageResponse | null;
  canOperate: boolean;
  grantEmail: string;
  onGrantEmailChange: (value: string) => void;
  onGrantViewer: () => void;
  onSelect: (userId: string) => void;
  onToggleStatus: (user: AdminUserRow) => void;
  onResendVerification: (userId: string) => void;
  onSaveQuota: (userId: string, values: {daily: string; uploadMb: string; active: string}) => void;
  emailVerificationActionsEnabled: boolean;
}) {
  const [daily, setDaily] = useState('');
  const [uploadMb, setUploadMb] = useState('');
  const [active, setActive] = useState('');

  useEffect(() => {
    setDaily(selectedUser?.quota_override.daily_job_limit?.toString() ?? '');
    setUploadMb(
      selectedUser?.quota_override.daily_upload_bytes_limit
        ? String(Math.round(selectedUser.quota_override.daily_upload_bytes_limit / 1024 / 1024))
        : '',
    );
    setActive(selectedUser?.quota_override.active_job_limit?.toString() ?? '');
  }, [selectedUser]);

  const selectedUserId = selectedUser?.user.id || null;

  return (
    <div className="space-y-5">
      <PageHeading
        title="用户管理"
        description="查看账号状态、在线情况、验证状态和配额使用。选中用户后可在右侧查看详情与可用操作。"
      />
      <div className="grid gap-5 xl:grid-cols-[1.1fr_0.9fr]">
        <section className={ADMIN_CARD_CLASS}>
          <div className="flex items-center justify-between gap-3 border-b border-slate-100 px-5 py-4">
            <div className="font-bold text-slate-900">用户列表</div>
            <div className="flex items-center gap-2">
              <input
                className={cn(ADMIN_INPUT_CLASS, 'w-56')}
                value={grantEmail}
                onChange={(event) => onGrantEmailChange(event.target.value)}
                placeholder="观察员邮箱"
                disabled={!canOperate}
              />
              <button
                className={ADMIN_PRIMARY_BUTTON_CLASS}
                type="button"
                disabled={!canOperate || !grantEmail.trim()}
                onClick={onGrantViewer}
              >
                授予观察员
              </button>
            </div>
          </div>
          <div className="divide-y divide-slate-100">
            {users.map((user) => {
              const selected = selectedUserId === user.id;

              return (
                <button
                  className={cn(
                    'block w-full px-5 py-4 text-left transition hover:bg-indigo-50/40',
                    selected && 'bg-indigo-50/70 ring-1 ring-inset ring-indigo-100',
                  )}
                  key={user.id}
                  type="button"
                  onClick={() => onSelect(user.id)}
                >
                  <div className="flex items-start justify-between gap-4">
                    <div className="min-w-0">
                      <div className="truncate font-semibold text-slate-900">{user.email}</div>
                      <div className="mt-2 flex flex-wrap items-center gap-2">
                        <StatusChip className={getUserStatusTone(user.status, user.online)}>{user.online ? '在线' : '离线'}</StatusChip>
                        {user.email_verified ? (
                          <StatusChip className="border-emerald-100 bg-emerald-50 text-emerald-700">已验证</StatusChip>
                        ) : (
                          <StatusChip className="border-amber-100 bg-amber-50 text-amber-700">未验证</StatusChip>
                        )}
                        {user.status === 'disabled' ? (
                          <StatusChip className="border-slate-200 bg-slate-100 text-slate-500">禁用</StatusChip>
                        ) : null}
                      </div>
                    </div>
                    <span className="shrink-0 text-xs tabular-nums text-slate-500">{formatDate(user.last_seen_at)}</span>
                  </div>
                </button>
              );
            })}
          </div>
        </section>
        <section className={ADMIN_CARD_CLASS}>
          <div className="border-b border-slate-100 px-5 py-4">
            <div className="font-bold text-slate-900">用户详情</div>
            <p className="mt-1 text-xs leading-5 text-slate-500">账号信息、配额概览和操作区分开展示。</p>
          </div>
          {selectedUser ? (
            <div className="space-y-5 p-5 text-sm">
              <div className="rounded-2xl border border-slate-100 bg-slate-50/70 p-4">
                <div className="mb-3 text-xs font-bold tracking-[0.18em] text-slate-400">账号信息</div>
                <div className="font-semibold text-slate-900">{selectedUser.user.email}</div>
                <div className="mt-2 flex flex-wrap items-center gap-2">
                  <StatusChip className={getUserStatusTone(selectedUser.user.status, selectedUser.presence.online)}>
                    {selectedUser.presence.online ? '在线' : '离线'}
                  </StatusChip>
                  {selectedUser.user.email_verified ? (
                    <StatusChip className="border-emerald-100 bg-emerald-50 text-emerald-700">已验证</StatusChip>
                  ) : (
                    <StatusChip className="border-amber-100 bg-amber-50 text-amber-700">未验证</StatusChip>
                  )}
                  {selectedUser.user.status === 'disabled' ? (
                    <StatusChip className="border-slate-200 bg-slate-100 text-slate-500">禁用</StatusChip>
                  ) : null}
                </div>
                <div className="mt-3 text-xs tabular-nums text-slate-500">最后活跃：{formatDate(selectedUser.presence.last_seen_at)}</div>
              </div>

              <div className="grid gap-3 md:grid-cols-3">
                <Metric label="今日任务" value={`${selectedUser.quota.jobs_used_today}/${selectedUser.quota.daily_job_limit}`} />
                <Metric label="今日上传" value={formatBytes(selectedUser.quota.upload_bytes_used_today)} />
                <Metric label="活跃任务" value={`${selectedUser.quota.active_jobs}/${selectedUser.quota.active_job_limit}`} />
              </div>

              <div className="space-y-3 rounded-2xl border border-slate-100 bg-slate-50/80 p-4">
                <div>
                  <div className="font-bold">单用户配额覆盖</div>
                  <p className="mt-1 text-xs leading-5 text-slate-500">留空表示使用系统默认额度，仅填写需要覆盖的字段。</p>
                </div>
                <div className="grid gap-3 md:grid-cols-3">
                  <input className={ADMIN_INPUT_CLASS} value={daily} onChange={(event) => setDaily(event.target.value)} placeholder="每日任务数" disabled={!canOperate} />
                  <input className={ADMIN_INPUT_CLASS} value={uploadMb} onChange={(event) => setUploadMb(event.target.value)} placeholder="每日上传 MB" disabled={!canOperate} />
                  <input className={ADMIN_INPUT_CLASS} value={active} onChange={(event) => setActive(event.target.value)} placeholder="活跃任务数" disabled={!canOperate} />
                </div>
                <button
                  className={ADMIN_PRIMARY_BUTTON_CLASS}
                  type="button"
                  disabled={!canOperate}
                  onClick={() => onSaveQuota(selectedUser.user.id, {daily, uploadMb, active})}
                >
                  保存配额
                </button>
              </div>

              {emailVerificationActionsEnabled ? (
                <div className="rounded-2xl border border-slate-100 bg-white p-4">
                  <div className="mb-3 text-xs font-bold tracking-[0.18em] text-slate-400">普通操作</div>
                  <button className={ADMIN_BUTTON_CLASS} type="button" disabled={!canOperate || selectedUser.user.email_verified} onClick={() => onResendVerification(selectedUser.user.id)}>
                    重发验证邮件
                  </button>
                </div>
              ) : null}

              <div className="rounded-2xl border border-rose-100 bg-rose-50/40 p-4">
                <div className="mb-3 text-xs font-bold tracking-[0.18em] text-rose-400">危险操作</div>
                <button className={ADMIN_DANGER_BUTTON_CLASS} type="button" disabled={!canOperate} onClick={() => onToggleStatus(users.find((user) => user.id === selectedUser.user.id) || ({id: selectedUser.user.id, status: selectedUser.user.status} as AdminUserRow))}>
                  {selectedUser.user.status === 'active' ? '禁用用户' : '启用用户'}
                </button>
              </div>
            </div>
          ) : (
            <div className="p-8 text-sm text-slate-500">请选择一个用户查看使用情况与配额。</div>
          )}
        </section>
      </div>
    </div>
  );
}

function HealthView({dashboard}: {dashboard: AdminDashboardResponse | null}) {
  const checks = dashboard?.readiness.checks || {};
  const checkedAt = formatDate(new Date().toISOString());

  return (
    <div className="space-y-5">
      <PageHeading
        title="系统健康"
        description="检查后台依赖的数据库、Redis/Tair 与文件存储状态。正常项使用绿色标识，异常项保留原因说明。"
      />
      <div className="flex items-center gap-2 text-xs text-slate-500">
        <Clock3 className="h-4 w-4 text-slate-400" />
        <span className="tabular-nums">最后检查时间：{checkedAt}</span>
      </div>
      <div className="grid gap-4 md:grid-cols-3">
        {Object.entries(checks).map(([name, check]) => (
          <section className={cn(ADMIN_CARD_CLASS, 'p-5')} key={name}>
            <div className="flex items-start justify-between gap-4">
              <div>
                <div className="text-xs font-bold tracking-[0.18em] text-slate-400">{READINESS_LABELS[name] || name}</div>
                <div className="mt-2 text-lg font-bold text-slate-950">{check.ok ? '运行正常' : '需要关注'}</div>
              </div>
              <div className={cn('flex h-10 w-10 items-center justify-center rounded-2xl', check.ok ? 'bg-emerald-50 text-emerald-600' : 'bg-rose-50 text-rose-600')}>
                {check.ok ? <CheckCircle2 className="h-5 w-5" /> : <XCircle className="h-5 w-5" />}
              </div>
            </div>
            <p className="mt-4 min-h-10 text-sm leading-6 text-slate-500">{check.detail === 'ok' ? '连接正常，服务可用。' : check.detail}</p>
            <StatusChip className={check.ok ? 'mt-3 border-emerald-100 bg-emerald-50 text-emerald-700' : 'mt-3 border-rose-100 bg-rose-50 text-rose-700'}>
              {check.ok ? <CheckCircle2 className="mr-1.5 h-3.5 w-3.5" /> : <XCircle className="mr-1.5 h-3.5 w-3.5" />}
              {check.ok ? '正常' : '异常'}
            </StatusChip>
          </section>
        ))}
      </div>
    </div>
  );
}

export function AdminApp() {
  const [admin, setAdmin] = useState<AdminAccountResponse | null>(null);
  const [bootstrapping, setBootstrapping] = useState(true);
  const [tab, setTab] = useState<AdminTab>('dashboard');
  const [dashboard, setDashboard] = useState<AdminDashboardResponse | null>(null);
  const [jobs, setJobs] = useState<AdminJobRow[]>([]);
  const [users, setUsers] = useState<AdminUserRow[]>([]);
  const [selectedJob, setSelectedJob] = useState<AdminJobDetailResponse | null>(null);
  const [selectedUser, setSelectedUser] = useState<AdminUserUsageResponse | null>(null);
  const [jobEmailFilter, setJobEmailFilter] = useState('');
  const [jobStatusFilter, setJobStatusFilter] = useState('');
  const [userEmailFilter, setUserEmailFilter] = useState('');
  const [grantEmail, setGrantEmail] = useState('');
  const [notice, setNotice] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const canOperate = admin?.role === 'owner';

  async function refreshAll() {
    if (!admin) {
      return;
    }
    const [nextDashboard, nextJobs, nextUsers] = await Promise.all([
      getAdminDashboard(),
      getAdminJobs({email: jobEmailFilter || undefined, status: jobStatusFilter || undefined, limit: 80}),
      getAdminUsers({email: userEmailFilter || undefined, limit: 80}),
    ]);
    setDashboard(nextDashboard);
    setJobs(nextJobs.jobs);
    setUsers(nextUsers.users);
    setError(null);
  }

  function handleAdminError(err: unknown) {
    if (err instanceof UnauthorizedError) {
      setNotice(null);
      setError(getErrorMessage(err));
      return;
    }
    setError(getErrorMessage(err));
  }

  function handleAdminLogin(nextAdmin: AdminAccountResponse) {
    setError(null);
    setNotice(null);
    setAdmin(nextAdmin);
  }

  async function handleRefreshAll() {
    try {
      await refreshAll();
    } catch (err) {
      handleAdminError(err);
    }
  }

  useEffect(() => {
    let cancelled = false;
    async function bootstrap() {
      try {
        const session = await getAdminSession();
        if (!cancelled) {
          setAdmin(session.admin);
        }
      } catch {
        if (!cancelled) {
          setAdmin(null);
        }
      } finally {
        if (!cancelled) {
          setBootstrapping(false);
        }
      }
    }
    void bootstrap();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!admin) {
      return undefined;
    }
    let cancelled = false;
    const refresh = () => {
      void refreshAll().catch((err) => {
        if (!cancelled) {
          handleAdminError(err);
        }
      });
    };
    refresh();
    const interval = window.setInterval(refresh, 8000);
    return () => {
      cancelled = true;
      window.clearInterval(interval);
    };
  }, [admin, jobEmailFilter, jobStatusFilter, userEmailFilter]);

  const navItems = useMemo(
    () => [
      {id: 'dashboard' as const, label: '总览', icon: BarChart3},
      {id: 'jobs' as const, label: '任务', icon: Activity},
      {id: 'users' as const, label: '用户', icon: Users},
      {id: 'health' as const, label: '健康', icon: SlidersHorizontal},
    ],
    [],
  );

  async function handleLogout() {
    await adminLogout().catch(() => undefined);
    setAdmin(null);
  }

  async function runAction(action: () => Promise<unknown>, success: string) {
    setError(null);
    setNotice(null);
    try {
      await action();
      setNotice(success);
      await refreshAll();
    } catch (err) {
      handleAdminError(err);
    }
  }

  if (bootstrapping) {
    return <div className="flex min-h-screen items-center justify-center bg-slate-50 text-sm text-slate-500">正在加载后台...</div>;
  }

  if (!admin) {
    return <AdminLoginView onLogin={handleAdminLogin} />;
  }

  return (
    <div className="min-h-screen bg-slate-50 text-slate-900">
      <aside className="fixed inset-y-0 left-0 hidden w-64 border-r border-slate-200/80 bg-white/95 p-5 shadow-sm shadow-slate-200/50 lg:block">
        <div className="mb-8 flex items-center gap-3">
          <div className="flex h-10 w-10 items-center justify-center rounded-2xl bg-indigo-600 text-white shadow-sm shadow-indigo-200">
            <ShieldCheck className="h-5 w-5" />
          </div>
          <div>
            <div className="font-bold text-slate-950">SmartAnalyst</div>
            <div className="text-xs text-slate-500">后台管理系统</div>
          </div>
        </div>
        <nav className="space-y-2">
          {navItems.map((item) => {
            const Icon = item.icon;
            return (
              <button
                className={cn(
                  'flex w-full items-center gap-3 rounded-xl px-3 py-2.5 text-sm font-semibold transition',
                  tab === item.id
                    ? 'bg-slate-900 text-white shadow-sm shadow-slate-300/50'
                    : 'text-slate-600 hover:bg-indigo-50 hover:text-indigo-700',
                )}
                key={item.id}
                type="button"
                onClick={() => setTab(item.id)}
              >
                <Icon className="h-4 w-4" />
                {item.label}
              </button>
            );
          })}
        </nav>
      </aside>
      <main className="min-h-screen p-5 lg:pl-72">
        <header className={cn(ADMIN_CARD_CLASS, 'mb-6 flex flex-wrap items-center justify-between gap-4 px-5 py-3.5')}>
          <div>
            <div className="text-xs font-semibold tracking-[0.18em] text-slate-400">当前身份：{ROLE_LABELS[admin.role] || admin.role}</div>
            <h1 className="mt-1 text-xl font-bold tracking-tight text-slate-950">{admin.email}</h1>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <button className={ADMIN_BUTTON_CLASS} type="button" onClick={() => void handleRefreshAll()}>
              <RefreshCw className="h-4 w-4" />
              刷新
            </button>
            <button className={ADMIN_BUTTON_CLASS} type="button" onClick={() => void handleLogout()}>
              <LogOut className="h-4 w-4" />
              退出
            </button>
          </div>
        </header>

        <div className="mb-4 flex flex-wrap gap-2 lg:hidden">
          {navItems.map((item) => (
            <button className={cn('rounded-xl px-3 py-2 text-sm font-semibold shadow-sm transition', tab === item.id ? 'bg-slate-900 text-white' : 'bg-white text-slate-600')} key={item.id} type="button" onClick={() => setTab(item.id)}>
              {item.label}
            </button>
          ))}
        </div>

        {notice ? <div className="mb-4 rounded-2xl border border-emerald-100 bg-emerald-50 px-4 py-3 text-sm text-emerald-700">{notice}</div> : null}
        {error ? <div className="mb-4 rounded-2xl border border-rose-100 bg-rose-50 px-4 py-3 text-sm text-rose-700">{error}</div> : null}

        {tab === 'jobs' ? (
          <div className={cn(ADMIN_CARD_CLASS, 'mb-4 flex flex-wrap gap-3 p-4')}>
            <div className="relative">
              <Search className="pointer-events-none absolute left-3 top-2.5 h-4 w-4 text-slate-400" />
              <input className={cn(ADMIN_INPUT_CLASS, 'py-2 pl-9 pr-3')} value={jobEmailFilter} onChange={(event) => setJobEmailFilter(event.target.value)} placeholder="按邮箱筛选" />
            </div>
            <select className={ADMIN_INPUT_CLASS} value={jobStatusFilter} onChange={(event) => setJobStatusFilter(event.target.value)}>
              <option value="">全部状态</option>
              {(['queued_analysis', 'running_analysis', 'awaiting_selection', 'queued_render', 'rendering', 'completed', 'failed', 'expired'] satisfies JobStatus[]).map((status) => (
                <option key={status} value={status}>{JOB_STATUS_LABELS[status] || status}</option>
              ))}
            </select>
          </div>
        ) : null}

        {tab === 'users' ? (
          <div className={cn(ADMIN_CARD_CLASS, 'mb-4 flex flex-wrap gap-3 p-4')}>
            <div className="relative">
              <Search className="pointer-events-none absolute left-3 top-2.5 h-4 w-4 text-slate-400" />
              <input className={cn(ADMIN_INPUT_CLASS, 'py-2 pl-9 pr-3')} value={userEmailFilter} onChange={(event) => setUserEmailFilter(event.target.value)} placeholder="按邮箱筛选" />
            </div>
          </div>
        ) : null}

        {tab === 'dashboard' ? <DashboardView dashboard={dashboard} /> : null}
        {tab === 'jobs' ? (
          <JobsView
            jobs={jobs}
            selectedJob={selectedJob}
            canOperate={canOperate}
            onSelect={(jobId) => void getAdminJobDetail(jobId).then(setSelectedJob).then(() => setError(null)).catch(handleAdminError)}
            onRetry={(jobId) => void runAction(() => retryAdminJob(jobId), '已重新加入任务队列。')}
            onCancel={(jobId) => void runAction(() => cancelAdminJob(jobId), '已取消排队任务。')}
          />
        ) : null}
        {tab === 'users' ? (
          <UsersView
            users={users}
            selectedUser={selectedUser}
            canOperate={canOperate}
            grantEmail={grantEmail}
            onGrantEmailChange={setGrantEmail}
            onGrantViewer={() => void runAction(() => grantViewerAdmin(grantEmail.trim().toLowerCase()), '已授予观察员权限。').then(() => setGrantEmail(''))}
            onSelect={(userId) => void getAdminUserUsage(userId).then(setSelectedUser).then(() => setError(null)).catch(handleAdminError)}
            onToggleStatus={(user) => void runAction(() => updateAdminUserStatus(user.id, user.status === 'active' ? 'disabled' : 'active'), '用户状态已更新。')}
            onResendVerification={(userId) => void runAction(() => resendAdminUserVerification(userId), '验证邮件已发送。')}
            onSaveQuota={(userId, values) => {
              const daily = values.daily.trim() ? Number(values.daily) : null;
              const upload = values.uploadMb.trim() ? Number(values.uploadMb) * 1024 * 1024 : null;
              const active = values.active.trim() ? Number(values.active) : null;
              void runAction(
                () => updateAdminUserQuota(userId, {
                  daily_job_limit: Number.isFinite(daily) ? daily : null,
                  daily_upload_bytes_limit: Number.isFinite(upload) ? upload : null,
                  active_job_limit: Number.isFinite(active) ? active : null,
                }),
                '配额覆盖已保存。',
              ).then(() => void getAdminUserUsage(userId).then(setSelectedUser));
            }}
            emailVerificationActionsEnabled={EMAIL_VERIFICATION_ACTIONS_ENABLED}
          />
        ) : null}
        {tab === 'health' ? <HealthView dashboard={dashboard} /> : null}
      </main>
    </div>
  );
}
