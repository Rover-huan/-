import {type FormEvent, type ReactNode, useEffect, useMemo, useState} from 'react';
import {
  AlertCircle,
  BarChart3,
  ChevronDown,
  Check,
  CheckCircle2,
  Clock3,
  Copy,
  Database,
  Download,
  FileCode,
  FileText,
  Info,
  Loader2,
  LogIn,
  LogOut,
  RefreshCw,
  ShieldCheck,
  Sparkles,
  Trash2,
  Upload,
  UserPlus,
  XCircle,
} from 'lucide-react';
import {AnimatePresence, motion} from 'motion/react';
import {type DropzoneOptions, useDropzone} from 'react-dropzone';
import {sendPresenceHeartbeat} from '../lib/api';
import type {
  ArtifactType,
  JobArtifactResponse,
  JobEventResponse,
  JobStatus,
  JobSummaryResponse,
  JobTaskResponse,
  QuotaRemainingResponse,
} from '../lib/types';
import {cn} from '../lib/utils';
import {useJobRuntime} from './jobs/useJobRuntime';
import {useSession} from './session/useSession';
import type {AlertState, AuthDraft, AuthMode, NoticeTone} from './types';

type WorkflowStep = 'upload' | 'analysis' | 'selection' | 'export';
type ProgressModuleState = 'done' | 'active' | 'pending';
type ProgressModuleId = 'received' | 'planning' | 'charts' | 'report';

interface ProgressModule {
  id: ProgressModuleId;
  title: string;
  state: ProgressModuleState;
  subtitle: string;
  helperText: string;
  estimatedDuration?: string;
}

interface StageCopy {
  workflowLabel?: string;
  title?: string;
  estimatedDuration?: string;
  pendingHint: string;
  runningHint: string;
  completedHint: string;
  runningStatus?: string;
}

const STAGE_COPY: Record<ProgressModuleId | WorkflowStep, StageCopy> = {
  upload: {
    workflowLabel: '上传数据',
    pendingHint: '选择文件后即可提交分析。',
    runningHint: '正在上传文件并创建分析任务。',
    completedHint: '数据文件已提交。',
  },
  analysis: {
    workflowLabel: '分析数据',
    estimatedDuration: '预计 3–6 分钟',
    pendingHint: '上传数据后，系统将开始分析。',
    runningStatus: '分析中 · 预计 3–6 分钟',
    runningHint: '分析数据并生成候选图表，预计 3–6 分钟，请不要关闭页面。',
    completedHint: '分析已完成，候选图表已准备好。',
  },
  selection: {
    workflowLabel: '勾选候选图',
    pendingHint: '候选图生成后，你可以从中勾选进入报告。',
    runningHint: '等待你选择需要写入最终报告的候选图。',
    completedHint: '候选图选择已提交。',
  },
  export: {
    workflowLabel: '下载报告',
    estimatedDuration: '预计 3—5 分钟',
    pendingHint: '选择候选图后，系统将调用高质量模型撰写并优化最终报告，预计需要 3—5 分钟。',
    runningStatus: '生成中 · 预计 3—5 分钟',
    runningHint: '最终报告生成中，系统正在进行正文精修与文档导出，请耐心等待。',
    completedHint: '报告已生成，可以下载。',
  },
  received: {
    title: '任务已接收',
    pendingHint: '文件上传后，系统会自动创建分析任务。',
    runningHint: '正在等待 worker 接收任务。',
    completedHint: '任务已被 worker 接收，分析流程已经启动。',
  },
  planning: {
    title: '扫描数据与规划问题',
    estimatedDuration: '预计 3–6 分钟',
    pendingHint: '系统会读取文件、清洗数据、识别字段，并规划分析问题。',
    runningStatus: '进行中 · 预计 3–6 分钟',
    runningHint: '正在读取、清洗、规划，并生成候选图表。',
    completedHint: '数据分析与候选图表生成已完成。',
  },
  charts: {
    title: '勾选候选图',
    pendingHint: '候选图生成后，你可以选择需要写入报告的图表。',
    runningStatus: '等待你选择',
    runningHint: '请选择需要写入最终报告的候选图表，选择后继续生成报告。',
    completedHint: '候选图选择已提交。',
  },
  report: {
    title: '撰写并导出最终报告',
    estimatedDuration: '预计 3—5 分钟',
    pendingHint: '选择候选图后，系统将调用高质量模型撰写并优化最终报告，预计需要 3—5 分钟。',
    runningStatus: '进行中 · 预计 3—5 分钟',
    runningHint: '最终报告生成中，系统正在进行正文精修与文档导出，请耐心等待。',
    completedHint: '报告与产物已生成，可以直接下载。',
  },
};

const LIVE_PROGRESS_STATUSES = new Set<JobStatus>([
  'queued_analysis',
  'running_analysis',
  'queued_render',
  'rendering',
]);
const ACTIVE_JOB_STATUSES = new Set<JobStatus>([
  'queued_analysis',
  'running_analysis',
  'awaiting_selection',
  'queued_render',
  'rendering',
]);
const RUNNING_JOB_STATUSES = new Set<JobStatus>(['running_analysis', 'rendering']);
const CAPTCHA_ENABLED = import.meta.env.VITE_CAPTCHA_ENABLED === 'true';
const EMAIL_VERIFICATION_UI_ENABLED = import.meta.env.VITE_EMAIL_VERIFICATION_ENABLED === 'true';

const ACCEPTED_FILE_TYPES = {
  'text/csv': ['.csv'],
  'application/vnd.ms-excel': ['.xls'],
  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': ['.xlsx'],
};

const ARTIFACT_META: Record<
  ArtifactType,
  {title: string; description: string; defaultFilename: string; downloadLabel: string}
> = {
  zip: {
    title: '结果压缩包',
    description: '完整交付物打包，适合直接下载保存。',
    defaultFilename: 'SmartAnalyst_Result_Bundle.zip',
    downloadLabel: '下载 ZIP',
  },
  docx: {
    title: 'Word 报告',
    description: '包含完整正文、图表和目录的正式分析报告。',
    defaultFilename: 'SmartAnalyst_Report.docx',
    downloadLabel: '下载 DOCX',
  },
  pdf: {
    title: 'PDF 报告',
    description: '固定版式，适合分享和归档。',
    defaultFilename: 'SmartAnalyst_Report.pdf',
    downloadLabel: '下载 PDF',
  },
  ipynb: {
    title: 'Notebook',
    description: '包含完整代码和可复现分析过程。',
    defaultFilename: 'SmartAnalyst_Notebook.ipynb',
    downloadLabel: '下载 IPYNB',
  },
  txt: {
    title: '清洗摘要',
    description: '记录数据清洗、字段处理和异常处理过程。',
    defaultFilename: 'SmartAnalyst_Cleaning_Summary.txt',
    downloadLabel: '下载 TXT',
  },
};
const VISIBLE_ARTIFACT_TYPES = new Set<ArtifactType>(['docx', 'ipynb', 'txt']);

function parseBackendDateTime(value: string) {
  const normalized = value.trim();
  const hasExplicitTimezone = /(?:z|[+-]\d{2}:?\d{2})$/i.test(normalized);
  return new Date(hasExplicitTimezone ? normalized : `${normalized}Z`);
}

function formatDateTime(value: string | null) {
  if (!value) {
    return '未记录';
  }

  const date = parseBackendDateTime(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }

  const parts = new Intl.DateTimeFormat('zh-CN', {
    timeZone: 'Asia/Shanghai',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).formatToParts(date);
  const partMap = Object.fromEntries(parts.map((part) => [part.type, part.value]));

  return `${partMap.year}/${partMap.month}/${partMap.day} ${partMap.hour}:${partMap.minute}`;
}

function formatFileSize(size: number) {
  if (size < 1024) {
    return `${size} B`;
  }
  if (size < 1024 * 1024) {
    return `${(size / 1024).toFixed(1)} KB`;
  }
  if (size < 1024 * 1024 * 1024) {
    return `${(size / (1024 * 1024)).toFixed(1)} MB`;
  }
  return `${(size / (1024 * 1024 * 1024)).toFixed(1)} GB`;
}

function formatStatus(status: JobStatus) {
  switch (status) {
    case 'uploaded':
      return '已上传';
    case 'queued_analysis':
      return '等待分析';
    case 'running_analysis':
      return '分析中';
    case 'awaiting_selection':
      return '等待选图';
    case 'queued_render':
      return '等待生成';
    case 'rendering':
      return '生成中';
    case 'completed':
      return '已完成';
    case 'failed':
      return '失败';
    case 'expired':
      return '已过期';
    default:
      return status;
  }
}

function getWorkflowStep(job: JobSummaryResponse | null): WorkflowStep {
  if (!job) {
    return 'upload';
  }
  if (job.status === 'awaiting_selection') {
    return 'selection';
  }
  if (job.status === 'queued_render' || job.status === 'rendering' || job.status === 'completed') {
    return 'export';
  }
  return 'analysis';
}

function getEvents(events: JobEventResponse[]) {
  return [...events].slice(-8);
}

function getReadyChartCount(events: JobEventResponse[], tasks: JobTaskResponse[]) {
  const readyEvents = events.filter((event) => event.event_type === 'job.task_ready');
  return readyEvents.length > 0 ? readyEvents.length : tasks.length;
}

function getProgressModules(
  job: JobSummaryResponse,
  events: JobEventResponse[],
  tasks: JobTaskResponse[],
): ProgressModule[] {
  const readyChartCount = getReadyChartCount(events, tasks);
  const queuedHelper = job.queue_position
    ? `当前队列位置：第 ${job.queue_position} 位。${STAGE_COPY.analysis.runningHint}`
    : STAGE_COPY.analysis.runningHint;

  const modules: ProgressModule[] = [
    {
      id: 'received',
      title: STAGE_COPY.received.title || '任务已接收',
      state: 'pending',
      subtitle: '等待开始',
      helperText: STAGE_COPY.received.pendingHint,
    },
    {
      id: 'planning',
      title: STAGE_COPY.planning.title || '扫描数据与规划问题',
      state: 'pending',
      subtitle: '等待开始',
      helperText: STAGE_COPY.planning.pendingHint,
      estimatedDuration: STAGE_COPY.planning.estimatedDuration,
    },
    {
      id: 'charts',
      title: STAGE_COPY.charts.title || '勾选候选图',
      state: 'pending',
      subtitle: '等待开始',
      helperText: STAGE_COPY.charts.pendingHint,
      estimatedDuration: STAGE_COPY.charts.estimatedDuration,
    },
    {
      id: 'report',
      title: STAGE_COPY.report.title || '撰写并导出报告',
      state: 'pending',
      subtitle: '等待开始',
      helperText: STAGE_COPY.report.pendingHint,
      estimatedDuration: STAGE_COPY.report.estimatedDuration,
    },
  ];

  switch (job.status) {
    case 'uploaded':
    case 'queued_analysis':
      modules[0] = {
        ...modules[0],
        state: 'active',
        subtitle: '进行中',
        helperText: queuedHelper,
      };
      break;
    case 'running_analysis':
      modules[0] = {
        ...modules[0],
        state: 'done',
        subtitle: '已完成',
        helperText: STAGE_COPY.received.completedHint,
      };
      if (readyChartCount > 0) {
        modules[1] = {
          ...modules[1],
          state: 'active',
          subtitle: STAGE_COPY.planning.runningStatus || '进行中',
          helperText: `已生成 ${readyChartCount} 张候选图，系统仍在整理分析结果。`,
        };
      } else {
        modules[1] = {
          ...modules[1],
          state: 'active',
          subtitle: STAGE_COPY.planning.runningStatus || '进行中',
          helperText: STAGE_COPY.planning.runningHint,
        };
      }
      break;
    case 'awaiting_selection':
      modules[0] = {
        ...modules[0],
        state: 'done',
        subtitle: '已完成',
        helperText: STAGE_COPY.received.completedHint,
      };
      modules[1] = {
        ...modules[1],
        state: 'done',
        subtitle: '已完成',
        helperText: STAGE_COPY.planning.completedHint,
      };
      modules[2] = {
        ...modules[2],
        state: 'done',
        subtitle: '已完成',
        helperText: readyChartCount > 0 ? `候选图已全部准备完成，共 ${readyChartCount} 张` : '候选图已全部准备完成。',
      };
      break;
    case 'queued_render':
      modules[0] = {
        ...modules[0],
        state: 'done',
        subtitle: '已完成',
        helperText: STAGE_COPY.received.completedHint,
      };
      modules[1] = {
        ...modules[1],
        state: 'done',
        subtitle: '已完成',
        helperText: STAGE_COPY.planning.completedHint,
      };
      modules[2] = {
        ...modules[2],
        state: 'done',
        subtitle: '已完成',
        helperText: readyChartCount > 0 ? `候选图已准备完成，共 ${readyChartCount} 张` : '候选图已准备完成。',
      };
      modules[3] = {
        ...modules[3],
        state: 'active',
        subtitle: STAGE_COPY.report.runningStatus || '进行中',
        helperText: job.queue_position
          ? `渲染已排队，当前队列位置：第 ${job.queue_position} 位。${STAGE_COPY.report.pendingHint}`
          : STAGE_COPY.report.pendingHint,
      };
      break;
    case 'rendering':
      modules[0] = {
        ...modules[0],
        state: 'done',
        subtitle: '已完成',
        helperText: STAGE_COPY.received.completedHint,
      };
      modules[1] = {
        ...modules[1],
        state: 'done',
        subtitle: '已完成',
        helperText: STAGE_COPY.planning.completedHint,
      };
      modules[2] = {
        ...modules[2],
        state: 'done',
        subtitle: '已完成',
        helperText: readyChartCount > 0 ? `候选图已准备完成，共 ${readyChartCount} 张` : '候选图已准备完成。',
      };
      modules[3] = {
        ...modules[3],
        state: 'active',
        subtitle: STAGE_COPY.report.runningStatus || '进行中',
        helperText: STAGE_COPY.report.runningHint,
      };
      break;
    case 'completed':
      return modules.map((module) => ({
        ...module,
        state: 'done',
        subtitle: '已完成',
        helperText:
          module.id === 'charts' && readyChartCount > 0
            ? `候选图已准备完成，共 ${readyChartCount} 张`
            : module.id === 'report'
              ? STAGE_COPY.report.completedHint
              : '该阶段已完成。',
      }));
    default:
      return modules;
  }

  return modules;
}

function getStatusTone(status: JobStatus): string {
  switch (status) {
    case 'completed':
      return 'bg-emerald-50 text-emerald-700 border-emerald-200';
    case 'failed':
    case 'expired':
      return 'bg-rose-50 text-rose-700 border-rose-200';
    case 'awaiting_selection':
      return 'bg-amber-50 text-amber-700 border-amber-200';
    default:
      return 'bg-sky-50 text-sky-700 border-sky-200';
  }
}

function getFooterHint(job: JobSummaryResponse | null) {
  if (!job) {
    return '上传数据后，系统会自动开始分析，并在页面中展示每一步进展。';
  }
  if (job.status === 'awaiting_selection') {
    return '候选图已经准备好。选择需要写入报告的图表后，系统会继续生成报告。';
  }
  if (job.status === 'completed') {
    return '报告产物已准备完成。文件采用短期保留策略，到期后将自动清理。';
  }
  if (job.status === 'failed') {
    return '任务未能完成。你可以查看提示后重新提交，或稍后再试。';
  }
  if (job.status === 'expired') {
    return '该任务文件已过期清理。如需报告，请重新上传数据生成。';
  }
  return '任务正在稳定执行，请勿关闭页面。系统会在完成后自动进入下一步。';
}

function formatEventMessage(event: JobEventResponse) {
  const message = event.message.trim();
  const candidateChartMatch = message.match(/^Prepared candidate chart for task\s+(\d+)\.$/i);
  if (candidateChartMatch) {
    return `候选图表 ${candidateChartMatch[1]} 已生成`;
  }

  const eventMessageMap: Record<string, string> = {
    'Analysis complete; awaiting user chart selection.': '分析完成，等待选择图表',
    'Render task queued after user chart selection.': '报告生成任务已提交',
    'Render phase completed and artifacts were uploaded.': '报告已生成，可下载产物已准备完成',
    'Report generated.': '报告已生成',
    'Job created.': '任务已创建',
    'Upload received.': '数据已上传',
    'Analysis started.': '开始分析数据',
    'Analysis phase started.': '开始分析数据',
    'Analysis task queued.': '分析任务已提交',
    'Analysis complete.': '数据分析完成',
    'Render phase started.': '开始生成报告',
  };

  return eventMessageMap[message] || '流程已继续推进';
}

function getProgressStateLabel(state: ProgressModuleState) {
  if (state === 'done') {
    return '已完成';
  }
  if (state === 'active') {
    return '进行中';
  }
  return '等待开始';
}

function getProgressBrief(module: ProgressModule) {
  const briefs: Record<ProgressModuleId, string> = {
    received: '文件已进入分析队列。',
    planning: '读取、清洗数据并生成候选图表。',
    charts: '等待你选择写入报告的图表。',
    report: '正在进行最终报告撰写与正文精修，系统将调用高质量模型优化报告表达，预计需要 3—5 分钟。',
  };
  return briefs[module.id];
}

function formatDurationBadge(duration?: string) {
  return duration?.replace(/^预计\s*/, '') || '自动推进';
}

const PROCESSING_FALLBACK_MESSAGES = [
  '已进入数据扫描阶段',
  '正在清洗数据',
  '正在规划分析问题',
  '候选图表正在生成',
];

function formatProcessingEventMessage(event: JobEventResponse, fallbackIndex: number) {
  const message = formatEventMessage(event);
  if (message !== '流程已继续推进') {
    return message;
  }

  const rawText = `${event.event_type} ${event.message}`.toLowerCase();
  if (rawText.includes('queue') || rawText.includes('queued')) {
    return '分析任务已提交';
  }
  if (rawText.includes('upload') || rawText.includes('created')) {
    return '数据已进入处理流程';
  }
  if (rawText.includes('clean')) {
    return '正在清洗数据';
  }
  if (rawText.includes('plan') || rawText.includes('scan')) {
    return '正在规划分析问题';
  }
  if (rawText.includes('chart') || rawText.includes('task_ready')) {
    return '候选图表正在生成';
  }
  if (rawText.includes('analysis')) {
    return '正在分析数据';
  }

  return PROCESSING_FALLBACK_MESSAGES[fallbackIndex % PROCESSING_FALLBACK_MESSAGES.length];
}

function getCompactProcessingEvents(events: JobEventResponse[], limit = 3) {
  const compactEvents: Array<{event: JobEventResponse; message: string}> = [];
  const seenMessages = new Set<string>();
  let fallbackIndex = 0;

  for (const event of [...events].reverse()) {
    const message = formatProcessingEventMessage(event, fallbackIndex);
    fallbackIndex += 1;

    if (seenMessages.has(message)) {
      continue;
    }

    seenMessages.add(message);
    compactEvents.push({event, message});

    if (compactEvents.length >= limit) {
      break;
    }
  }

  return compactEvents.reverse();
}

function canDeleteJob(job: JobSummaryResponse) {
  return !RUNNING_JOB_STATUSES.has(job.status);
}

function getJobActionLabel(job: JobSummaryResponse) {
  if (job.status === 'completed' || job.status === 'failed' || job.status === 'expired') {
    return '删除任务';
  }
  return '取消并删除';
}

function getFailureStageLabel(stage?: string | null) {
  const normalized = (stage || '').toLowerCase();
  if (normalized.includes('render') || normalized.includes('export')) {
    return '生成报告';
  }
  if (normalized.includes('selection')) {
    return '勾选候选图';
  }
  if (normalized.includes('upload')) {
    return '上传数据';
  }
  if (normalized.includes('analysis') || normalized.includes('scanner') || normalized.includes('synthesizer')) {
    return '分析数据';
  }
  return '分析数据';
}

function getErrorTypeLabel(code?: string | null) {
  switch (code) {
    case 'executor_internal_error':
      return '内部处理异常';
    case 'data_empty':
      return '数据内容为空';
    case 'content_risk':
      return '内容安全风险';
    case 'unsupported_file_type':
      return '文件格式不支持';
    case 'analysis_timeout':
      return '分析超时';
    default:
      return '处理异常';
  }
}

function getActiveJobHint(job: JobSummaryResponse) {
  if (job.status === 'queued_analysis' || job.status === 'running_analysis') {
    return '正在分析数据并生成候选图表';
  }
  if (job.status === 'queued_render' || job.status === 'rendering') {
    return '最终报告生成中，系统正在进行正文精修与文档导出';
  }
  if (job.status === 'awaiting_selection') {
    return '候选图已生成，等待勾选图表';
  }
  return formatStatus(job.status);
}

function getWorkflowStepStatusText(step: WorkflowStep, job: JobSummaryResponse | null) {
  if (!job) {
    return null;
  }
  if (job.status === 'failed' && step === 'analysis') {
    return '失败';
  }
  if (step === 'analysis' && ['uploaded', 'queued_analysis', 'running_analysis'].includes(job.status)) {
    return STAGE_COPY.analysis.runningStatus || formatStatus(job.status);
  }
  if (step === 'selection' && job.status === 'awaiting_selection') {
    return '等待你选择';
  }
  if (step === 'export' && ['queued_render', 'rendering'].includes(job.status)) {
    return STAGE_COPY.export.runningStatus || formatStatus(job.status);
  }
  return formatStatus(job.status);
}

function getWorkflowStepHint(step: WorkflowStep, job: JobSummaryResponse | null) {
  if (!job) {
    return null;
  }
  if (job.status === 'failed' && step === 'analysis') {
    return '分析阶段失败。请查看右侧错误详情。';
  }
  if (step === 'analysis' && ['uploaded', 'queued_analysis', 'running_analysis'].includes(job.status)) {
    return STAGE_COPY.analysis.runningHint;
  }
  if (step === 'selection' && job.status === 'awaiting_selection') {
    return '候选图生成后，选择需要写入报告的图表。';
  }
  if (step === 'export' && job.status === 'queued_render') {
    return STAGE_COPY.export.pendingHint;
  }
  if (step === 'export' && job.status === 'rendering') {
    return STAGE_COPY.export.runningHint;
  }
  return null;
}

function getCurrentStageStatusText(job: JobSummaryResponse | null) {
  if (!job) {
    return '等待上传文件';
  }
  if (['uploaded', 'queued_analysis', 'running_analysis'].includes(job.status)) {
    return '分析中 · 预计 3–6 分钟';
  }
  if (job.status === 'awaiting_selection') {
    return '等待你选择';
  }
  if (job.status === 'queued_render' || job.status === 'rendering') {
    return '最终报告生成中 · 预计 3—5 分钟';
  }
  if (job.status === 'completed') {
    return '已完成';
  }
  if (job.status === 'failed') {
    return '失败 · 请查看详情';
  }
  if (job.status === 'expired') {
    return '已过期';
  }
  return formatStatus(job.status);
}

function getCurrentStageChipTone(job: JobSummaryResponse | null) {
  if (!job) {
    return 'border-slate-200 bg-slate-50 text-slate-600';
  }
  if (job.status === 'completed') {
    return 'border-emerald-100 bg-emerald-50 text-emerald-700';
  }
  if (job.status === 'failed' || job.status === 'expired') {
    return 'border-rose-100 bg-rose-50 text-rose-700';
  }
  if (job.status === 'awaiting_selection') {
    return 'border-cyan-100 bg-cyan-50 text-cyan-700';
  }
  return 'border-sky-100 bg-sky-50 text-sky-700';
}

function AlertBanner({alert}: {alert: AlertState}) {
  const toneStyles: Record<NoticeTone, string> = {
    info: 'border-sky-200 bg-sky-50 text-sky-800',
    success: 'border-emerald-200 bg-emerald-50 text-emerald-800',
    error: 'border-rose-200 bg-rose-50 text-rose-800',
  };

  return (
    <div className={cn('min-w-0 whitespace-pre-wrap break-words rounded-2xl border px-4 py-3 text-sm leading-6', toneStyles[alert.tone])}>
      {alert.text}
    </div>
  );
}

function buildFailureTechnicalText(job: JobSummaryResponse, events: JobEventResponse[], canViewRaw: boolean) {
  const recentEvents = getEvents(events).map((event) => ({
    created_at: event.created_at,
    level: event.level,
    event_type: event.event_type,
    message: event.message,
    payload_json: canViewRaw ? event.payload_json : null,
  }));

  return JSON.stringify(
    {
      task_id: job.id,
      stage: job.failure_stage || job.phase || null,
      error_code: job.error_code || job.failure_code || 'unknown',
      error_category: job.error_category || 'unknown',
      raw_message: canViewRaw ? job.raw_detail || job.error_summary : null,
      recent_events: recentEvents,
    },
    null,
    2,
  );
}

function FailureNoticeCard({job, events}: {job: JobSummaryResponse; events: JobEventResponse[]}) {
  const [expanded, setExpanded] = useState(false);
  const [copied, setCopied] = useState(false);
  const canViewRaw = Boolean(job.raw_detail);
  const rawErrorCode = job.error_code || job.failure_code || 'unknown';
  const failureStage = getFailureStageLabel(job.failure_stage || job.phase);
  const errorType = getErrorTypeLabel(rawErrorCode);
  const latestFailureEvent = [...events].reverse().find((event) => event.level === 'error') || events[events.length - 1];
  const possibleReasons = ['文件格式不受支持', '文件内容为空或结构异常', '当前分析服务处理异常'];
  const suggestedActions = ['请稍后重试', '如多次失败，请重新上传文件', '如仍失败，请联系管理员并提供任务 ID'];
  const technicalText = useMemo(
    () => buildFailureTechnicalText(job, events, canViewRaw),
    [job, events, canViewRaw],
  );

  async function copyDetails() {
    if (typeof navigator === 'undefined' || !navigator.clipboard) {
      return;
    }
    await navigator.clipboard.writeText(technicalText);
    setCopied(true);
    window.setTimeout(() => setCopied(false), 1400);
  }

  return (
    <section className="min-w-0 rounded-3xl border border-rose-100 bg-white p-5 text-slate-900 shadow-sm shadow-rose-100/70">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
        <div className="min-w-0">
          <div className="inline-flex items-center gap-2 rounded-full bg-rose-50 px-3 py-1 text-xs font-bold text-rose-700">
            <AlertCircle className="h-3.5 w-3.5" />
            错误类型：{errorType}
          </div>
          <h3 className="mt-3 text-xl font-bold tracking-tight text-slate-950">{job.error_title || '分析任务未完成'}</h3>
          <p className="mt-2 whitespace-pre-wrap break-words text-sm leading-7 text-slate-600">
            {job.user_message || job.error_summary || '系统在处理数据时遇到异常，本次任务未完成。'}
          </p>
        </div>
        <div className="inline-flex w-fit rounded-2xl border border-rose-100 bg-rose-50/70 px-3 py-2 text-xs font-semibold text-rose-700">
          任务未完成
        </div>
      </div>

      <div className="mt-5 grid gap-3 rounded-2xl border border-rose-100 bg-rose-50/45 p-4 md:grid-cols-3">
        <div>
          <div className="text-xs font-bold tracking-[0.18em] text-rose-400">失败阶段</div>
          <div className="mt-1 text-sm font-semibold text-slate-800">{failureStage}</div>
        </div>
        <div>
          <div className="text-xs font-bold tracking-[0.18em] text-rose-400">错误时间</div>
          <div className="mt-1 text-sm font-semibold text-slate-800">
            {formatDateTime(latestFailureEvent?.created_at || job.finished_at || job.started_at || job.created_at)}
          </div>
        </div>
        <div>
          <div className="text-xs font-bold tracking-[0.18em] text-rose-400">任务 ID</div>
          <div className="mt-1 break-all font-mono text-xs font-semibold text-slate-700">{job.id}</div>
        </div>
      </div>

      <div className="mt-5 grid gap-4 md:grid-cols-2">
        <div className="rounded-2xl border border-slate-100 bg-slate-50/70 p-4">
          <div className="text-sm font-bold text-slate-900">可能原因</div>
          <ul className="mt-3 list-disc space-y-2 pl-5 text-sm leading-6 text-slate-600">
            {possibleReasons.map((reason) => (
              <li className="break-words" key={reason}>{reason}</li>
            ))}
          </ul>
        </div>
        <div className="rounded-2xl border border-slate-100 bg-slate-50/70 p-4">
          <div className="text-sm font-bold text-slate-900">建议操作</div>
          <ul className="mt-3 list-disc space-y-2 pl-5 text-sm leading-6 text-slate-600">
            {suggestedActions.map((action) => (
              <li className="break-words" key={action}>{action}</li>
            ))}
          </ul>
        </div>
      </div>

      <div className="mt-5 rounded-2xl border border-slate-200 bg-white">
        <button
          type="button"
          className="flex w-full items-center justify-between gap-3 px-4 py-3 text-left text-sm font-bold text-slate-800"
          onClick={() => setExpanded((value) => !value)}
        >
          <span>错误详情</span>
          <ChevronDown className={cn('h-4 w-4 shrink-0 transition', expanded ? 'rotate-180' : '')} />
        </button>
        {expanded ? (
          <div className="border-t border-slate-100 p-4">
            {!canViewRaw ? (
              <p className="mb-3 text-sm leading-6 text-slate-600">
                当前账号不显示完整技术错误。请将任务 ID 提供给管理员进行排查。
              </p>
            ) : null}
            <div className="mb-3 rounded-xl bg-slate-50 px-3 py-2 text-xs text-slate-500">
              原始错误码：<span className="font-mono text-slate-700">{rawErrorCode}</span>
            </div>
            <div className="mb-3 flex justify-end">
              <button
                type="button"
                className="inline-flex items-center gap-2 rounded-xl border border-slate-200 px-3 py-2 text-xs font-bold text-slate-700 transition hover:bg-slate-50"
                onClick={() => void copyDetails()}
              >
                <Copy className="h-3.5 w-3.5" />
                {copied ? '已复制' : '复制详情'}
              </button>
            </div>
            <pre className="max-h-80 min-w-0 overflow-auto whitespace-pre-wrap break-words rounded-2xl bg-slate-950 p-4 text-xs leading-6 text-slate-100">
              <code>{technicalText}</code>
            </pre>
          </div>
        ) : null}
      </div>
    </section>
  );
}

function LiveDots({tone = 'sky'}: {tone?: 'sky' | 'indigo'}) {
  const toneClass = tone === 'indigo' ? 'bg-indigo-500' : 'bg-sky-500';

  return (
    <div className="inline-flex items-center gap-1.5">
      {[0, 1, 2].map((index) => (
        <motion.span
          key={index}
          className={cn('h-1.5 w-1.5 rounded-full', toneClass)}
          animate={{opacity: [0.35, 1, 0.35], y: [0, -2, 0], scale: [0.9, 1.1, 0.9]}}
          transition={{duration: 1.05, repeat: Infinity, delay: index * 0.14, ease: 'easeInOut'}}
        />
      ))}
    </div>
  );
}

function ProgressSheen({
  className,
  duration = 1.8,
}: {
  className?: string;
  duration?: number;
}) {
  return (
    <motion.div
      aria-hidden="true"
      className={cn(
        'pointer-events-none absolute inset-y-0 -left-32 w-32 bg-gradient-to-r from-transparent via-white/85 to-transparent',
        className,
      )}
      animate={{x: ['0%', '460%']}}
      transition={{duration, ease: 'easeInOut', repeat: Infinity}}
    />
  );
}

function FeatureCard({
  icon,
  title,
  description,
}: {
  icon: ReactNode;
  title: string;
  description: string;
}) {
  return (
    <div className="rounded-2xl border border-white/10 bg-white/5 p-4 backdrop-blur-sm">
      <div className="mb-3 inline-flex h-10 w-10 items-center justify-center rounded-xl bg-white/10 text-indigo-100">
        {icon}
      </div>
      <div className="text-sm font-semibold">{title}</div>
      <div className="mt-2 text-xs leading-6 text-slate-300">{description}</div>
    </div>
  );
}

function StatusBadge({status}: {status: JobStatus}) {
  return (
    <span
      className={cn(
        'inline-flex min-h-7 flex-shrink-0 items-center whitespace-nowrap rounded-full border px-3 py-1 text-xs font-bold leading-5',
        getStatusTone(status),
      )}
    >
      {formatStatus(status)}
    </span>
  );
}

function EventList({events}: {events: JobEventResponse[]}) {
  const visibleEvents = getEvents(events);
  if (!visibleEvents.length) {
    return (
      <div className="rounded-2xl border border-dashed border-slate-200 bg-slate-50 px-4 py-8 text-sm text-slate-400">
        暂无更多事件信息。
      </div>
    );
  }

  return (
    <div className="space-y-3">
      {visibleEvents.map((event) => (
        <div key={event.id} className="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3">
          <div className="whitespace-pre-wrap break-words text-sm font-semibold text-slate-800">
            {formatEventMessage(event)}
          </div>
          <div className="mt-1 text-xs text-slate-400">{formatDateTime(event.created_at)}</div>
        </div>
      ))}
    </div>
  );
}

function ProcessingEventTimeline({events}: {events: JobEventResponse[]}) {
  const timelineEvents = getCompactProcessingEvents(events);
  if (!timelineEvents.length) {
    return (
      <div className="rounded-2xl border border-dashed border-slate-200 bg-slate-50 px-4 py-8 text-sm text-slate-400">
        任务动态会在分析开始后自动更新。
      </div>
    );
  }

  return (
    <div className="relative">
      <div className="absolute bottom-4 left-[0.45rem] top-4 w-px bg-slate-200" />
      <div className="space-y-3">
        {timelineEvents.map(({event, message}, index) => {
          const isLatest = index === timelineEvents.length - 1;

          return (
            <div key={event.id} className="relative pl-7">
              <span
                className={cn(
                  'absolute left-0 top-3 h-2.5 w-2.5 rounded-full ring-4',
                  isLatest ? 'bg-sky-500 ring-sky-100' : 'bg-slate-300 ring-white',
                )}
              />
              <div
                className={cn(
                  'rounded-2xl border px-4 py-3',
                  isLatest ? 'border-sky-100 bg-sky-50/80' : 'border-slate-100 bg-slate-50/80',
                )}
              >
                <div className={cn('text-sm font-semibold', isLatest ? 'text-sky-800' : 'text-slate-700')}>
                  {message}
                </div>
                <div className="mt-1 text-xs tabular-nums text-slate-400">{formatDateTime(event.created_at)}</div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function QuotaPanel({quota}: {quota: QuotaRemainingResponse | null}) {
  if (!quota) {
    return (
      <div className="rounded-2xl border border-slate-200 bg-white/80 p-4">
        <div className="text-xs font-bold tracking-[0.2em] text-slate-400">使用额度</div>
        <div className="mt-2 text-xs leading-5 text-slate-500">登录后显示任务、上传与并发额度。</div>
      </div>
    );
  }

  return (
    <div className="rounded-2xl border border-slate-200 bg-white/75 p-3.5 shadow-sm shadow-slate-200/30">
      <div className="flex items-center justify-between gap-3">
        <div className="text-xs font-bold tracking-[0.2em] text-slate-400">使用额度</div>
        <div className="rounded-full bg-slate-100 px-2.5 py-1 text-[11px] font-semibold text-slate-500">今日</div>
      </div>
      <div className="mt-3 space-y-2 text-xs text-slate-500">
        <div className="grid grid-cols-[auto_1fr] items-center gap-3">
          <span>剩余任务</span>
          <span className="text-right font-semibold tabular-nums text-slate-700">{quota.jobs_remaining} / {quota.daily_job_limit}</span>
        </div>
        <div className="grid grid-cols-[auto_1fr] items-center gap-3">
          <span>上传额度</span>
          <span className="text-right font-semibold tabular-nums text-slate-700">
            {formatFileSize(quota.upload_bytes_remaining)} / {formatFileSize(quota.daily_upload_bytes_limit)}
          </span>
        </div>
        <div className="grid grid-cols-[auto_1fr] items-center gap-3">
          <span>并发任务</span>
          <span className="text-right font-semibold tabular-nums text-slate-700">{quota.active_jobs_remaining} / {quota.active_job_limit}</span>
        </div>
      </div>
    </div>
  );
}

function ActiveJobsPanel({
  jobs,
  currentJobId,
  deletingJobId,
  maxItems,
  onSelect,
  onDelete,
}: {
  jobs: JobSummaryResponse[];
  currentJobId: string | null;
  deletingJobId: string | null;
  maxItems: number;
  onSelect: (jobId: string) => void;
  onDelete: (jobId: string) => void;
}) {
  const visibleJobs = jobs.slice(0, Math.max(maxItems, 1));

  return (
    <div className="rounded-[22px] border border-slate-200/70 bg-white/80 p-4 shadow-sm shadow-slate-200/35">
      <div className="flex items-center justify-between gap-3">
        <div>
          <div className="text-xs font-bold tracking-[0.2em] text-slate-400">活跃任务</div>
          <div className="mt-1 text-sm font-semibold text-slate-900">当前分析任务</div>
        </div>
        <div className="rounded-full border border-cyan-100 bg-cyan-50 px-2.5 py-1 text-xs font-semibold text-cyan-700">
          分析中
        </div>
      </div>

      <div className="mt-3 max-h-[18rem] space-y-2 overflow-y-auto pr-1">
        {visibleJobs.length ? (
          visibleJobs.map((job) => {
            const deleting = deletingJobId === job.id;
            const deleteDisabled = deleting || !canDeleteJob(job);
            const selected = job.id === currentJobId;

            return (
              <div
                key={job.id}
                className={cn(
                  'group relative overflow-hidden rounded-[18px] border px-3.5 py-3 transition',
                  selected
                    ? 'border-indigo-100 bg-indigo-50/70 shadow-sm shadow-indigo-100/60'
                    : 'border-slate-100 bg-slate-50/50 hover:border-slate-200 hover:bg-white',
                )}
              >
                <div
                  className={cn(
                    'absolute inset-y-3 left-0 w-1 rounded-r-full transition',
                    selected ? 'bg-indigo-500' : 'bg-transparent group-hover:bg-slate-300',
                  )}
                />
                <div className="flex items-start justify-between gap-3 pl-1">
                  <div className="min-w-0">
                    <div className="truncate text-sm font-semibold text-slate-800">
                      {selected ? '当前分析任务' : '智能分析任务'}
                    </div>
                    <div className="mt-1 flex items-center gap-1.5 text-xs text-slate-400">
                      <Clock3 className="h-3.5 w-3.5" />
                      {formatDateTime(job.created_at)}
                    </div>
                  </div>
                  <span className="inline-flex min-h-7 flex-shrink-0 items-center gap-1.5 whitespace-nowrap rounded-full border border-indigo-100 bg-indigo-50 px-3 py-1 text-xs font-bold leading-5 text-indigo-700">
                    分析中
                    {LIVE_PROGRESS_STATUSES.has(job.status) ? <LiveDots tone="indigo" /> : null}
                  </span>
                </div>

                <div className="mt-3 pl-1 text-xs leading-5 text-slate-500">{getActiveJobHint(job)}</div>

                <div className="mt-3 flex items-center justify-between gap-2 pl-1">
                  <button
                    type="button"
                    className={cn(
                      'inline-flex items-center justify-center gap-2 rounded-full px-3 py-1.5 text-xs font-semibold transition',
                      selected
                        ? 'bg-white text-indigo-700 shadow-sm shadow-indigo-100'
                        : 'bg-white text-slate-600 ring-1 ring-slate-200 hover:text-slate-900 hover:ring-slate-300',
                    )}
                    onClick={() => onSelect(job.id)}
                  >
                    {selected ? '当前任务' : '继续处理'}
                  </button>

                  {!selected ? (
                    <button
                      type="button"
                      className={cn(
                        'inline-flex h-8 w-8 items-center justify-center rounded-full opacity-0 transition group-hover:opacity-100',
                        deleteDisabled
                          ? 'cursor-not-allowed bg-slate-100 text-slate-300'
                          : 'bg-white text-slate-400 ring-1 ring-slate-200 hover:bg-rose-50 hover:text-rose-600 hover:ring-rose-200',
                      )}
                      onClick={() => {
                        if (!deleteDisabled && window.confirm('确认删除这个任务吗？删除后将释放当前占用的任务名额。')) {
                          onDelete(job.id);
                        }
                      }}
                      disabled={deleteDisabled}
                      title={canDeleteJob(job) ? getJobActionLabel(job) : '当前阶段暂不支持强制中断'}
                    >
                      {deleting ? <Loader2 className="h-4 w-4 animate-spin" /> : <Trash2 className="h-4 w-4" />}
                      <span className="sr-only">{canDeleteJob(job) ? getJobActionLabel(job) : '处理中'}</span>
                    </button>
                  ) : (
                    <span className="text-[11px] font-medium text-slate-300">进行中</span>
                  )}
                </div>
              </div>
            );
          })
        ) : (
          <div className="rounded-[18px] border border-dashed border-slate-200 bg-slate-50/70 px-4 py-7 text-center text-sm leading-6 text-slate-400">
            当前没有占用配额的活跃任务。
          </div>
        )}
      </div>
    </div>
  );
}

function RecentJobsPanel({
  jobs,
  currentJobId,
  deletingJobId,
  onSelect,
  onDelete,
}: {
  jobs: JobSummaryResponse[];
  currentJobId: string | null;
  deletingJobId: string | null;
  onSelect: (jobId: string) => void;
  onDelete: (jobId: string) => void;
}) {
  return (
    <ManagedRecentJobsPanel
      jobs={jobs}
      currentJobId={currentJobId}
      deletingJobId={deletingJobId}
      onSelect={onSelect}
      onDelete={onDelete}
    />
  );
}

function ManagedRecentJobsPanel({
  jobs,
  currentJobId,
  deletingJobId,
  onSelect,
  onDelete,
}: {
  jobs: JobSummaryResponse[];
  currentJobId: string | null;
  deletingJobId: string | null;
  onSelect: (jobId: string) => void;
  onDelete: (jobId: string) => void;
}) {
  return (
    <div className="rounded-[24px] border border-slate-200/80 bg-white/90 p-4 shadow-sm shadow-slate-200/70">
      <div className="flex items-center justify-between gap-3">
        <div>
          <div className="text-xs font-bold tracking-[0.2em] text-slate-400">历史任务</div>
          <div className="mt-1 text-sm font-semibold text-slate-900">历史任务</div>
        </div>
        <div className="rounded-full bg-slate-100 px-2.5 py-1 text-xs font-semibold text-slate-500">
          {jobs.length} 条记录
        </div>
      </div>
      <div className="mt-4 max-h-[18rem] space-y-2 overflow-y-auto pr-1">
        {jobs.length ? (
          jobs.map((job) => {
            const deleting = deletingJobId === job.id;
            const deleteDisabled = deleting || !canDeleteJob(job);
            const selected = job.id === currentJobId;

            return (
              <div
                key={job.id}
                className={cn(
                  'group relative overflow-hidden rounded-[18px] border px-3.5 py-3 transition',
                  selected
                    ? 'border-indigo-200 bg-indigo-50/85 shadow-sm shadow-indigo-100/80'
                    : 'border-slate-200 bg-slate-50/70 hover:border-slate-300 hover:bg-white',
                )}
              >
                <div
                  className={cn(
                    'absolute inset-y-3 left-0 w-1 rounded-r-full transition',
                    selected ? 'bg-indigo-500' : 'bg-transparent group-hover:bg-slate-300',
                  )}
                />
                <div className="flex items-start justify-between gap-3 pl-1">
                  <div className="min-w-0">
                    <div className="truncate text-sm font-semibold text-slate-800">
                      {job.report_title || '数据分析任务'}
                    </div>
                    <div className="mt-1 flex items-center gap-1.5 text-xs text-slate-400">
                      <Clock3 className="h-3.5 w-3.5" />
                      {formatDateTime(job.created_at)}
                    </div>
                    <div className="mt-1 text-[11px] leading-4 text-slate-300">任务编号：{job.id.slice(0, 8)}</div>
                  </div>
                  <StatusBadge status={job.status} />
                </div>

                <div className="mt-3 h-1.5 overflow-hidden rounded-full bg-slate-200/80">
                  <div
                    className={cn(
                      'h-full rounded-full transition-all',
                      LIVE_PROGRESS_STATUSES.has(job.status)
                        ? 'bg-gradient-to-r from-indigo-500 via-sky-500 to-cyan-500'
                        : job.status === 'completed'
                          ? 'bg-emerald-500'
                          : job.status === 'failed'
                            ? 'bg-rose-500'
                            : 'bg-indigo-500',
                    )}
                    style={{width: `${Math.max(job.progress_percent, LIVE_PROGRESS_STATUSES.has(job.status) ? 8 : 0)}%`}}
                  />
                </div>

                <div className="mt-3 flex items-center justify-between gap-2 pl-1">
                  <button
                    type="button"
                    className={cn(
                      'inline-flex items-center justify-center gap-2 rounded-full px-3 py-1.5 text-xs font-semibold transition',
                      selected
                        ? 'bg-white text-indigo-700 shadow-sm shadow-indigo-100'
                        : 'bg-white text-slate-600 ring-1 ring-slate-200 hover:text-slate-900 hover:ring-slate-300',
                    )}
                    onClick={() => onSelect(job.id)}
                  >
                    {selected ? '当前任务' : '查看任务'}
                  </button>

                  <button
                    type="button"
                    className={cn(
                      'inline-flex h-8 w-8 items-center justify-center rounded-full transition',
                      deleteDisabled
                        ? 'cursor-not-allowed bg-slate-100 text-slate-300'
                        : 'bg-white text-slate-400 ring-1 ring-slate-200 hover:bg-rose-50 hover:text-rose-600 hover:ring-rose-200',
                    )}
                    onClick={() => {
                      if (!deleteDisabled && window.confirm('确认删除这个任务吗？删除后将从列表中移除，并释放可用配额。')) {
                        onDelete(job.id);
                      }
                    }}
                    disabled={deleteDisabled}
                    title={canDeleteJob(job) ? getJobActionLabel(job) : '当前阶段暂不支持强制中断'}
                  >
                    {deleting ? <Loader2 className="h-4 w-4 animate-spin" /> : <Trash2 className="h-4 w-4" />}
                    <span className="sr-only">{canDeleteJob(job) ? getJobActionLabel(job) : '处理中'}</span>
                  </button>
                </div>
              </div>
            );
          })
        ) : (
          <div className="rounded-[18px] border border-dashed border-slate-200 bg-slate-50/70 px-4 py-7 text-center text-sm leading-6 text-slate-400">
            暂无历史任务
          </div>
        )}
      </div>
    </div>
  );
}

function ProgressSidebar({
  currentStep,
  currentJob,
  recentJobs,
  currentJobId,
  deletingJobId,
  onSelectJob,
  onDeleteJob,
}: {
  currentStep: WorkflowStep;
  currentJob: JobSummaryResponse | null;
  recentJobs: JobSummaryResponse[];
  currentJobId: string | null;
  deletingJobId: string | null;
  onSelectJob: (jobId: string) => void;
  onDeleteJob: (jobId: string) => void;
}) {
  const steps = (['upload', 'analysis', 'selection', 'export'] satisfies WorkflowStep[]).map((id) => ({
    id,
    label: STAGE_COPY[id].workflowLabel || id,
  }));
  const currentIndex = steps.findIndex((step) => step.id === currentStep);
  const quota = currentJob?.quota_remaining || recentJobs[0]?.quota_remaining || null;
  const jobIsLive = currentJob ? LIVE_PROGRESS_STATUSES.has(currentJob.status) : false;
  const jobIsCompleted = currentJob?.status === 'completed';
  const jobIsFailed = currentJob?.status === 'failed';
  const activeJobs = recentJobs
    .filter((job) => ACTIVE_JOB_STATUSES.has(job.status))
    .sort((a, b) => Number(b.id === currentJobId) - Number(a.id === currentJobId));
  const historyJobs = recentJobs.filter((job) => job.id !== currentJobId && !ACTIVE_JOB_STATUSES.has(job.status));
  const activeJobLimit = quota?.active_job_limit || 2;
  const currentJobCanDelete = currentJob ? canDeleteJob(currentJob) : false;
  const currentJobDeleting = currentJob ? deletingJobId === currentJob.id : false;
  const currentStepLabel = steps[currentIndex]?.label || '分析流程';
  const currentStageStatusText = getCurrentStageStatusText(currentJob);
  const currentStageChipTone = getCurrentStageChipTone(currentJob);
  const showCurrentJobCard = true;
  const showActiveJobsPanel = !jobIsCompleted && !jobIsFailed && activeJobs.length > 0;
  const showQuotaPanel = !jobIsCompleted && !jobIsFailed;
  const showRecentJobsPanel = true;

  return (
    <aside className="flex min-h-0 w-[21.5rem] flex-shrink-0 flex-col gap-5 overflow-y-auto border-r border-slate-200 bg-slate-50/80 p-5 pr-4">
      <div>
        <p className="text-xs font-bold tracking-[0.25em] text-slate-400">分析流程</p>
        <h2 className="mt-2 text-lg font-bold text-slate-900">报告生成流程</h2>
      </div>

      {showCurrentJobCard ? (
        <motion.div
          initial={false}
          animate={jobIsLive ? {boxShadow: ['0 0 0 rgba(56,189,248,0)', '0 10px 28px rgba(56,189,248,0.1)', '0 0 0 rgba(56,189,248,0)']} : {boxShadow: '0 0 0 rgba(56,189,248,0)'}}
          transition={jobIsLive ? {duration: 2.2, repeat: Infinity, ease: 'easeInOut'} : {duration: 0.2}}
          className={cn(
            'group relative h-auto min-h-[7rem] rounded-[22px] border bg-white/90 px-4 py-4 shadow-sm shadow-slate-200/50',
            jobIsLive ? 'border-sky-200' : 'border-slate-200/80',
            jobIsCompleted && 'border-emerald-100',
            jobIsFailed && 'border-rose-100',
          )}
        >
          <div className="text-xs font-bold tracking-[0.2em] text-slate-400">当前阶段</div>
          <div className="mt-2 whitespace-normal break-words text-base font-semibold leading-normal text-slate-900">
            {currentStepLabel}
          </div>
          <div className="mt-2 flex flex-wrap items-center gap-2">
            <span
              className={cn(
                'inline-flex max-w-full items-center gap-2 whitespace-normal rounded-full border px-3 py-1.5 text-xs font-semibold leading-normal',
                currentStageChipTone,
              )}
            >
              <span className="break-words">{currentStageStatusText}</span>
              {jobIsLive ? <LiveDots /> : null}
            </span>
          </div>

          {currentJob ? (
            <div className="mt-4 flex items-center justify-between gap-3 border-t border-slate-100 pt-3">
              <div className="min-w-0 text-[11px] leading-relaxed text-slate-300">
                任务编号：<span className="break-all font-mono">{currentJob.id.slice(0, 8)}</span>
              </div>
              {!jobIsLive && currentJobCanDelete ? (
                <button
                  type="button"
                  className="inline-flex h-8 w-8 flex-shrink-0 items-center justify-center rounded-full bg-white text-slate-400 opacity-0 ring-1 ring-slate-200 transition hover:bg-rose-50 hover:text-rose-600 hover:ring-rose-200 group-hover:opacity-100"
                  onClick={() => {
                    if (currentJob && window.confirm('确认删除这个当前任务吗？删除后将释放可用任务名额。')) {
                      onDeleteJob(currentJob.id);
                    }
                  }}
                  title={getJobActionLabel(currentJob)}
                >
                  {currentJobDeleting ? <Loader2 className="h-4 w-4 animate-spin" /> : <Trash2 className="h-4 w-4" />}
                  <span className="sr-only">{getJobActionLabel(currentJob)}</span>
                </button>
              ) : (
                <span className="text-[11px] font-medium text-slate-300">{jobIsLive ? '进行中' : null}</span>
              )}
            </div>
          ) : (
            <div className="mt-3 text-xs leading-relaxed text-slate-500">选择文件后即可开始分析。</div>
          )}
        </motion.div>
      ) : null}

      <div className="order-last space-y-4">
        {steps.map((step, index) => {
          const completed = currentIndex > index;
          const active = currentIndex === index;
          const activeLive = active && jobIsLive;
          const activeFailed = active && jobIsFailed;
          const activeStatusText = active ? getWorkflowStepStatusText(step.id, currentJob) : null;
          const activeHint = active ? getWorkflowStepHint(step.id, currentJob) : null;

          return (
            <motion.div
              key={step.id}
              initial={false}
              animate={
                activeLive
                  ? {
                      boxShadow: [
                        '0 0 0 rgba(79,70,229,0)',
                        '0 10px 28px rgba(99,102,241,0.16)',
                        '0 0 0 rgba(79,70,229,0)',
                      ],
                    }
                  : {boxShadow: '0 0 0 rgba(79,70,229,0)'}
              }
              transition={activeLive ? {duration: 1.8, repeat: Infinity, ease: 'easeInOut'} : {duration: 0.2}}
              className={cn(
                'relative overflow-hidden rounded-2xl px-2 py-2',
                active && 'bg-indigo-50',
                activeFailed && 'bg-rose-50',
                activeLive && 'ring-1 ring-indigo-200/70',
                activeFailed && 'ring-1 ring-rose-100',
              )}
            >
              {activeLive ? <ProgressSheen className="w-28 via-white/70" duration={1.5} /> : null}
              <div
                className={cn(
                  'relative z-10 flex h-8 w-8 items-center justify-center rounded-full text-xs font-bold',
                  completed && 'bg-emerald-100 text-emerald-700',
                  active && 'bg-indigo-600 text-white',
                  activeFailed && 'bg-rose-100 text-rose-700',
                  !completed && !active && 'border-2 border-slate-200 text-slate-400',
                )}
              >
                {activeFailed ? (
                  <XCircle className="h-4 w-4" />
                ) : completed ? (
                  <Check className="h-4 w-4" />
                ) : activeLive ? (
                  <motion.div
                    animate={{scale: [1, 1.08, 1]}}
                    transition={{duration: 1.1, repeat: Infinity, ease: 'easeInOut'}}
                  >
                    {index + 1}
                  </motion.div>
                ) : (
                  index + 1
                )}
              </div>
              <div className="relative z-10">
                <div className={cn('text-sm font-semibold', active ? 'text-slate-900' : 'text-slate-500', activeFailed && 'text-rose-900')}>
                  {step.label}
                </div>
                {currentJob && active ? (
                  <div className={cn('mt-1 flex flex-wrap items-center gap-2 text-xs text-slate-500', activeFailed && 'text-rose-600')}>
                    <span className="break-words">{activeStatusText}</span>
                    {activeLive ? <LiveDots tone="indigo" /> : null}
                  </div>
                ) : null}
                {activeHint ? (
                  <p className={cn('mt-2 break-words text-xs leading-5 text-slate-500', activeFailed && 'text-rose-600')}>{activeHint}</p>
                ) : null}
              </div>
            </motion.div>
          );
        })}
      </div>

      {showActiveJobsPanel ? (
        <ActiveJobsPanel
          jobs={activeJobs}
          currentJobId={currentJobId}
          deletingJobId={deletingJobId}
          maxItems={activeJobLimit}
          onSelect={onSelectJob}
          onDelete={onDeleteJob}
        />
      ) : null}

      {showQuotaPanel ? <QuotaPanel quota={quota} /> : null}
      {showRecentJobsPanel ? (
        <RecentJobsPanel
          jobs={historyJobs}
          currentJobId={currentJobId}
          deletingJobId={deletingJobId}
          onSelect={onSelectJob}
          onDelete={onDeleteJob}
        />
      ) : null}
    </aside>
  );
}

function AuthView({
  mode,
  draft,
  busy,
  alert,
  onModeChange,
  onDraftChange,
  onSubmit,
}: {
  mode: AuthMode;
  draft: AuthDraft;
  busy: boolean;
  alert: AlertState | null;
  onModeChange: (mode: AuthMode) => void;
  onDraftChange: (field: keyof AuthDraft, value: string) => void;
  onSubmit: (event: FormEvent<HTMLFormElement>) => void;
}) {
  return (
    <div className="min-h-screen bg-slate-50 px-6 py-10 text-slate-900">
      <div className="mx-auto flex min-h-[calc(100vh-5rem)] max-w-6xl flex-col overflow-hidden rounded-[36px] border-8 border-white bg-white shadow-2xl lg:flex-row">
        <section className="flex flex-1 flex-col justify-between bg-gradient-to-br from-slate-900 via-slate-800 to-indigo-900 p-10 text-white">
          <div className="space-y-8">
            <div className="inline-flex h-14 w-14 items-center justify-center rounded-2xl bg-white/10 ring-1 ring-white/20">
              <Sparkles className="h-7 w-7" />
            </div>
            <div className="space-y-4">
              <p className="text-xs font-bold tracking-[0.3em] text-indigo-200">SmartAnalyst</p>
              <h1 className="max-w-lg text-4xl font-bold tracking-tight">
                智能量化分析报告平台 · Beta
              </h1>
              <p className="max-w-xl text-sm leading-7 text-slate-200">
                登录后即可上传 CSV 或 Excel 文件，等待真实分析任务完成，勾选候选图，再下载服务端生成的最终报告。
              </p>
            </div>
          </div>

          <div className="grid gap-4 md:grid-cols-3">
            <FeatureCard icon={<ShieldCheck className="h-5 w-5" />} title="服务端密钥" description="用户无需输入模型 API Key，统一走服务端配置。" />
            <FeatureCard icon={<Database className="h-5 w-5" />} title="异步队列" description="分析和渲染通过队列执行，支持受控并发与重试。" />
            <FeatureCard icon={<BarChart3 className="h-5 w-5" />} title="真实任务状态" description="页面展示的是后端真实进度、事件流和结果产物。" />
          </div>
        </section>

        <section className="flex w-full flex-col justify-center bg-white p-8 lg:max-w-xl lg:p-12">
          <div className="mb-8 flex rounded-2xl bg-slate-100 p-1 text-sm font-semibold">
            <button
              type="button"
              className={cn(
                'flex-1 rounded-xl px-4 py-3 transition-colors',
                mode === 'login' ? 'bg-white text-slate-900 shadow-sm' : 'text-slate-500',
              )}
              onClick={() => onModeChange('login')}
            >
              登录
            </button>
            <button
              type="button"
              className={cn(
                'flex-1 rounded-xl px-4 py-3 transition-colors',
                mode === 'register' ? 'bg-white text-slate-900 shadow-sm' : 'text-slate-500',
              )}
              onClick={() => onModeChange('register')}
            >
              注册
            </button>
          </div>

          <div className="mb-8">
            <h2 className="text-3xl font-bold tracking-tight text-slate-900">
              {mode === 'login' ? '登录到工作台' : '创建公网 Beta 账号'}
            </h2>
            <p className="mt-3 text-sm leading-7 text-slate-500">
              {mode === 'login'
                ? '登录成功后会建立浏览器会话，不再把敏感 token 存进 localStorage。'
                : '注册成功后不会自动登录，会切回登录表单并保留邮箱。'}
            </p>
          </div>

          <form className="space-y-5" onSubmit={onSubmit}>
            {alert ? <AlertBanner alert={alert} /> : null}

            <label className="block space-y-2">
              <span className="text-sm font-semibold text-slate-700">邮箱</span>
              <input
                className="w-full rounded-2xl border border-slate-200 px-4 py-3 text-sm outline-none transition focus:border-indigo-500 focus:ring-4 focus:ring-indigo-100"
                type="email"
                autoComplete="email"
                value={draft.email}
                onChange={(event) => onDraftChange('email', event.target.value)}
                placeholder="you@example.com"
                required
              />
            </label>

            <label className="block space-y-2">
              <span className="text-sm font-semibold text-slate-700">密码</span>
              <input
                className="w-full rounded-2xl border border-slate-200 px-4 py-3 text-sm outline-none transition focus:border-indigo-500 focus:ring-4 focus:ring-indigo-100"
                type="password"
                autoComplete={mode === 'login' ? 'current-password' : 'new-password'}
                value={draft.password}
                onChange={(event) => onDraftChange('password', event.target.value)}
                placeholder="至少 8 位"
                minLength={8}
                required
              />
            </label>

            {mode === 'register' && CAPTCHA_ENABLED ? (
              <label className="block space-y-2">
                <span className="text-sm font-semibold text-slate-700">Captcha</span>
                <input
                  className="w-full rounded-2xl border border-slate-200 px-4 py-3 text-sm outline-none transition focus:border-indigo-500 focus:ring-4 focus:ring-indigo-100"
                  type="text"
                  value={draft.captchaVerifyParam}
                  onChange={(event) => onDraftChange('captchaVerifyParam', event.target.value)}
                  placeholder="captchaVerifyParam"
                  required
                />
              </label>
            ) : null}

            <button
              className="inline-flex w-full items-center justify-center gap-2 rounded-2xl bg-indigo-600 px-5 py-3.5 text-sm font-bold text-white shadow-xl shadow-indigo-100 transition hover:bg-indigo-700 disabled:cursor-not-allowed disabled:opacity-70"
              type="submit"
              disabled={busy}
            >
              {busy ? <Loader2 className="h-4 w-4 animate-spin" /> : mode === 'login' ? <LogIn className="h-4 w-4" /> : <UserPlus className="h-4 w-4" />}
              {mode === 'login' ? '登录并进入工作台' : '创建账号'}
            </button>
          </form>

          <div className="mt-8 rounded-2xl border border-slate-200 bg-slate-50 p-4 text-sm text-slate-600">
            <div className="mb-2 flex items-center gap-2 font-semibold text-slate-700">
              <Info className="h-4 w-4" />
              服务状态
            </div>
            <p className="text-xs text-slate-500">服务正常，登录后即可开始分析。</p>
          </div>
        </section>
      </div>
    </div>
  );
}

function UploadView({
  files,
  busy,
  alert,
  onAddFiles,
  onRemoveFile,
  onStart,
}: {
  files: File[];
  busy: boolean;
  alert: AlertState | null;
  onAddFiles: (files: File[]) => void;
  onRemoveFile: (index: number) => void;
  onStart: () => void;
}) {
  const dropzoneOptions: DropzoneOptions = {
    accept: ACCEPTED_FILE_TYPES,
    multiple: true,
    noClick: true,
    onDragEnter: undefined,
    onDragOver: undefined,
    onDragLeave: undefined,
    onDropAccepted: (acceptedFiles) => onAddFiles(acceptedFiles),
    onDropRejected: () => onAddFiles([]),
  };
  const {getInputProps, getRootProps, isDragActive, open} = useDropzone(dropzoneOptions);

  return (
    <div className="flex h-full flex-col p-8 lg:p-10">
      <div className="mb-10 max-w-3xl">
        <h2 className="text-3xl font-bold tracking-tight text-slate-900">上传你的原始数据</h2>
        <p className="mt-4 max-w-2xl text-sm leading-7 text-slate-500">
          支持 CSV、XLS、XLSX。文件会发送到 SmartAnalyst 后端，由服务端统一完成分析、候选图生成和报告渲染。
        </p>
      </div>

      <div className="grid flex-1 gap-8 xl:grid-cols-[1.2fr_0.8fr]">
        <div className="space-y-6">
          {alert ? <AlertBanner alert={alert} /> : null}

          <div
            {...getRootProps()}
            className={cn(
              'flex min-h-[300px] cursor-pointer flex-col items-center justify-center rounded-[32px] border-2 border-dashed px-8 text-center transition-all',
              isDragActive
                ? 'border-indigo-500 bg-indigo-50'
                : 'border-slate-200 bg-slate-50 hover:border-indigo-400 hover:bg-white',
            )}
          >
            <input {...getInputProps()} />
            <div className="mb-6 flex h-16 w-16 items-center justify-center rounded-2xl bg-indigo-50 text-indigo-600">
              <Upload className="h-8 w-8" />
            </div>
            <h3 className="text-2xl font-bold tracking-tight text-slate-900">
              {isDragActive ? '松开以上传文件' : '把数据文件拖到这里'}
            </h3>
            <p className="mt-4 max-w-xl text-sm leading-7 text-slate-500">
              也可以点击下方按钮手动选择文件。系统会在提交前显示你当前准备上传的文件列表。
            </p>
            <button
              type="button"
              className="mt-8 inline-flex items-center gap-2 rounded-2xl border border-slate-200 bg-white px-5 py-3 text-sm font-semibold text-slate-700 transition hover:border-slate-300 hover:bg-slate-100"
              onClick={open}
            >
              <Upload className="h-4 w-4" />
              选择文件
            </button>
          </div>
        </div>

        <div className="rounded-[32px] border border-slate-200 bg-white p-6 shadow-sm">
          <div className="flex items-center justify-between">
            <div>
              <div className="text-xs font-bold uppercase tracking-[0.2em] text-slate-400">Upload List</div>
              <h3 className="mt-2 text-xl font-bold tracking-tight text-slate-900">待提交文件</h3>
            </div>
            <div className="rounded-full bg-slate-100 px-3 py-1 text-xs font-semibold text-slate-500">
              {files.length} 个文件
            </div>
          </div>

          <div className="mt-6 space-y-3">
            {files.length ? (
              files.map((file, index) => (
                <div key={`${file.name}-${file.lastModified}`} className="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3">
                  <div className="flex items-start justify-between gap-3">
                    <div className="min-w-0">
                      <div className="truncate text-sm font-semibold text-slate-800">{file.name}</div>
                      <div className="mt-1 text-xs text-slate-400">{formatFileSize(file.size)}</div>
                    </div>
                    <button
                      type="button"
                      className="rounded-xl px-3 py-2 text-xs font-semibold text-slate-500 transition hover:bg-slate-200 hover:text-slate-700"
                      onClick={() => onRemoveFile(index)}
                    >
                      删除
                    </button>
                  </div>
                </div>
              ))
            ) : (
              <div className="rounded-2xl border border-dashed border-slate-200 bg-slate-50 px-4 py-8 text-sm text-slate-400">
                还没有待上传的文件。
              </div>
            )}
          </div>

          <button
            type="button"
            className="mt-6 inline-flex w-full items-center justify-center gap-2 rounded-2xl bg-slate-900 px-5 py-3 text-sm font-bold text-white transition hover:bg-slate-700 disabled:cursor-not-allowed disabled:opacity-70"
            onClick={onStart}
            disabled={busy}
          >
            {busy ? <Loader2 className="h-4 w-4 animate-spin" /> : <Sparkles className="h-4 w-4" />}
            开始分析
          </button>
        </div>
      </div>
    </div>
  );
}

function ProcessingView({
  job,
  events,
  tasks,
  title,
  description,
  alert,
}: {
  job: JobSummaryResponse;
  events: JobEventResponse[];
  tasks: JobTaskResponse[];
  title: string;
  description: string;
  alert: AlertState | null;
}) {
  const modules = getProgressModules(job, events, tasks);
  const moduleIcons: Record<ProgressModuleId, typeof Upload> = {
    received: Upload,
    planning: Database,
    charts: BarChart3,
    report: FileText,
  };
  const activeModule = modules.find((module) => module.state === 'active');

  return (
    <div className="flex h-full flex-col p-7 lg:p-9">
      <div className="mb-7 flex flex-col gap-4 xl:flex-row xl:items-end xl:justify-between">
        <div className="max-w-3xl">
          <div className="mb-3 inline-flex items-center gap-2 rounded-full bg-sky-50 px-3 py-1 text-xs font-bold text-sky-700">
            <Sparkles className="h-4 w-4" />
            智能分析工作流
          </div>
          <h2 className="text-3xl font-extrabold tracking-tight text-slate-900">{title}</h2>
          <p className="mt-3 text-sm leading-7 text-slate-500">{description}</p>
        </div>
        <div className="inline-flex w-fit items-center gap-2 rounded-full bg-white px-3.5 py-2 text-xs font-semibold text-slate-700 ring-1 ring-slate-200">
          <LiveDots />
          <span>{formatStatus(job.status)}</span>
          {activeModule ? <span className="text-slate-400">·</span> : null}
          {activeModule ? <span className="text-slate-500">{activeModule.title}</span> : null}
        </div>
      </div>

      <div className="grid flex-1 gap-6 xl:grid-cols-[1.35fr_0.65fr]">
        <div className="rounded-[32px] border border-slate-200 bg-white p-7 shadow-md shadow-slate-200/60">
          <div className="mb-5 flex items-center justify-between gap-4">
            <div>
              <div className="text-xs font-bold tracking-[0.2em] text-slate-400">实时流程</div>
              <h3 className="mt-2 text-xl font-bold tracking-tight text-slate-900">分析执行进度</h3>
            </div>
            <div className="inline-flex items-center gap-2 rounded-full bg-slate-50 px-3 py-1 text-xs font-semibold text-slate-500 ring-1 ring-slate-100">
              <Clock3 className="h-4 w-4" />
              自动刷新
            </div>
          </div>

          <div className="grid gap-4 lg:grid-cols-2">
            {modules.map((module, index) => {
              const Icon = moduleIcons[module.id];
              const isDone = module.state === 'done';
              const isActive = module.state === 'active';
              const stateLabel = getProgressStateLabel(module.state);

              return (
                <motion.div
                  key={module.id}
                  layout
                  initial={false}
                  animate={
                    isActive
                      ? {
                          y: [0, -2, 0],
                          boxShadow: [
                            '0 10px 26px rgba(56,189,248,0.08)',
                            '0 20px 44px rgba(56,189,248,0.2)',
                            '0 10px 26px rgba(56,189,248,0.08)',
                          ],
                        }
                      : {y: 0, boxShadow: isDone ? '0 10px 26px rgba(16,185,129,0.08)' : '0 0 0 rgba(0,0,0,0)'}
                  }
                  transition={
                    isActive
                      ? {duration: 2.1, repeat: Infinity, ease: 'easeInOut'}
                      : {duration: 0.24, ease: 'easeOut'}
                  }
                  className={cn(
                    'relative min-h-[9.5rem] overflow-hidden rounded-[26px] border p-4 transition-colors',
                    isDone && 'border-emerald-100 bg-emerald-50/70 shadow-sm shadow-emerald-100',
                    isActive &&
                      'border-sky-200 bg-gradient-to-br from-sky-50 via-white to-cyan-50 shadow-lg shadow-sky-100/70 ring-1 ring-sky-100',
                    !isDone && !isActive && 'border-slate-200 bg-slate-50/70 opacity-80',
                  )}
                >
                  {isActive ? (
                    <>
                      <motion.div
                        aria-hidden="true"
                        className="pointer-events-none absolute -left-10 top-6 h-16 w-16 rounded-full bg-sky-200/50 blur-2xl"
                        animate={{opacity: [0.35, 0.7, 0.35], scale: [0.9, 1.2, 0.9]}}
                        transition={{duration: 1.6, ease: 'easeInOut', repeat: Infinity}}
                      />
                      <ProgressSheen className="w-40 via-white/90" duration={1.35} />
                    </>
                  ) : null}

                  <div className="relative z-10 flex h-full gap-4">
                    <div className="flex flex-col items-center gap-3">
                      <div
                        className={cn(
                          'flex h-11 w-11 flex-shrink-0 items-center justify-center rounded-2xl',
                          isDone && 'bg-emerald-100 text-emerald-700',
                          isActive && 'bg-sky-100 text-sky-700',
                          !isDone && !isActive && 'bg-white text-slate-400',
                        )}
                      >
                        {isActive ? (
                          <motion.div
                            animate={{scale: [1, 1.08, 1], rotate: [0, -4, 4, 0]}}
                            transition={{duration: 1.5, repeat: Infinity, ease: 'easeInOut'}}
                          >
                            <Icon className="h-5 w-5" />
                          </motion.div>
                        ) : (
                          <Icon className="h-5 w-5" />
                        )}
                      </div>
                      <div
                        className={cn(
                          'inline-flex h-7 min-w-7 items-center justify-center rounded-full px-2 text-xs font-bold tabular-nums',
                          isDone && 'bg-emerald-100 text-emerald-700',
                          isActive && 'bg-sky-100 text-sky-700',
                          !isDone && !isActive && 'bg-slate-200 text-slate-500',
                        )}
                      >
                        {isDone ? <Check className="h-4 w-4" /> : index + 1}
                      </div>
                    </div>

                    <div className="min-w-0 flex-1">
                      <div className="flex flex-wrap items-center gap-2">
                        <div className="text-base font-bold tracking-tight text-slate-900">{module.title}</div>
                        {isActive ? <LiveDots /> : null}
                      </div>
                      <div className="mt-2 flex flex-wrap items-center gap-2">
                        <span
                          className={cn(
                            'inline-flex rounded-full px-2.5 py-1 text-xs font-semibold',
                            isDone && 'bg-emerald-100 text-emerald-700',
                            isActive && 'bg-sky-100 text-sky-700',
                            !isDone && !isActive && 'bg-slate-100 text-slate-500',
                          )}
                        >
                          {stateLabel}
                        </span>
                        <span className="inline-flex items-center gap-1 rounded-full bg-white/80 px-2.5 py-1 text-xs font-semibold tabular-nums text-slate-500 ring-1 ring-slate-100">
                          <Clock3 className="h-3.5 w-3.5" />
                          {formatDurationBadge(module.estimatedDuration)}
                        </span>
                      </div>
                      <p className="mt-3 text-sm leading-6 text-slate-500">{getProgressBrief(module)}</p>
                    </div>
                  </div>
                </motion.div>
              );
            })}
          </div>

          {alert ? (
            <div className="mt-6">
              <AlertBanner alert={alert} />
            </div>
          ) : null}
        </div>

        <div className="rounded-[32px] border border-slate-200 bg-white p-7 shadow-sm">
          <div className="mb-5 flex items-center justify-between">
            <div>
              <div className="text-xs font-bold tracking-[0.2em] text-slate-400">任务动态</div>
              <h3 className="mt-2 text-xl font-bold tracking-tight text-slate-900">实时记录</h3>
            </div>
            <div className="rounded-full bg-slate-100 px-3 py-1 text-xs font-semibold text-slate-500">
              最近 {getCompactProcessingEvents(events).length} 条
            </div>
          </div>
          <ProcessingEventTimeline events={events} />
        </div>
      </div>
    </div>
  );
}

function SelectionView({
  job,
  tasks,
  selectedTaskIds,
  busy,
  alert,
  onToggleTask,
  onSubmitSelection,
}: {
  job: JobSummaryResponse;
  tasks: JobTaskResponse[];
  selectedTaskIds: number[];
  busy: boolean;
  alert: AlertState | null;
  onToggleTask: (taskIndex: number) => void;
  onSubmitSelection: () => void;
}) {
  return (
    <div className="flex h-full flex-col p-8 lg:p-10">
      <div className="mb-8 flex flex-col gap-5 xl:flex-row xl:items-end xl:justify-between">
        <div className="max-w-3xl">
          <div className="mb-3">
            <StatusBadge status={job.status} />
          </div>
          <h2 className="text-3xl font-bold tracking-tight text-slate-900">选择要进入最终报告的图表</h2>
          <p className="mt-4 text-sm leading-7 text-slate-500">
            候选图和分析摘要来自后端真实输出。勾选后会立即进入渲染队列。
          </p>
        </div>

        <button
          type="button"
          className="inline-flex items-center justify-center gap-2 rounded-2xl bg-slate-900 px-5 py-3 text-sm font-bold text-white transition hover:bg-slate-700 disabled:cursor-not-allowed disabled:opacity-70"
          onClick={onSubmitSelection}
          disabled={busy}
        >
          {busy ? <Loader2 className="h-4 w-4 animate-spin" /> : <CheckCircle2 className="h-4 w-4" />}
          开始生成报告
        </button>
      </div>

      {alert ? <div className="mb-6"><AlertBanner alert={alert} /></div> : null}

      <div className="grid gap-6 xl:grid-cols-2">
        {tasks.map((task) => {
          const checked = selectedTaskIds.includes(task.task_index);
          return (
            <label
              key={task.task_index}
              className={cn(
                'block cursor-pointer overflow-hidden rounded-[32px] border bg-white shadow-sm transition',
                checked ? 'border-indigo-300 ring-4 ring-indigo-50' : 'border-slate-200 hover:border-slate-300',
              )}
            >
              <div className="flex items-center justify-between border-b border-slate-100 px-6 py-4">
                <div>
                  <div className="text-xs font-bold uppercase tracking-[0.2em] text-slate-400">
                    Task {task.task_index}
                  </div>
                  <div className="mt-2 text-lg font-bold tracking-tight text-slate-900">{task.question_zh}</div>
                </div>
                <input
                  type="checkbox"
                  className="h-5 w-5 rounded border-slate-300 text-indigo-600 focus:ring-indigo-500"
                  checked={checked}
                  onChange={() => onToggleTask(task.task_index)}
                />
              </div>

              <div className="aspect-[16/9] bg-slate-100">
                {task.chart_url ? (
                  <img className="h-full w-full object-cover" src={task.chart_url} alt={task.question_zh} loading="lazy" />
                ) : (
                  <div className="flex h-full items-center justify-center text-sm text-slate-400">暂无图像预览</div>
                )}
              </div>

              <div className="space-y-4 px-6 py-5">
                <div>
                  <div className="text-xs font-bold tracking-[0.2em] text-slate-400">分析类型</div>
                  <div className="mt-2 text-sm font-semibold text-slate-700">{task.analysis_type}</div>
                </div>
                <div>
                  <div className="text-xs font-bold tracking-[0.2em] text-slate-400">分析说明</div>
                  <p className="mt-2 text-sm leading-7 text-slate-500">
                    {task.analysis_text || '后端未返回额外摘要说明。'}
                  </p>
                </div>
              </div>
            </label>
          );
        })}
      </div>
    </div>
  );
}

function ResultView({
  job,
  artifacts,
  events,
  alert,
  downloadingArtifact,
  onDownload,
  onRestart,
}: {
  job: JobSummaryResponse;
  artifacts: JobArtifactResponse[];
  events: JobEventResponse[];
  alert: AlertState | null;
  downloadingArtifact: ArtifactType | null;
  onDownload: (artifact: JobArtifactResponse) => void;
  onRestart: () => void;
}) {
  const visibleArtifacts = artifacts.filter((artifact) => VISIBLE_ARTIFACT_TYPES.has(artifact.artifact_type));
  const zipArtifact = artifacts.find((artifact) => artifact.artifact_type === 'zip');

  return (
    <div className="flex h-full flex-col p-8 lg:p-10">
      <div className="mb-8 flex flex-col gap-5 xl:flex-row xl:items-end xl:justify-between">
        <div className="max-w-3xl">
          <div className="mb-3 inline-flex items-center gap-2 rounded-full bg-emerald-50 px-3 py-1 text-xs font-bold text-emerald-700">
            <CheckCircle2 className="h-4 w-4" />
            报告已完成
          </div>
          <h2 className="text-3xl font-bold tracking-tight text-slate-900">你的报告已经可以下载</h2>
          <p className="mt-4 text-sm leading-7 text-slate-500">
            三个核心产物已准备完成。过期时间：{formatDateTime(job.retention_expires_at)}
          </p>
        </div>

        <div className="flex flex-wrap items-center gap-3">
          <button
            type="button"
            className="inline-flex items-center gap-2 rounded-2xl border border-slate-200 bg-white px-5 py-3 text-sm font-semibold text-slate-700 transition hover:border-slate-300 hover:bg-slate-50"
            onClick={onRestart}
          >
            <RefreshCw className="h-4 w-4" />
            新建任务
          </button>
        </div>
      </div>

      {alert ? <div className="mb-6"><AlertBanner alert={alert} /></div> : null}

      <div className="grid flex-1 gap-5 2xl:grid-cols-[1.08fr_0.92fr]">
        <div className="rounded-[32px] border border-slate-200 bg-white p-7 shadow-md shadow-slate-200/70">
          <div className="mb-5 flex flex-wrap items-center justify-between gap-3">
            <div>
              <div className="text-xs font-bold tracking-[0.2em] text-slate-400">核心产物</div>
              <h3 className="mt-2 text-xl font-bold tracking-tight text-slate-900">可下载产物</h3>
            </div>
            <div className="flex flex-wrap items-center justify-end gap-2">
              <div className="rounded-full bg-slate-100 px-3 py-1 text-xs font-semibold text-slate-500">
                {visibleArtifacts.length} 个文件
              </div>
              {zipArtifact ? (
                <button
                  type="button"
                  className="inline-flex items-center gap-1.5 rounded-full border border-slate-200 bg-white px-3 py-1.5 text-xs font-semibold text-slate-600 shadow-sm shadow-slate-200/40 transition hover:border-indigo-200 hover:bg-indigo-50 hover:text-indigo-700 disabled:cursor-not-allowed disabled:opacity-70"
                  onClick={() => onDownload(zipArtifact)}
                  disabled={downloadingArtifact === zipArtifact.artifact_type}
                  title="下载包含 Word 报告、Notebook 和清洗摘要的压缩包"
                  aria-label="下载包含 Word 报告、Notebook 和清洗摘要的压缩包"
                >
                  {downloadingArtifact === zipArtifact.artifact_type ? (
                    <Loader2 className="h-3.5 w-3.5 animate-spin" />
                  ) : (
                    <Download className="h-3.5 w-3.5" />
                  )}
                  打包下载 ZIP
                </button>
              ) : null}
            </div>
          </div>

          <div className="grid gap-3 md:grid-cols-2 lg:grid-cols-3">
            {visibleArtifacts.map((artifact) => (
              <button
                key={artifact.artifact_type}
                type="button"
                className="group rounded-2xl border border-slate-200 bg-slate-50 p-4 text-left transition hover:border-indigo-200 hover:bg-white hover:shadow-sm disabled:cursor-not-allowed disabled:opacity-70"
                onClick={() => onDownload(artifact)}
                disabled={downloadingArtifact === artifact.artifact_type}
              >
                <div className="mb-3 inline-flex h-10 w-10 items-center justify-center rounded-2xl bg-white text-slate-700 shadow-sm">
                  {artifact.artifact_type === 'docx' ? <FileText className="h-5 w-5" /> : <FileCode className="h-5 w-5" />}
                </div>
                <div className="text-sm font-semibold text-slate-800">{ARTIFACT_META[artifact.artifact_type].title}</div>
                <div className="mt-2 text-xs leading-5 text-slate-500">
                  {ARTIFACT_META[artifact.artifact_type].description}
                </div>
                <div className="mt-3 flex items-center justify-end">
                  <span className="inline-flex items-center gap-1.5 rounded-full bg-white px-3 py-1.5 text-xs font-semibold text-slate-700 ring-1 ring-slate-200 transition group-hover:text-indigo-700 group-hover:ring-indigo-200">
                    {downloadingArtifact === artifact.artifact_type ? (
                      <Loader2 className="h-3.5 w-3.5 animate-spin" />
                    ) : (
                      <Download className="h-3.5 w-3.5" />
                    )}
                    {ARTIFACT_META[artifact.artifact_type].downloadLabel}
                  </span>
                </div>
              </button>
            ))}
          </div>
        </div>

        <div className="rounded-[32px] border border-slate-200 bg-white p-7 shadow-sm">
          <div className="mb-5 flex items-center justify-between">
            <div>
              <div className="text-xs font-bold tracking-[0.2em] text-slate-400">任务动态</div>
              <h3 className="mt-2 text-xl font-bold tracking-tight text-slate-900">完成摘要</h3>
            </div>
          </div>
          <div className="mb-5 space-y-2 rounded-2xl border border-emerald-100 bg-emerald-50/70 p-4">
            {[
              '报告已生成',
              `${visibleArtifacts.length} 个核心产物已准备完成`,
              'Word 报告已准备完成',
              'Notebook 已准备完成',
              '清洗摘要已准备完成',
            ].map((item) => (
              <div key={item} className="flex items-center gap-2 text-sm font-semibold text-emerald-800">
                <CheckCircle2 className="h-4 w-4 flex-shrink-0" />
                <span>{item}</span>
              </div>
            ))}
          </div>
          <div className="border-t border-slate-100 pt-5">
            <div className="mb-3 text-xs font-bold tracking-[0.2em] text-slate-400">处理记录</div>
            <EventList events={events} />
          </div>
        </div>
      </div>
    </div>
  );
}

function FailureView({
  job,
  events,
  title,
  description,
  alert,
  onRefresh,
  onRestart,
}: {
  job: JobSummaryResponse;
  events: JobEventResponse[];
  title: string;
  description: string;
  alert: AlertState | null;
  onRefresh: () => void;
  onRestart: () => void;
}) {
  const [idCopied, setIdCopied] = useState(false);

  async function copyTaskId() {
    if (typeof navigator === 'undefined' || !navigator.clipboard) {
      return;
    }
    await navigator.clipboard.writeText(job.id);
    setIdCopied(true);
    window.setTimeout(() => setIdCopied(false), 1400);
  }

  return (
    <div className="min-h-full overflow-x-hidden p-4 sm:p-6 lg:p-10">
      <div className="mx-auto w-full max-w-5xl rounded-[30px] border border-slate-200 bg-white p-5 shadow-sm shadow-slate-200/70 sm:p-8">
        <div className="flex flex-col gap-6 lg:flex-row lg:items-start lg:justify-between">
          <div className="min-w-0">
            <div className="mb-5 inline-flex h-14 w-14 items-center justify-center rounded-2xl bg-rose-50 text-rose-600">
              <AlertCircle className="h-7 w-7" />
            </div>
            <h2 className="text-3xl font-bold tracking-tight text-slate-900">{title}</h2>
            <p className="mt-4 max-w-2xl text-sm leading-7 text-slate-500">{description}</p>
          </div>

          <div className="flex flex-wrap items-center gap-3 lg:justify-end">
            <button
              type="button"
              className="inline-flex items-center gap-2 rounded-2xl border border-slate-200 bg-white px-4 py-3 text-sm font-bold text-slate-700 transition hover:border-slate-300 hover:bg-slate-50"
              onClick={() => void copyTaskId()}
            >
              <Copy className="h-4 w-4" />
              {idCopied ? '已复制任务 ID' : '复制任务 ID'}
            </button>

            <button
              type="button"
              className="inline-flex items-center gap-2 rounded-2xl border border-slate-200 bg-white px-4 py-3 text-sm font-bold text-slate-700 transition hover:border-slate-300 hover:bg-slate-50"
              onClick={onRefresh}
            >
              <RefreshCw className="h-4 w-4" />
              刷新状态
            </button>

            <button
              type="button"
              className="inline-flex items-center gap-2 rounded-2xl bg-slate-900 px-5 py-3 text-sm font-bold text-white transition hover:bg-slate-700"
              onClick={onRestart}
            >
              <Upload className="h-4 w-4" />
              返回上传页
            </button>
          </div>
        </div>

        <div className="mt-6 space-y-4">
          {alert ? <AlertBanner alert={alert} /> : null}
          <FailureNoticeCard job={job} events={events} />
        </div>

        <div className="mt-8 rounded-3xl border border-slate-200 bg-slate-50/60 p-5">
          <div className="mb-4 text-sm font-bold text-slate-900">最近动态</div>
          <EventList events={events} />
        </div>
      </div>
    </div>
  );
}

function LoadingView({message}: {message: string}) {
  return (
    <div className="flex h-full items-center justify-center p-8">
      <div className="flex max-w-md flex-col items-center text-center">
        <div className="mb-6 flex h-16 w-16 items-center justify-center rounded-2xl bg-indigo-50 text-indigo-600">
          <Loader2 className="h-8 w-8 animate-spin" />
        </div>
        <h2 className="text-2xl font-bold tracking-tight text-slate-900">{message}</h2>
        <p className="mt-3 text-sm leading-7 text-slate-500">正在同步浏览器会话和 SmartAnalyst 后端状态。</p>
      </div>
    </div>
  );
}

export function SmartAnalystApp() {
  const session = useSession();
  const jobs = useJobRuntime(Boolean(session.user), session.logoutUser);

  const currentStep = getWorkflowStep(jobs.currentJob);
  const footerHint = getFooterHint(jobs.currentJob);

  useEffect(() => {
    if (!session.user) {
      return undefined;
    }

    let cancelled = false;
    const sendHeartbeat = () => {
      if (cancelled) {
        return;
      }
      void sendPresenceHeartbeat({
        current_job_id: jobs.currentJobId,
        current_path: window.location.pathname,
      }).catch(() => {
        // Presence is best-effort and should never interrupt user workflows.
      });
    };

    sendHeartbeat();
    const interval = window.setInterval(sendHeartbeat, 60_000);
    return () => {
      cancelled = true;
      window.clearInterval(interval);
    };
  }, [session.user, jobs.currentJobId]);

  async function handleLogout() {
    jobs.resetWorkspace();
    await session.logoutUser();
  }

  function renderMainContent() {
    if (jobs.currentJobId && jobs.jobLoading && !jobs.currentJob) {
      return <LoadingView message="正在恢复最近一次任务" />;
    }

    if (!jobs.currentJob) {
      return (
        <UploadView
          files={jobs.files}
          busy={jobs.uploadBusy}
          alert={jobs.uploadAlert}
          onAddFiles={jobs.addFiles}
          onRemoveFile={jobs.removeFile}
          onStart={() => void jobs.createNewJob()}
        />
      );
    }

    if (
      jobs.currentJob.status === 'uploaded' ||
      jobs.currentJob.status === 'queued_analysis' ||
      jobs.currentJob.status === 'running_analysis'
    ) {
      return (
        <ProcessingView
          job={jobs.currentJob}
          events={jobs.events}
          tasks={jobs.tasks}
          title="正在进行分析"
          description="系统正在分析数据并生成候选图表，完成后你可以选择需要写入报告的图表。请不要关闭页面。"
          alert={jobs.jobAlert}
        />
      );
    }

    if (jobs.currentJob.status === 'awaiting_selection') {
      return (
        <SelectionView
          job={jobs.currentJob}
          tasks={jobs.tasks}
          selectedTaskIds={jobs.selectedTaskIds}
          busy={jobs.selectionBusy}
          alert={jobs.selectionAlert || jobs.jobAlert}
          onToggleTask={jobs.toggleTask}
          onSubmitSelection={() => void jobs.submitTaskSelection()}
        />
      );
    }

    if (jobs.currentJob.status === 'queued_render' || jobs.currentJob.status === 'rendering') {
      return (
        <ProcessingView
          job={jobs.currentJob}
          events={jobs.events}
          tasks={jobs.tasks}
          title="正在生成最终报告"
          description="系统已收到你的选图结果，正在调用高质量模型撰写并优化最终报告，同时进行文档导出，预计需要 3—5 分钟，请耐心等待。"
          alert={jobs.jobAlert}
        />
      );
    }

    if (jobs.currentJob.status === 'completed') {
      return (
        <ResultView
          job={jobs.currentJob}
          artifacts={jobs.artifacts}
          events={jobs.events}
          alert={jobs.jobAlert}
          downloadingArtifact={jobs.downloadingArtifact}
          onDownload={(artifact) => void jobs.downloadArtifact(artifact)}
          onRestart={jobs.resetWorkspace}
        />
      );
    }

    if (jobs.currentJob.status === 'expired') {
      return (
        <FailureView
          job={jobs.currentJob}
          events={jobs.events}
          title="任务资源已过期"
          description="该任务的上传文件和产物已经被后端清理，当前保留的是状态信息和最近事件，方便用户理解发生过什么。"
          alert={jobs.jobAlert}
          onRefresh={() => void jobs.refreshCurrentJob()}
          onRestart={jobs.resetWorkspace}
        />
      );
    }

    return (
      <FailureView
        job={jobs.currentJob}
        events={jobs.events}
        title="任务未能完成"
        description="系统在分析数据时遇到异常，本次任务未完成。你可以重新尝试，或查看错误详情后再提交。"
        alert={jobs.jobAlert}
        onRefresh={() => void jobs.refreshCurrentJob()}
        onRestart={jobs.resetWorkspace}
      />
    );
  }

  if (session.loading) {
    return (
      <div className="min-h-screen bg-slate-50">
        <LoadingView message="正在恢复浏览器会话" />
      </div>
    );
  }

  if (!session.user) {
    return (
      <AuthView
        mode={session.authMode}
        draft={session.authDraft}
        busy={session.authBusy}
        alert={session.authAlert}
        onModeChange={session.setAuthMode}
        onDraftChange={session.setAuthField}
        onSubmit={(event) => {
          event.preventDefault();
          void session.submitAuth();
        }}
      />
    );
  }

  return (
    <div className="h-screen w-full overflow-hidden bg-slate-50 p-4 text-slate-900">
      <div className="flex h-full flex-col overflow-hidden rounded-[40px] border-8 border-white bg-white shadow-2xl">
        <header className="flex h-20 flex-shrink-0 items-center justify-between border-b border-slate-200 px-8">
          <div className="flex items-center gap-4">
            <div className="flex h-11 w-11 items-center justify-center rounded-2xl bg-indigo-600 text-white shadow-lg shadow-indigo-100">
              <Sparkles className="h-5 w-5" />
            </div>
            <div>
              <h1 className="text-xl font-bold tracking-tight text-slate-900">SmartAnalyst</h1>
              <p className="text-xs text-slate-400">智能量化分析报告平台 · Beta</p>
            </div>
          </div>

          <div className="flex items-center gap-5">
            <div className="hidden text-right lg:block">
              <div className="text-[10px] font-bold uppercase tracking-[0.25em] text-slate-400">当前用户</div>
              <div className="mt-1 text-sm font-semibold text-slate-700">{session.user.email}</div>
              {EMAIL_VERIFICATION_UI_ENABLED || session.user.email_verified ? (
                <div className={cn('mt-1 text-xs font-semibold', session.user.email_verified ? 'text-emerald-600' : 'text-amber-600')}>
                  {session.user.email_verified ? '邮箱已验证' : '邮箱未验证'}
                </div>
              ) : null}
            </div>
            <button
              type="button"
              className="inline-flex items-center gap-2 rounded-2xl border border-slate-200 bg-white px-4 py-3 text-sm font-semibold text-slate-700 transition hover:border-slate-300 hover:bg-slate-100"
              onClick={() => void handleLogout()}
            >
              <LogOut className="h-4 w-4" />
              退出登录
            </button>
          </div>
        </header>

        <main className="flex min-h-0 flex-1 overflow-hidden">
          <ProgressSidebar
            currentStep={currentStep}
            currentJob={jobs.currentJob}
            recentJobs={jobs.recentJobs}
            currentJobId={jobs.currentJobId}
            deletingJobId={jobs.deletingJobId}
            onSelectJob={jobs.setCurrentJobId}
            onDeleteJob={(jobId) => void jobs.deleteJob(jobId)}
          />
          <section className="flex min-h-0 flex-1 flex-col overflow-hidden bg-white/50">
            <AnimatePresence mode="wait">
              <motion.div
                key={jobs.currentJob ? `${jobs.currentJob.status}-${jobs.currentJob.id}` : 'upload'}
                className="min-h-0 flex-1 overflow-auto"
                initial={{opacity: 0, x: 18}}
                animate={{opacity: 1, x: 0}}
                exit={{opacity: 0, x: -18}}
                transition={{duration: 0.22}}
              >
                {renderMainContent()}
              </motion.div>
            </AnimatePresence>
          </section>
        </main>

        <footer className="flex h-24 flex-shrink-0 items-center justify-between border-t border-slate-200 px-8">
          <div className="flex max-w-2xl items-start gap-3 text-slate-500">
            <Info className="mt-0.5 h-4 w-4 flex-shrink-0" />
            <p className="text-xs leading-6">{footerHint}</p>
          </div>

          <div className="hidden items-center gap-2 rounded-2xl bg-slate-100 px-4 py-3 text-xs font-semibold text-slate-500 md:flex">
            <CheckCircle2 className="h-4 w-4 text-emerald-600" />
            服务正常
          </div>
        </footer>
      </div>
    </div>
  );
}
