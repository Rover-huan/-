import {useEffect, useEffectEvent, useRef, useState} from 'react';
import {
  ApiError,
  UnauthorizedError,
  createJob,
  deleteJob as deleteJobRequest,
  fetchBinary,
  getJobArtifacts,
  getJobEvents,
  getJobSummary,
  getJobTasks,
  getJobs,
  openJobStream,
  submitSelection,
} from '../../lib/api';
import {downloadBlobFile} from '../../lib/download';
import type {
  ArtifactType,
  JobArtifactResponse,
  JobDeleteResponse,
  JobEventResponse,
  JobStatus,
  JobSummaryResponse,
  JobTaskResponse,
} from '../../lib/types';
import type {AlertState} from '../types';

const JOB_STORAGE_KEY = 'smartanalyst.currentJobId';
const STREAMABLE_STATUSES = new Set<JobStatus>([
  'queued_analysis',
  'running_analysis',
  'queued_render',
  'rendering',
]);
const TASK_VISIBLE_STATUSES = new Set<JobStatus>([
  'awaiting_selection',
  'queued_render',
  'rendering',
  'completed',
  'failed',
]);

function readStoredValue(key: string) {
  if (typeof window === 'undefined') {
    return null;
  }
  return window.localStorage.getItem(key);
}

function persistStoredValue(key: string, value: string | null) {
  if (typeof window === 'undefined') {
    return;
  }

  if (value) {
    window.localStorage.setItem(key, value);
  } else {
    window.localStorage.removeItem(key);
  }
}

function getErrorMessage(error: unknown) {
  if (error instanceof ApiError) {
    return error.detail;
  }
  if (error instanceof Error) {
    return error.message;
  }
  return '操作失败，请稍后重试。';
}

function sortArtifacts(artifacts: JobArtifactResponse[]) {
  const priority: Record<ArtifactType, number> = {
    zip: 0,
    docx: 1,
    pdf: 2,
    ipynb: 3,
    txt: 4,
  };

  return [...artifacts].sort((left, right) => {
    const leftPriority = priority[left.artifact_type] ?? 99;
    const rightPriority = priority[right.artifact_type] ?? 99;
    return leftPriority - rightPriority;
  });
}

function getSelectedTaskIds(summary: JobSummaryResponse | null, tasks: JobTaskResponse[]) {
  if (summary?.selected_task_ids?.length) {
    return [...summary.selected_task_ids];
  }
  return tasks.filter((task) => task.selected).map((task) => task.task_index);
}

export interface JobRuntimeController {
  currentJobId: string | null;
  currentJob: JobSummaryResponse | null;
  recentJobs: JobSummaryResponse[];
  tasks: JobTaskResponse[];
  artifacts: JobArtifactResponse[];
  events: JobEventResponse[];
  files: File[];
  selectedTaskIds: number[];
  jobLoading: boolean;
  uploadBusy: boolean;
  selectionBusy: boolean;
  deletingJobId: string | null;
  downloadingArtifact: ArtifactType | null;
  uploadAlert: AlertState | null;
  selectionAlert: AlertState | null;
  jobAlert: AlertState | null;
  setCurrentJobId: (jobId: string | null) => void;
  addFiles: (files: File[]) => void;
  removeFile: (index: number) => void;
  createNewJob: () => Promise<void>;
  toggleTask: (taskIndex: number) => void;
  submitTaskSelection: () => Promise<void>;
  deleteJob: (jobId: string) => Promise<void>;
  downloadArtifact: (artifact: JobArtifactResponse) => Promise<void>;
  refreshCurrentJob: (showLoader?: boolean) => Promise<void>;
  refreshRecentJobs: () => Promise<void>;
  resetWorkspace: () => void;
}

export function useJobRuntime(
  enabled: boolean,
  onUnauthorized: (message?: string) => Promise<void> | void,
): JobRuntimeController {
  const [currentJobId, setCurrentJobIdState] = useState<string | null>(() => readStoredValue(JOB_STORAGE_KEY));
  const [currentJob, setCurrentJob] = useState<JobSummaryResponse | null>(null);
  const [recentJobs, setRecentJobs] = useState<JobSummaryResponse[]>([]);
  const [tasks, setTasks] = useState<JobTaskResponse[]>([]);
  const [artifacts, setArtifacts] = useState<JobArtifactResponse[]>([]);
  const [events, setEvents] = useState<JobEventResponse[]>([]);
  const [files, setFiles] = useState<File[]>([]);
  const [selectedTaskIds, setSelectedTaskIds] = useState<number[]>([]);

  const [jobLoading, setJobLoading] = useState(false);
  const [uploadBusy, setUploadBusy] = useState(false);
  const [selectionBusy, setSelectionBusy] = useState(false);
  const [deletingJobId, setDeletingJobId] = useState<string | null>(null);
  const [downloadingArtifact, setDownloadingArtifact] = useState<ArtifactType | null>(null);

  const [uploadAlert, setUploadAlert] = useState<AlertState | null>(null);
  const [selectionAlert, setSelectionAlert] = useState<AlertState | null>(null);
  const [jobAlert, setJobAlert] = useState<AlertState | null>(null);

  const eventsCursorRef = useRef<string | null>(null);
  const tasksLoadedForJobRef = useRef<string | null>(null);
  const artifactsLoadedForJobRef = useRef<string | null>(null);
  const eventsLoadedForJobRef = useRef<string | null>(null);

  useEffect(() => {
    persistStoredValue(JOB_STORAGE_KEY, currentJobId);
  }, [currentJobId]);

  const mergeEvents = useEffectEvent((incoming: JobEventResponse[]) => {
    if (!incoming.length) {
      return;
    }

    setEvents((previous) => {
      const merged = new Map<string, JobEventResponse>(previous.map((event) => [event.id, event]));
      for (const event of incoming) {
        merged.set(event.id, event);
      }
      const nextEvents = [...merged.values()].sort((left, right) =>
        left.created_at.localeCompare(right.created_at),
      );
      eventsCursorRef.current = nextEvents[nextEvents.length - 1]?.id ?? eventsCursorRef.current;
      return nextEvents;
    });
  });

  const clearJobDetails = useEffectEvent(() => {
    setTasks([]);
    setArtifacts([]);
    setEvents([]);
    setSelectedTaskIds([]);
    tasksLoadedForJobRef.current = null;
    artifactsLoadedForJobRef.current = null;
    eventsLoadedForJobRef.current = null;
    eventsCursorRef.current = null;
  });

  const upsertRecentJob = useEffectEvent((summary: JobSummaryResponse) => {
    setRecentJobs((previous) => {
      const next = [summary, ...previous.filter((item) => item.id !== summary.id)];
      return next.sort((left, right) => right.created_at.localeCompare(left.created_at)).slice(0, 12);
    });
  });

  const removeRecentJob = useEffectEvent((jobId: string) => {
    setRecentJobs((previous) => previous.filter((item) => item.id !== jobId));
  });

  const applyDeleteFeedback = useEffectEvent((jobId: string, response: JobDeleteResponse) => {
    const infoText = response.queue_task_revoked
      ? '任务已取消并删除，排队占位已经释放。'
      : '任务已删除，你现在可以继续新建任务。';

    removeRecentJob(jobId);
    if (currentJobId === jobId) {
      setCurrentJobIdState(null);
      setCurrentJob(null);
      clearJobDetails();
      setUploadAlert({tone: 'success', text: infoText});
      setSelectionAlert(null);
      setJobAlert(null);
      return;
    }

    setJobAlert({tone: 'success', text: infoText});
  });

  const handleApiFailure = useEffectEvent(async (error: unknown, target: 'upload' | 'selection' | 'job') => {
    if (error instanceof UnauthorizedError) {
      await onUnauthorized('登录状态已失效，请重新登录。');
      return;
    }

    const alert = {tone: 'error' as const, text: getErrorMessage(error)};
    if (target === 'upload') {
      setUploadAlert(alert);
      return;
    }
    if (target === 'selection') {
      setSelectionAlert(alert);
      return;
    }
    setJobAlert(alert);
  });

  const hydrateEvents = useEffectEvent(async (jobId: string) => {
    try {
      const response = await getJobEvents(jobId);
      eventsLoadedForJobRef.current = jobId;
      eventsCursorRef.current = response.cursor;
      setEvents(response.events);
    } catch (error) {
      await handleApiFailure(error, 'job');
    }
  });

  const hydrateTasks = useEffectEvent(async (summary: JobSummaryResponse) => {
    if (!TASK_VISIBLE_STATUSES.has(summary.status) || tasksLoadedForJobRef.current === summary.id) {
      return;
    }

    try {
      const response = await getJobTasks(summary.id);
      tasksLoadedForJobRef.current = summary.id;
      setTasks(response.tasks);
      setSelectedTaskIds(getSelectedTaskIds(summary, response.tasks));
    } catch (error) {
      await handleApiFailure(error, 'selection');
    }
  });

  const hydrateArtifacts = useEffectEvent(async (summary: JobSummaryResponse) => {
    if (summary.status !== 'completed' || artifactsLoadedForJobRef.current === summary.id) {
      return;
    }

    try {
      const response = await getJobArtifacts(summary.id);
      artifactsLoadedForJobRef.current = summary.id;
      setArtifacts(sortArtifacts(response.artifacts));
    } catch (error) {
      await handleApiFailure(error, 'job');
    }
  });

  const applySummary = useEffectEvent(async (summary: JobSummaryResponse) => {
    setCurrentJob(summary);
    upsertRecentJob(summary);
    setJobAlert(null);
    if (summary.selected_task_ids?.length) {
      setSelectedTaskIds([...summary.selected_task_ids]);
    }
    await hydrateTasks(summary);
    await hydrateArtifacts(summary);
  });

  async function refreshRecentJobs() {
    if (!enabled) {
      setRecentJobs([]);
      return;
    }

    try {
      const response = await getJobs(12);
      setRecentJobs(response.jobs);
    } catch (error) {
      await handleApiFailure(error, 'job');
    }
  }

  async function refreshCurrentJob(showLoader = false) {
    if (!enabled || !currentJobId) {
      setCurrentJob(null);
      clearJobDetails();
      return;
    }

    if (showLoader) {
      setJobLoading(true);
    }

    try {
      const summary = await getJobSummary(currentJobId);
      await applySummary(summary);
      if (eventsLoadedForJobRef.current !== summary.id) {
        await hydrateEvents(summary.id);
      }
    } catch (error) {
      if (error instanceof ApiError && error.status === 404) {
        setCurrentJobIdState(null);
        setCurrentJob(null);
        clearJobDetails();
        setUploadAlert({tone: 'info', text: '最近一次任务已不存在，已为你重置到上传页。'});
        await refreshRecentJobs();
        return;
      }
      await handleApiFailure(error, 'job');
    } finally {
      if (showLoader) {
        setJobLoading(false);
      }
    }
  }

  const pollCurrentJob = useEffectEvent(async () => {
    if (!currentJobId) {
      return;
    }

    try {
      const summary = await getJobSummary(currentJobId);
      await applySummary(summary);
      const response = await getJobEvents(currentJobId, eventsCursorRef.current);
      eventsCursorRef.current = response.cursor;
      mergeEvents(response.events);
    } catch (error) {
      await handleApiFailure(error, 'job');
    }
  });

  useEffect(() => {
    if (!enabled) {
      setCurrentJob(null);
      setRecentJobs([]);
      clearJobDetails();
      return;
    }

    void refreshRecentJobs();
  }, [enabled]);

  useEffect(() => {
    if (!enabled || !currentJobId) {
      setCurrentJob(null);
      clearJobDetails();
      return;
    }

    clearJobDetails();
    void refreshCurrentJob(true);
  }, [enabled, currentJobId]);

  useEffect(() => {
    if (!enabled || !currentJob || !STREAMABLE_STATUSES.has(currentJob.status)) {
      return;
    }

    let cancelled = false;
    let pollTimer: number | null = null;
    const source = openJobStream(currentJob.stream_url);

    const stopPolling = () => {
      if (pollTimer !== null) {
        window.clearInterval(pollTimer);
        pollTimer = null;
      }
    };

    const startPolling = () => {
      if (pollTimer !== null) {
        return;
      }

      pollTimer = window.setInterval(() => {
        if (!cancelled) {
          void pollCurrentJob();
        }
      }, 3000);
    };

    source.addEventListener('job.summary', (event) => {
      if (cancelled) {
        return;
      }
      const nextSummary = JSON.parse((event as MessageEvent<string>).data) as JobSummaryResponse;
      void applySummary(nextSummary);
      if (!STREAMABLE_STATUSES.has(nextSummary.status)) {
        source.close();
        stopPolling();
      }
    });

    source.addEventListener('job.events', (event) => {
      if (cancelled) {
        return;
      }
      const payload = JSON.parse((event as MessageEvent<string>).data) as {
        events: JobEventResponse[];
        cursor: string | null;
      };
      eventsCursorRef.current = payload.cursor;
      mergeEvents(payload.events);
    });

    source.onerror = () => {
      source.close();
      startPolling();
    };

    return () => {
      cancelled = true;
      source.close();
      stopPolling();
    };
  }, [enabled, currentJob?.id, currentJob?.status, currentJob?.stream_url]);

  async function createNewJob() {
    if (!files.length) {
      setUploadAlert({tone: 'error', text: '请先选择至少一个数据文件。'});
      return;
    }

    setUploadBusy(true);
    setUploadAlert(null);
    setJobAlert(null);

    try {
      const summary = await createJob(files);
      clearJobDetails();
      setFiles([]);
      setCurrentJobIdState(summary.id);
      await applySummary(summary);
      await refreshRecentJobs();
    } catch (error) {
      await handleApiFailure(error, 'upload');
    } finally {
      setUploadBusy(false);
    }
  }

  function addFiles(nextFiles: File[]) {
    if (!nextFiles.length) {
      setUploadAlert({tone: 'error', text: '仅支持上传 CSV、XLS、XLSX 文件。'});
      return;
    }

    setFiles((previous) => {
      const merged = [...previous];
      for (const file of nextFiles) {
        const exists = merged.some(
          (current) =>
            current.name === file.name &&
            current.lastModified === file.lastModified &&
            current.size === file.size,
        );
        if (!exists) {
          merged.push(file);
        }
      }
      return merged;
    });
    setUploadAlert(null);
  }

  function removeFile(index: number) {
    setFiles((previous) => previous.filter((_, currentIndex) => currentIndex !== index));
  }

  function toggleTask(taskIndex: number) {
    setSelectedTaskIds((previous) =>
      previous.includes(taskIndex)
        ? previous.filter((current) => current !== taskIndex)
        : [...previous, taskIndex].sort((left, right) => left - right),
    );
  }

  async function submitTaskSelection() {
    if (!currentJob) {
      return;
    }
    if (!selectedTaskIds.length) {
      setSelectionAlert({tone: 'error', text: '请至少选择一张图再提交。'});
      return;
    }

    setSelectionBusy(true);
    setSelectionAlert(null);

    try {
      const summary = await submitSelection(currentJob.id, selectedTaskIds);
      await applySummary(summary);
      await refreshRecentJobs();
    } catch (error) {
      await handleApiFailure(error, 'selection');
    } finally {
      setSelectionBusy(false);
    }
  }

  async function deleteJob(jobId: string) {
    setDeletingJobId(jobId);

    try {
      const response = await deleteJobRequest(jobId);
      applyDeleteFeedback(jobId, response);
      await refreshRecentJobs();
    } catch (error) {
      await handleApiFailure(error, 'job');
    } finally {
      setDeletingJobId(null);
    }
  }

  async function downloadArtifact(artifact: JobArtifactResponse) {
    setDownloadingArtifact(artifact.artifact_type);
    try {
      const {blob, filename} = await fetchBinary(artifact.download_url);
      downloadBlobFile(blob, filename || artifact.download_url.split('/').pop() || 'artifact');
    } catch (error) {
      await handleApiFailure(error, 'job');
    } finally {
      setDownloadingArtifact(null);
    }
  }

  function resetWorkspace() {
    setCurrentJobIdState(null);
    setCurrentJob(null);
    setFiles([]);
    setUploadAlert(null);
    setSelectionAlert(null);
    setJobAlert(null);
    setDeletingJobId(null);
    clearJobDetails();
  }

  return {
    currentJobId,
    currentJob,
    recentJobs,
    tasks,
    artifacts,
    events,
    files,
    selectedTaskIds,
    jobLoading,
    uploadBusy,
    selectionBusy,
    deletingJobId,
    downloadingArtifact,
    uploadAlert,
    selectionAlert,
    jobAlert,
    setCurrentJobId: setCurrentJobIdState,
    addFiles,
    removeFile,
    createNewJob,
    toggleTask,
    submitTaskSelection,
    deleteJob,
    downloadArtifact,
    refreshCurrentJob,
    refreshRecentJobs,
    resetWorkspace,
  };
}
