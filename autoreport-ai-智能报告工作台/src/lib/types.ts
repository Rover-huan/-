export type JobStatus =
  | 'uploaded'
  | 'queued_analysis'
  | 'running_analysis'
  | 'awaiting_selection'
  | 'queued_render'
  | 'rendering'
  | 'completed'
  | 'failed'
  | 'expired';

export type JobPhase =
  | 'upload'
  | 'analysis'
  | 'selection'
  | 'render'
  | 'complete'
  | 'failed'
  | 'expired';

export type ArtifactType = 'docx' | 'pdf' | 'ipynb' | 'txt' | 'zip';

export interface UserResponse {
  id: string;
  email: string;
  status: string;
  created_at: string;
  email_verified: boolean;
  email_verified_at: string | null;
}

export interface SessionResponse {
  user: UserResponse;
}

export interface AdminAccountResponse {
  id: string;
  email: string;
  role: 'owner' | 'viewer';
  status: string;
  created_at: string;
  last_login_at: string | null;
}

export interface AdminSessionResponse {
  admin: AdminAccountResponse;
}

export interface QuotaRemainingResponse {
  jobs_used_today: number;
  jobs_remaining: number;
  daily_job_limit: number;
  upload_bytes_used_today: number;
  upload_bytes_remaining: number;
  daily_upload_bytes_limit: number;
  active_jobs: number;
  active_jobs_remaining: number;
  active_job_limit: number;
}

export interface JobTaskResponse {
  task_index: number;
  question_zh: string;
  analysis_type: string;
  required_datasets: string[];
  selected: boolean;
  analysis_text: string | null;
  chart_url: string | null;
}

export interface JobTaskListResponse {
  tasks: JobTaskResponse[];
}

export interface JobArtifactResponse {
  artifact_type: ArtifactType;
  download_url: string;
  created_at: string;
}

export interface JobArtifactListResponse {
  artifacts: JobArtifactResponse[];
}

export interface JobEventResponse {
  id: string;
  level: string;
  event_type: string;
  message: string;
  payload_json: Record<string, unknown> | null;
  created_at: string;
}

export interface JobEventListResponse {
  events: JobEventResponse[];
  cursor: string | null;
}

export interface JobSummaryResponse {
  id: string;
  status: JobStatus;
  phase: JobPhase;
  progress_percent: number;
  report_title: string | null;
  selected_task_ids: number[] | null;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
  retention_expires_at: string | null;
  error_summary: string | null;
  error_title: string | null;
  user_message: string | null;
  error_category: string | null;
  error_code: string | null;
  raw_detail: string | null;
  suggested_actions: string[];
  queue_position: number | null;
  retry_count: number;
  failure_code: string | null;
  failure_stage: string | null;
  latest_event_id: string | null;
  quota_remaining: QuotaRemainingResponse;
  tasks_url: string;
  events_url: string;
  artifacts_url: string;
  stream_url: string;
}

export interface JobListResponse {
  jobs: JobSummaryResponse[];
}

export interface JobDeleteResponse {
  status: string;
  job_id: string;
  queue_task_revoked: boolean;
  quota_remaining: QuotaRemainingResponse;
}

export interface AdminOverviewResponse {
  total_users: number;
  verified_users: number;
  active_jobs_total: number;
  queued_analysis: number;
  running_analysis: number;
  awaiting_selection: number;
  queued_render: number;
  rendering: number;
  jobs_created_today: number;
  completed_today: number;
  failed_today: number;
  uploads_bytes_today: number;
  llm_calls_today: number;
  llm_daily_budget_limit: number;
}

export interface AdminOnlineUser {
  user_id: string;
  email: string;
  last_seen_at: string;
  current_job_id: string | null;
  current_path: string | null;
}

export interface AdminDashboardResponse {
  overview: AdminOverviewResponse;
  online_window_seconds: number;
  online_users: AdminOnlineUser[];
  failure_rate_today: number;
  llm_usage: {
    calls_today: number;
    daily_budget_limit: number;
    remaining: number;
  };
  readiness: {
    status: string;
    checks: Record<string, {ok: boolean; detail: string}>;
  };
}

export interface AdminJobRow {
  id: string;
  user_id: string;
  user_email: string;
  status: JobStatus;
  phase: JobPhase;
  progress_percent: number;
  queue_task_id: string | null;
  report_title: string | null;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
  queue_position: number | null;
  failure_code: string | null;
  failure_stage: string | null;
  error_summary: string | null;
}

export interface AdminJobsResponse {
  jobs: AdminJobRow[];
}

export interface AdminUserRow {
  id: string;
  email: string;
  status: string;
  created_at: string;
  email_verified: boolean;
  email_verified_at: string | null;
  online: boolean;
  last_seen_at: string | null;
  current_job_id: string | null;
  quota: QuotaRemainingResponse;
}

export interface AdminUsersResponse {
  users: AdminUserRow[];
}

export interface AdminJobDetailResponse {
  user: UserResponse;
  job: JobSummaryResponse;
  inputs: Array<Record<string, unknown>>;
  tasks: JobTaskResponse[];
  artifacts: JobArtifactResponse[];
  events: JobEventResponse[];
}

export interface AdminUserUsageResponse {
  user: UserResponse;
  quota: QuotaRemainingResponse;
  quota_limits: {
    daily_job_limit: number;
    daily_upload_bytes_limit: number;
    active_job_limit: number;
  };
  quota_override: {
    daily_job_limit: number | null;
    daily_upload_bytes_limit: number | null;
    active_job_limit: number | null;
    updated_at: string | null;
  };
  presence: {
    online: boolean;
    last_seen_at: string | null;
    current_job_id: string | null;
    current_path: string | null;
  };
  jobs_created_today: number;
  uploads_bytes_today: number;
  recent_jobs: AdminJobRow[];
}
